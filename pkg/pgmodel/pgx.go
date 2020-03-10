package pgmodel

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/allegro/bigcache"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

const (
	insertLabelBatchSize     = 100
	insertLabelSQL           = "INSERT INTO labels(name, value) VALUES %s ON CONFLICT DO NOTHING"
	labelSQLFormat           = "('%s', '%s')"
	insertSeriesSQL          = "INSERT INTO series(fingerprint, labels) VALUES %s ON CONFLICT DO NOTHING RETURNING id, fingerprint"
	seriesSQLFormat          = "(%d, '%s')"
	getCreateMetricsTableSQL = "SELECT get_or_create_metric_table_name($1)"
)

var (
	copyColumns = []string{"time", "value", "series_id"}

	errMissingTableName = fmt.Errorf("missing metric table name")
)

type pgxConn interface {
	Close(ctx context.Context) error
	UseDatabase(database string)
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	CopyFromRows(rows [][]interface{}) pgx.CopyFromSource
}

type PgxConnImpl struct {
	Config *pgx.ConnConfig

	conn       *pgx.Conn
	activeConn bool
}

func (p *PgxConnImpl) newConn() (*pgx.Conn, error) {
	return pgx.ConnectConfig(context.Background(), p.Config)
}

func (p *PgxConnImpl) getConn() (*pgx.Conn, error) {
	if p.activeConn {
		return p.conn, nil
	}

	conn, err := p.newConn()
	p.conn = conn

	if err != nil {
		p.activeConn = true
	}

	return conn, err
}

func (p *PgxConnImpl) Close(ctx context.Context) error {
	conn, err := p.getConn()

	if err != nil {
		return nil
	}
	p.activeConn = false
	return conn.Close(ctx)
}

func (p *PgxConnImpl) UseDatabase(dbName string) {
	p.Config.Database = dbName
	p.Close(context.Background())
}

func (p *PgxConnImpl) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	conn, err := p.getConn()

	if err != nil {
		return nil, err
	}

	return conn.Exec(ctx, sql, arguments...)
}

func (p *PgxConnImpl) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	conn, err := p.getConn()

	if err != nil {
		return nil, err
	}

	return conn.Query(ctx, sql, args...)
}

func (p *PgxConnImpl) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	conn, err := p.getConn()

	if err != nil {
		return 0, err
	}

	return conn.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (p *PgxConnImpl) CopyFromRows(rows [][]interface{}) pgx.CopyFromSource {
	return pgx.CopyFromRows(rows)
}

func NewPgxInserter(c *pgx.ConnConfig) *DBIngestor {
	pi := &pgxInserter{
		conn: &PgxConnImpl{
			Config: c,
		},
	}

	config := bigcache.DefaultConfig(10 * time.Minute)

	labels, _ := bigcache.NewBigCache(config)
	series, _ := bigcache.NewBigCache(config)

	bc := &bCache{
		labels: labels,
		series: series,
	}

	return &DBIngestor{
		db:    pi,
		cache: bc,
	}
}

type seriesWithFP struct {
	series      *model.LabelSet
	fingerprint uint64
}

type pgxInserter struct {
	conn           pgxConn
	labelsToInsert []*prompb.Label
	seriesToInsert []*seriesWithFP
	// TODO: update implementation to match existing caching layer.
	metricTableNames map[string]string
}

func (p *pgxInserter) AddLabel(label *prompb.Label) {
	p.labelsToInsert = append(p.labelsToInsert, label)
}

func (p *pgxInserter) AddSeries(fingerprint uint64, series *model.LabelSet) {
	p.seriesToInsert = append(p.seriesToInsert, &seriesWithFP{series, fingerprint})
}

func (p *pgxInserter) InsertLabels() ([]*prompb.Label, error) {
	idx, start, end := 0, 0, 0
	size := len(p.labelsToInsert)
	ret := make([]*prompb.Label, 0, size)

	sort.Slice(
		p.labelsToInsert,
		func(i, j int) bool {
			if p.labelsToInsert[i].Name != p.labelsToInsert[j].Name {
				return p.labelsToInsert[i].Name < p.labelsToInsert[j].Name
			}
			return p.labelsToInsert[i].Value < p.labelsToInsert[j].Value
		})

	for idx*insertLabelBatchSize < size {
		lastSeenLabelName, lastSeenLabelValue := "", ""
		start = idx * insertLabelBatchSize
		end = (idx + 1) * insertLabelBatchSize
		if end >= size {
			end = size
		}
		labelValues := make([]string, 0, insertLabelBatchSize)

		// Remove duplicates.
		for _, l := range p.labelsToInsert[start:end] {
			if lastSeenLabelName == l.Name &&
				lastSeenLabelValue == l.Value {
				continue
			}
			lastSeenLabelName = l.Name
			lastSeenLabelValue = l.Value
			labelValues = append(labelValues, fmt.Sprintf(labelSQLFormat, l.Name, l.Value))
			ret = append(ret, l)
		}

		_, err := p.conn.Exec(
			context.Background(),
			fmt.Sprintf(insertLabelSQL,
				strings.Join(labelValues, ",")),
		)

		if err != nil {
			return nil, err
		}

		idx++
	}
	// Flushing inserted labels.
	p.labelsToInsert = make([]*prompb.Label, 0)

	return ret, nil
}

func (p *pgxInserter) InsertSeries() ([]uint64, []uint64, error) {
	var ids, fps []uint64
	var lastSeenFP uint64
	insertSeries := make([]string, 0, len(p.seriesToInsert))

	// Sort and remove duplicates.
	sort.Slice(p.seriesToInsert, func(i, j int) bool { return p.seriesToInsert[i].fingerprint < p.seriesToInsert[j].fingerprint })
	for _, curr := range p.seriesToInsert {
		if lastSeenFP == curr.fingerprint {
			continue
		}

		json, err := json.Marshal(curr.series)

		if err != nil {
			return ids, fps, err
		}

		lastSeenFP = curr.fingerprint
		insertSeries = append(insertSeries, fmt.Sprintf(seriesSQLFormat, curr.fingerprint, json))
	}

	res, err := p.conn.Query(
		context.Background(),
		fmt.Sprintf(insertSeriesSQL,
			strings.Join(insertSeries, ",")),
	)
	if err != nil {
		return ids, fps, err
	}
	defer res.Close()

	var id, fp uint64

	for res.Next() {
		err = res.Scan(&id, &fp)
		if err != nil {
			return ids, fps, err
		}

		ids = append(ids, id)
		fps = append(fps, fp)
	}
	// Flushing inserted series.
	p.seriesToInsert = make([]*seriesWithFP, 0)

	return ids, fps, nil
}

func (p *pgxInserter) InsertData(rows map[string][][]interface{}) (uint64, error) {
	var result uint64
	var err error
	var tableName string
	for metricName, data := range rows {
		tableName, err = p.getMetricTableName(metricName)
		if err != nil {
			return result, err
		}
		inserted, err := p.conn.CopyFrom(
			context.Background(),
			pgx.Identifier{tableName},
			copyColumns,
			p.conn.CopyFromRows(data),
		)
		if err != nil {
			return result, err
		}
		result = result + uint64(inserted)
		if inserted != int64(len(rows)) {
			return result, fmt.Errorf("Failed to insert all the data! Expected: %d, Got: %d", len(rows), inserted)
		}
	}

	return result, nil
}

func (p *pgxInserter) createMetricTable(metric string) (string, error) {
	res, err := p.conn.Query(
		context.Background(),
		getCreateMetricsTableSQL,
		metric,
	)

	if err != nil {
		return "", err
	}

	var tableName string
	defer res.Close()
	if !res.Next() {
		return "", errMissingTableName
	}

	if err := res.Scan(&tableName); err != nil {
		return "", err
	}

	return tableName, nil
}

func (p *pgxInserter) getMetricTableName(metric string) (string, error) {
	var tableName string
	var err error
	var ok bool
	if tableName, ok = p.metricTableNames[metric]; !ok {
		if p.metricTableNames == nil {
			p.metricTableNames = make(map[string]string)
		}
		tableName, err = p.createMetricTable(metric)
		if err != nil {
			return "", err
		}
		p.metricTableNames[metric] = tableName
	}

	return tableName, nil
}
