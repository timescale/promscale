package pgmodel

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/allegro/bigcache"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/prometheus/pkg/labels"
)

const (
	getCreateMetricsTableSQL = "SELECT table_name FROM get_or_create_metric_table_name($1)"
	getSeriesIDForLabelSQL   = "SELECT get_series_id_for_label($1)"
	dataTableSchema          = "prom"
)

var (
	copyColumns         = []string{"time", "value", "series_id"}
	errMissingTableName = fmt.Errorf("missing metric table name")
)

type pgxBatch interface {
	Queue(query string, arguments ...interface{})
}

type pgxConn interface {
	Close(ctx context.Context) error
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	CopyFromRows(rows [][]interface{}) pgx.CopyFromSource
	NewBatch() pgxBatch
	SendBatch(ctx context.Context, b pgxBatch) (pgx.BatchResults, error)
}

type pgxConnImpl struct {
	conn *pgx.Conn
}

func (p *pgxConnImpl) getConn() (*pgx.Conn, error) {
	return p.conn, nil
}

func (p *pgxConnImpl) Close(ctx context.Context) error {
	conn, err := p.getConn()
	if err != nil {
		return nil
	}
	p.conn = nil
	return conn.Close(ctx)
}

func (p *pgxConnImpl) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	conn, err := p.getConn()

	if err != nil {
		return nil, err
	}

	return conn.Exec(ctx, sql, arguments...)
}

func (p *pgxConnImpl) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	conn, err := p.getConn()

	if err != nil {
		return nil, err
	}

	return conn.Query(ctx, sql, args...)
}

func (p *pgxConnImpl) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	conn, err := p.getConn()

	if err != nil {
		return 0, err
	}

	return conn.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (p *pgxConnImpl) CopyFromRows(rows [][]interface{}) pgx.CopyFromSource {
	return pgx.CopyFromRows(rows)
}

func (p *pgxConnImpl) NewBatch() pgxBatch {
	return &pgx.Batch{}
}

func (p *pgxConnImpl) SendBatch(ctx context.Context, b pgxBatch) (pgx.BatchResults, error) {
	conn, err := p.getConn()

	if err != nil {
		return nil, err
	}

	return conn.SendBatch(ctx, b.(*pgx.Batch)), nil
}

// NewPgxIngestor returns a new Ingestor that write to PostgreSQL using PGX
func NewPgxIngestor(c *pgx.Conn) *DBIngestor {
	pi := &pgxInserter{
		conn: &pgxConnImpl{
			conn: c,
		},
	}

	config := bigcache.DefaultConfig(10 * time.Minute)

	series, _ := bigcache.NewBigCache(config)

	bc := &bCache{
		series: series,
	}

	return &DBIngestor{
		db:    pi,
		cache: bc,
	}
}

type seriesWithCallback struct {
	series   labels.Labels
	callback func(id SeriesID) error
}

type pgxInserter struct {
	conn           pgxConn
	seriesToInsert []*seriesWithCallback
	// TODO: update implementation to match existing caching layer.
	metricTableNames map[string]string
}

func (p *pgxInserter) AddSeries(lset labels.Labels, callback func(id SeriesID) error) {
	p.seriesToInsert = append(p.seriesToInsert, &seriesWithCallback{lset, callback})
}

func (p *pgxInserter) InsertSeries() error {
	var lastSeenLabel labels.Labels = nil

	batch := p.conn.NewBatch()
	numQueries := 0
	// Sort and remove duplicates. The sort is needed both to prevent DB deadlocks and to remove duplicates
	sort.Slice(p.seriesToInsert, func(i, j int) bool {
		return labels.Compare(p.seriesToInsert[i].series, p.seriesToInsert[j].series) < 0
	})

	batchSeries := make([][]*seriesWithCallback, 0, len(p.seriesToInsert))
	for _, curr := range p.seriesToInsert {
		if lastSeenLabel != nil && labels.Equal(lastSeenLabel, curr.series) {
			batchSeries[len(batchSeries)-1] = append(batchSeries[len(batchSeries)-1], curr)
			continue
		}

		json, err := curr.series.MarshalJSON()

		if err != nil {
			return err
		}

		batch.Queue(getSeriesIDForLabelSQL, json)
		numQueries++
		batchSeries = append(batchSeries, []*seriesWithCallback{curr})

		lastSeenLabel = curr.series
	}

	br, err := p.conn.SendBatch(context.Background(), batch)
	if err != nil {
		return err
	}
	defer br.Close()

	if numQueries != len(batchSeries) {
		return fmt.Errorf("unexpected difference in numQueries and batchSeries")
	}

	for i := 0; i < numQueries; i++ {
		row := br.QueryRow()

		var id SeriesID
		err = row.Scan(&id)
		if err != nil {
			return err
		}
		for _, swc := range batchSeries[i] {
			err := swc.callback(id)
			if err != nil {
				return err
			}
		}
	}

	// Flushing inserted series.
	p.seriesToInsert = p.seriesToInsert[:0]

	return nil
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
			pgx.Identifier{dataTableSchema, tableName},
			copyColumns,
			p.conn.CopyFromRows(data),
		)
		if err != nil {
			return result, err
		}
		result = result + uint64(inserted)
		if inserted != int64(len(data)) {
			return result, fmt.Errorf("Failed to insert all the data! Expected: %d, Got: %d", len(data), inserted)
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
