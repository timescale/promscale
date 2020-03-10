package pgmodel

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/allegro/bigcache"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/common/model"
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

type PgxConnImpl struct {
	conn *pgx.Conn
}

func (p *PgxConnImpl) getConn() (*pgx.Conn, error) {
	return p.conn, nil
}

func (p *PgxConnImpl) Close(ctx context.Context) error {
	conn, err := p.getConn()
	if err != nil {
		return nil
	}
	p.conn = nil
	return conn.Close(ctx)
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

func (p *PgxConnImpl) NewBatch() pgxBatch {
	return &pgx.Batch{}
}

func (p *PgxConnImpl) SendBatch(ctx context.Context, b pgxBatch) (pgx.BatchResults, error) {
	conn, err := p.getConn()

	if err != nil {
		return nil, err
	}

	return conn.SendBatch(ctx, b.(*pgx.Batch)), nil
}

func NewPgxInserter(c *pgx.Conn) *DBIngestor {
	pi := &pgxInserter{
		conn: &PgxConnImpl{
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

type seriesWithFP struct {
	series      *model.LabelSet
	fingerprint uint64
}

type pgxInserter struct {
	conn           pgxConn
	seriesToInsert []*seriesWithFP
	// TODO: update implementation to match existing caching layer.
	metricTableNames map[string]string
}

func (p *pgxInserter) AddSeries(fingerprint uint64, series *model.LabelSet) {
	p.seriesToInsert = append(p.seriesToInsert, &seriesWithFP{series, fingerprint})
}

func (p *pgxInserter) InsertSeries() ([]SeriesID, []uint64, error) {
	var ids []SeriesID
	var fps []uint64
	var lastSeenFP uint64

	batch := p.conn.NewBatch()
	numQueries := 0
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

		batch.Queue(getSeriesIDForLabelSQL, json)
		numQueries++
		fps = append(fps, curr.fingerprint)

		lastSeenFP = curr.fingerprint
	}

	br, err := p.conn.SendBatch(context.Background(), batch)
	if err != nil {
		return ids, fps, err
	}
	defer br.Close()

	for i := 0; i < numQueries; i++ {
		row := br.QueryRow()

		var id SeriesID
		err = row.Scan(&id)
		if err != nil {
			return ids, fps, err
		}

		ids = append(ids, id)
	}

	// Flushing inserted series.
	p.seriesToInsert = p.seriesToInsert[:0]

	if len(ids) != len(fps) {
		panic("The length of ids and fingerprints should always be equal")
	}

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
