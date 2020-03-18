package pgmodel

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/allegro/bigcache"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

const (
	getCreateMetricsTableSQL = "SELECT table_name FROM get_or_create_metric_table_name($1)"
	getSeriesIDForLabelSQL   = "SELECT get_series_id_for_key_value_array($1, $2, $3)"
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
	Close()
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	CopyFromRows(rows [][]interface{}) pgx.CopyFromSource
	NewBatch() pgxBatch
	SendBatch(ctx context.Context, b pgxBatch) (pgx.BatchResults, error)
}

type pgxConnImpl struct {
	conn *pgxpool.Pool
}

func (p *pgxConnImpl) getConn() *pgxpool.Pool {
	return p.conn
}

func (p *pgxConnImpl) Close() {
	conn := p.getConn()
	p.conn = nil
	conn.Close()
}

func (p *pgxConnImpl) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	conn := p.getConn()

	return conn.Exec(ctx, sql, arguments...)
}

func (p *pgxConnImpl) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	conn := p.getConn()

	return conn.Query(ctx, sql, args...)
}

func (p *pgxConnImpl) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	conn := p.getConn()

	return conn.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (p *pgxConnImpl) CopyFromRows(rows [][]interface{}) pgx.CopyFromSource {
	return pgx.CopyFromRows(rows)
}

func (p *pgxConnImpl) NewBatch() pgxBatch {
	return &pgx.Batch{}
}

func (p *pgxConnImpl) SendBatch(ctx context.Context, b pgxBatch) (pgx.BatchResults, error) {
	conn := p.getConn()

	return conn.SendBatch(ctx, b.(*pgx.Batch)), nil
}

// NewPgxIngestor returns a new Ingestor that write to PostgreSQL using PGX
func NewPgxIngestor(c *pgxpool.Pool) *DBIngestor {
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

type pgxInserter struct {
	conn pgxConn
	// TODO: update implementation to match existing caching layer?
	metricTableNames sync.Map
}

func (p *pgxInserter) InsertNewData(newSeries []SeriesWithCallback, rows map[string]*SampleInfoIterator) (uint64, error) {
	err := p.InsertSeries(newSeries)
	if err != nil {
		return 0, err
	}

	return p.InsertData(rows)
}

func (p *pgxInserter) InsertSeries(seriesToInsert []SeriesWithCallback) error {
	if len(seriesToInsert) == 0 {
		return nil
	}

	var lastSeenLabel Labels

	batch := p.conn.NewBatch()
	numQueries := 0
	// Sort and remove duplicates. The sort is needed both to prevent DB deadlocks and to remove duplicates
	sort.Slice(seriesToInsert, func(i, j int) bool {
		return seriesToInsert[i].Series.Compare(seriesToInsert[j].Series) < 0
	})

	batchSeries := make([][]SeriesWithCallback, 0, len(seriesToInsert))
	for _, curr := range seriesToInsert {
		if !lastSeenLabel.isEmpty() && lastSeenLabel.Equal(curr.Series) {
			batchSeries[len(batchSeries)-1] = append(batchSeries[len(batchSeries)-1], curr)
			continue
		}

		batch.Queue(getSeriesIDForLabelSQL, curr.Series.metric_name, curr.Series.names, curr.Series.values)
		numQueries++
		batchSeries = append(batchSeries, []SeriesWithCallback{curr})

		lastSeenLabel = curr.Series
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
			err := swc.Callback(swc.Series, id)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *pgxInserter) InsertData(rows map[string]*SampleInfoIterator) (uint64, error) {
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
			data,
		)
		if err != nil {
			return result, err
		}
		result = result + uint64(inserted)
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
	var tableNameI interface{}
	var err error
	var ok bool
	var tableName string
	if tableNameI, ok = p.metricTableNames.Load(metric); !ok {
		tableName, err = p.createMetricTable(metric)
		if err != nil {
			return "", err
		}
		p.metricTableNames.Store(metric, tableName)
	} else {
		tableName = tableNameI.(string)
	}

	return tableName, nil
}
