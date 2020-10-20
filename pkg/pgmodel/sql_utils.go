// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package pgmodel

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
)

const (
	promSchema       = "prom_api"
	seriesViewSchema = "prom_series"
	metricViewSchema = "prom_metric"
	dataSchema       = "prom_data"
	dataSeriesSchema = "prom_data_series"
	infoSchema       = "prom_info"
	catalogSchema    = "_prom_catalog"
	extSchema        = "_prom_ext"

	getCreateMetricsTableWithNewSQL = "SELECT table_name, possibly_new FROM " + catalogSchema + ".get_or_create_metric_table_name($1)"
)

var (
	errMissingTableName = fmt.Errorf("missing metric table name")
)

type pgxBatch interface {
	Queue(query string, arguments ...interface{})
}

type pgxConn interface {
	Close()
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	CopyFromRows(rows [][]interface{}) pgx.CopyFromSource
	NewBatch() pgxBatch
	SendBatch(ctx context.Context, b pgxBatch) (pgx.BatchResults, error)
}

type pgxConnImpl struct {
	conn     *pgxpool.Pool
	readHist prometheus.ObserverVec
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
	if p.readHist != nil {
		defer func(start time.Time, hist prometheus.ObserverVec, path string) {
			elapsedMs := float64(time.Since(start).Milliseconds())
			hist.WithLabelValues(path).Observe(elapsedMs)
		}(time.Now(), p.readHist, sql[0:6])
	}

	return conn.Query(ctx, sql, args...)
}

func (p *pgxConnImpl) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	conn := p.getConn()
	if p.readHist != nil {
		defer func(start time.Time, hist prometheus.ObserverVec, path string) {
			elapsedMs := float64(time.Since(start).Milliseconds())
			hist.WithLabelValues(path).Observe(elapsedMs)
		}(time.Now(), p.readHist, sql[0:6])
	}

	return conn.QueryRow(ctx, sql, args...)
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

// SampleInfoIterator is an iterator over a collection of sampleInfos that returns
// data in the format expected for the data table row.
type SampleInfoIterator struct {
	sampleInfos     []samplesInfo
	sampleInfoIndex int
	sampleIndex     int
	minSeen         int64
}

// NewSampleInfoIterator is the constructor
func NewSampleInfoIterator() SampleInfoIterator {
	si := SampleInfoIterator{sampleInfos: make([]samplesInfo, 0)}
	si.ResetPosition()
	return si
}

//Append adds a sample info to the back of the iterator
func (t *SampleInfoIterator) Append(s samplesInfo) {
	t.sampleInfos = append(t.sampleInfos, s)
}

//ResetPosition resets the iteration position to the beginning
func (t *SampleInfoIterator) ResetPosition() {
	t.sampleIndex = -1
	t.sampleInfoIndex = 0
	t.minSeen = math.MaxInt64
}

// Next returns true if there is another row and makes the next row data
// available to Values(). When there are no more rows available or an error
// has occurred it returns false.
func (t *SampleInfoIterator) Next() bool {
	t.sampleIndex++
	if t.sampleInfoIndex < len(t.sampleInfos) && t.sampleIndex >= len(t.sampleInfos[t.sampleInfoIndex].samples) {
		t.sampleInfoIndex++
		t.sampleIndex = 0
	}
	return t.sampleInfoIndex < len(t.sampleInfos)
}

// Values returns the values for the current row
func (t *SampleInfoIterator) Values() (time.Time, float64, SeriesID) {
	info := t.sampleInfos[t.sampleInfoIndex]
	sample := info.samples[t.sampleIndex]
	if t.minSeen > sample.Timestamp {
		t.minSeen = sample.Timestamp
	}
	return model.Time(sample.Timestamp).Time(), sample.Value, info.seriesID
}

// Err returns any error that has been encountered by the CopyFromSource. If
// this is not nil *Conn.CopyFrom will abort the copy.
func (t *SampleInfoIterator) Err() error {
	return nil
}

// MetricCache provides a caching mechanism for metric table names.
type MetricCache interface {
	Get(metric string) (string, error)
	Set(metric string, tableName string) error
}

func getMetricTableName(conn pgxConn, metric string) (string, bool, error) {
	res, err := conn.Query(
		context.Background(),
		getCreateMetricsTableWithNewSQL,
		metric,
	)

	if err != nil {
		return "", true, err
	}

	var tableName string
	var possiblyNew bool
	defer res.Close()
	if !res.Next() {
		return "", true, errMissingTableName
	}

	if err := res.Scan(&tableName, &possiblyNew); err != nil {
		return "", true, err
	}

	return tableName, possiblyNew, nil
}

func timestamptzToMs(t pgtype.Timestamptz) int64 {
	switch t.InfinityModifier {
	case pgtype.NegativeInfinity:
		return math.MinInt64
	case pgtype.Infinity:
		return math.MaxInt64
	default:
		return t.Time.UnixNano() / 1e6
	}
}
