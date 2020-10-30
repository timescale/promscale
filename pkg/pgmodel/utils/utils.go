// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package utils

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
	"github.com/timescale/promscale/pkg/prompb"
)

const (
	PromSchema       = "prom_api"
	SeriesViewSchema = "prom_series"
	MetricViewSchema = "prom_metric"
	DataSchema       = "prom_data"
	DataSeriesSchema = "prom_data_series"
	InfoSchema       = "prom_info"
	CatalogSchema    = "_prom_catalog"
	ExtSchema        = "_prom_ext"

	getCreateMetricsTableWithNewSQL = "SELECT table_name, possibly_new FROM " + CatalogSchema + ".get_or_create_metric_table_name($1)"
)

var ErrMissingTableName = fmt.Errorf("missing metric table name")

type PgxBatch interface {
	Queue(query string, arguments ...interface{})
}

type PgxConn interface {
	Close()
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	CopyFromRows(rows [][]interface{}) pgx.CopyFromSource
	NewBatch() PgxBatch
	SendBatch(ctx context.Context, b PgxBatch) (pgx.BatchResults, error)
}

type PgxConnImpl struct {
	Conn     *pgxpool.Pool
	readHist prometheus.ObserverVec
}

func (p *PgxConnImpl) getConn() *pgxpool.Pool {
	return p.Conn
}

func (p *PgxConnImpl) Close() {
	conn := p.getConn()
	p.Conn = nil
	conn.Close()
}

func (p *PgxConnImpl) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	conn := p.getConn()
	return conn.Exec(ctx, sql, arguments...)
}

func (p *PgxConnImpl) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	conn := p.getConn()
	if p.readHist != nil {
		defer func(start time.Time, hist prometheus.ObserverVec, path string) {
			elapsedMs := float64(time.Since(start).Milliseconds())
			hist.WithLabelValues(path).Observe(elapsedMs)
		}(time.Now(), p.readHist, sql[0:6])
	}
	return conn.Query(ctx, sql, args...)
}

func (p *PgxConnImpl) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	conn := p.getConn()
	if p.readHist != nil {
		defer func(start time.Time, hist prometheus.ObserverVec, path string) {
			elapsedMs := float64(time.Since(start).Milliseconds())
			hist.WithLabelValues(path).Observe(elapsedMs)
		}(time.Now(), p.readHist, sql[0:6])
	}
	return conn.QueryRow(ctx, sql, args...)
}

func (p *PgxConnImpl) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	conn := p.getConn()
	return conn.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (p *PgxConnImpl) CopyFromRows(rows [][]interface{}) pgx.CopyFromSource {
	return pgx.CopyFromRows(rows)
}

func (p *PgxConnImpl) NewBatch() PgxBatch {
	return &pgx.Batch{}
}

func (p *PgxConnImpl) SendBatch(ctx context.Context, b PgxBatch) (pgx.BatchResults, error) {
	conn := p.getConn()

	return conn.SendBatch(ctx, b.(*pgx.Batch)), nil
}

// SeriesID represents a globally unique id for the series. This should be equivalent
// to the PostgreSQL type in the series table (currently BIGINT).
type SeriesID int64

// SamplesInfo information about the samples.
type SamplesInfo struct {
	Labels   *Labels
	SeriesID SeriesID
	Samples  []prompb.Sample
}

// SampleInfoIterator is an iterator over a collection of sampleInfos that returns
// data in the format expected for the data table row.
type SampleInfoIterator struct {
	SampleInfos     []SamplesInfo
	sampleInfoIndex int
	sampleIndex     int
	MinSeen         int64
}

// NewSampleInfoIterator is the constructor
func NewSampleInfoIterator() SampleInfoIterator {
	si := SampleInfoIterator{SampleInfos: make([]SamplesInfo, 0)}
	si.ResetPosition()
	return si
}

//Append adds a sample info to the back of the iterator
func (t *SampleInfoIterator) Append(s SamplesInfo) {
	t.SampleInfos = append(t.SampleInfos, s)
}

//ResetPosition resets the iteration position to the beginning
func (t *SampleInfoIterator) ResetPosition() {
	t.sampleIndex = -1
	t.sampleInfoIndex = 0
	t.MinSeen = math.MaxInt64
}

// Next returns true if there is another row and makes the next row data
// available to Values(). When there are no more rows available or an error
// has occurred it returns false.
func (t *SampleInfoIterator) Next() bool {
	t.sampleIndex++
	if t.sampleInfoIndex < len(t.SampleInfos) && t.sampleIndex >= len(t.SampleInfos[t.sampleInfoIndex].Samples) {
		t.sampleInfoIndex++
		t.sampleIndex = 0
	}
	return t.sampleInfoIndex < len(t.SampleInfos)
}

// Values returns the values for the current row
func (t *SampleInfoIterator) Values() (time.Time, float64, SeriesID) {
	info := t.SampleInfos[t.sampleInfoIndex]
	sample := info.Samples[t.sampleIndex]
	if t.MinSeen > sample.Timestamp {
		t.MinSeen = sample.Timestamp
	}
	return model.Time(sample.Timestamp).Time(), sample.Value, info.SeriesID
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

// GetMetricTableName returns the metrics table name and a bool that indicates whether the metric was newly created.
func GetMetricTableName(conn PgxConn, metric string) (string, bool, error) {
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
		return "", true, ErrMissingTableName
	}

	if err := res.Scan(&tableName, &possiblyNew); err != nil {
		return "", true, err
	}

	return tableName, possiblyNew, nil
}

// TimestamptzToMs converts Timestamp in pgtype to milliseconds.
func TimestamptzToMs(t pgtype.Timestamptz) int64 {
	switch t.InfinityModifier {
	case pgtype.NegativeInfinity:
		return math.MinInt64
	case pgtype.Infinity:
		return math.MaxInt64
	default:
		return t.Time.UnixNano() / 1e6
	}
}
