// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgxconn

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/timescale/promscale/pkg/log"
)

const namespace = "promscale"

var (
	requestTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "database",
			Name:      "requests_total",
			Help:      "Total number of database requests.",
		}, []string{"method"},
	)
	errorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "database",
			Name:      "request_errors_total",
			Help:      "Total number of database request errors.",
		}, []string{"method"},
	)
	duration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: "database",
			Name:      "requests_duration_seconds",
			Help:      "Time taken to complete a database request and process the response.",
		}, []string{"method"},
	)
)

func init() {
	prometheus.MustRegister(requestTotal, errorsTotal, duration)
}

func promMethodLabel(method string) prometheus.Labels {
	return prometheus.Labels{"method": method}
}

type PgxBatch interface {
	Queue(query string, arguments ...any) *pgx.QueuedQuery
	Len() int
}

type PgxConn interface {
	Close()
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...interface{}) (PgxRows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
	CopyFrom(
		ctx context.Context,
		tx pgx.Tx,
		tableName pgx.Identifier,
		columnNames []string,
		rowSrc pgx.CopyFromSource,
		oids []uint32,
	) (int64, error)
	CopyFromRows(rows [][]interface{}) pgx.CopyFromSource
	NewBatch() PgxBatch
	SendBatch(ctx context.Context, b PgxBatch) (pgx.BatchResults, error)
	Acquire(ctx context.Context) (*pgxpool.Conn, error)
	BeginTx(ctx context.Context) (pgx.Tx, error)
}

type PgxRows interface {
	Next() bool
	Scan(...interface{}) error
	Err() error
	Close()
}

// This is only used by querier to log the time consumed
// by SQL queries executed using Query(), QueryRow()
func NewQueryLoggingPgxConn(pool *pgxpool.Pool) PgxConn {
	return &loggingConnImpl{
		connImpl: connImpl{Conn: pool},
	}
}

func NewPgxConn(pool *pgxpool.Pool) PgxConn {
	return &connImpl{
		Conn: pool,
	}
}

type loggingConnImpl struct {
	connImpl
}

type connImpl struct {
	Conn *pgxpool.Pool
}

type loggingPgxRows struct {
	pgx.Rows
	sqlQuery  string
	args      []interface{}
	startTime time.Time
	logged    bool
}

func (p *loggingPgxRows) Next() bool {
	// The query fetch is async and happens on the first call to next.
	// so to get timing right, we log timing after first Next() call.
	if !p.logged {
		p.logged = true
		res := p.Rows.Next()
		logQueryStats(p.sqlQuery, p.startTime, p.args...)()
		return res
	}
	return p.Rows.Next()
}

func (p *loggingConnImpl) Query(ctx context.Context, sql string, args ...interface{}) (PgxRows, error) {
	startTime := time.Now()
	rows, err := p.Conn.Query(ctx, sql, args...)
	return &loggingPgxRows{Rows: rows, sqlQuery: sql, args: args, startTime: startTime}, err
}

func (p *loggingConnImpl) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	defer logQueryStats(sql, time.Time{}, args...)()
	return p.Conn.QueryRow(ctx, sql, args...)
}

// log the SQL query, args and time consumed by the query in execution
func logQueryStats(sql string, startTime time.Time, args ...interface{}) func() {
	if startTime.IsZero() {
		startTime = time.Now()
	}
	return func() {
		log.Debug("msg", "SQL query timing", "query", filterIndentChars(sql), "args", fmt.Sprintf("%v", args), "time", time.Since(startTime))
	}
}

func (p *connImpl) Close() {
	conn := p.Conn
	p.Conn = nil
	conn.Close()
}

func (p *connImpl) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	execLabels := promMethodLabel("exec")
	requestTotal.With(execLabels).Inc()
	start := time.Now()
	defer func() {
		duration.With(execLabels).Observe(time.Since(start).Seconds())
	}()
	tag, err := p.Conn.Exec(ctx, sql, args...)
	if err != nil {
		errorsTotal.With(execLabels).Inc()
	}
	return tag, err
}

func (p *connImpl) Query(ctx context.Context, sql string, args ...interface{}) (PgxRows, error) {
	// TODO (harkishen): Add sql queries as labels to know how much time taken for each query type.
	queryLabels := promMethodLabel("query")
	requestTotal.With(queryLabels).Inc()
	start := time.Now()
	rows, err := p.Conn.Query(ctx, sql, args...)
	if err != nil {
		errorsTotal.With(queryLabels).Inc()
		return nil, err
	}
	return newRowsWithDuration(rows, start), err
}

func (p *connImpl) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	queryRowLabels := promMethodLabel("query_row")
	requestTotal.With(queryRowLabels).Inc()
	start := time.Now()
	defer func() {
		duration.With(queryRowLabels).Observe(time.Since(start).Seconds())
	}()
	return rowWithTelemetry{p.Conn.QueryRow(ctx, sql, args...)}
}

func (p *connImpl) CopyFrom(
	ctx context.Context,
	tx pgx.Tx,
	tableName pgx.Identifier,
	columnNames []string,
	rowSrc pgx.CopyFromSource,
	oids []uint32,
) (int64, error) {
	return doCopyFrom(
		ctx,
		tx.Conn(),
		tableName,
		columnNames,
		rowSrc,
		oids,
	)
}

func (p *connImpl) CopyFromRows(rows [][]interface{}) pgx.CopyFromSource {
	return pgx.CopyFromRows(rows)
}

func (p *connImpl) NewBatch() PgxBatch {
	return &pgx.Batch{}
}

func (p *connImpl) SendBatch(ctx context.Context, b PgxBatch) (pgx.BatchResults, error) {
	requestTotal.With(promMethodLabel("send_batch")).Inc()
	return newBatchResultsWithDuration(p.Conn.SendBatch(ctx, b.(*pgx.Batch)), time.Now()), nil
}

func (p *connImpl) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	return p.Conn.Acquire(ctx)
}

func (p *connImpl) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return p.Conn.BeginTx(ctx, pgx.TxOptions{})
}

// filters out indentation characters from the
// SQL query for better query logging
func filterIndentChars(query string) string {
	dropChars := strings.NewReplacer("\n\t", " ", "\n", "", "\t", "", "\"", "")
	query = dropChars.Replace(query)
	return query
}
