// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgxconn

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/timescale/promscale/pkg/log"
)

type PgxBatch interface {
	Queue(query string, arguments ...interface{})
}

type PgxConn interface {
	Close()
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...interface{}) (PgxRows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	CopyFromRows(rows [][]interface{}) pgx.CopyFromSource
	NewBatch() PgxBatch
	SendBatch(ctx context.Context, b PgxBatch) (pgx.BatchResults, error)
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
		Conn:     pool,
	}
}

func NewPgxConn(pool *pgxpool.Pool) PgxConn {
	return &connImpl{
		Conn: pool,
	}
}

type loggingConnImpl struct {
	connImpl
	Conn *pgxpool.Pool
}

type connImpl struct {
	Conn *pgxpool.Pool
}

type pgxRows struct {
	pgx.Rows
	sqlQuery  string
	args      []interface{}
	startTime time.Time
	logged    bool
}

func (p *loggingConnImpl) Query(ctx context.Context, sql string, args ...interface{}) (PgxRows, error) {
	startTime := time.Now()
	rows, err := p.Conn.Query(ctx, sql, args...)
	return &pgxRows{Rows: rows, sqlQuery: sql, args: args, startTime: startTime}, err
}

func (p *loggingConnImpl) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	defer logQueryStats(sql, time.Time{}, args...)()
	return p.Conn.QueryRow(ctx, sql, args...)
}

func (p *connImpl) Close() {
	conn := p.Conn
	p.Conn = nil
	conn.Close()
}

func (p *pgxRows) Next() bool {
	// The query fetch is async and happens on the first call to next.
	// so to get timing right, we log timing after first Next() call.
	if !p.logged {
		p.logged = true
		res := p.Rows.Next()
		logQueryStats(p.sqlQuery, p.startTime, p.args)()
		return res
	}
	return p.Rows.Next()
}

// calc SQL query execution time
func logQueryStats(sql string, startTime time.Time, args ...interface{}) func() {
	if startTime.IsZero() {
		startTime = time.Now()
	}
	return func() {
		log.Debug("msg", "SQL query timing", "query", filterIndentChars(sql), "args", fmt.Sprintf("%v", args...), "time", time.Since(startTime))
	}
}

func (p *connImpl) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	return p.Conn.Exec(ctx, sql, args...)
}

func (p *connImpl) Query(ctx context.Context, sql string, args ...interface{}) (PgxRows, error) {
	return p.Conn.Query(ctx, sql, args...)
}

func (p *connImpl) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return p.Conn.QueryRow(ctx, sql, args...)
}

func (p *connImpl) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return p.Conn.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (p *connImpl) CopyFromRows(rows [][]interface{}) pgx.CopyFromSource {
	return pgx.CopyFromRows(rows)
}

func (p *connImpl) NewBatch() PgxBatch {
	return &pgx.Batch{}
}

func (p *connImpl) SendBatch(ctx context.Context, b PgxBatch) (pgx.BatchResults, error) {
	return p.Conn.SendBatch(ctx, b.(*pgx.Batch)), nil
}

// filters out indentation characters from the
// SQL query for better query logging
func filterIndentChars(query string) string {
	dropChars := []string{"\n", "\t", "\""}
	query = strings.ReplaceAll(query, "\n\t", " ")
	for _, c := range dropChars {
		query = strings.ReplaceAll(query, c, "")
	}

	return query
}
