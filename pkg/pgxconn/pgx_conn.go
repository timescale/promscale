// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgxconn

import (
	"context"
	"fmt"
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

func NewPgxConn(pool *pgxpool.Pool) PgxConn {
	return &connImpl{
		Conn: pool,
	}
}

type connImpl struct {
	Conn *pgxpool.Pool
}

type pgxRows struct {
	pgx.Rows
	sqlQuery  string
	args      []interface{}
	startTime time.Time
	loggged   bool
}

func (p *connImpl) Close() {
	conn := p.Conn
	p.Conn = nil
	conn.Close()
}

func (p *pgxRows) Next() bool {
	if !p.loggged {
		p.loggged = true
		LogQueryStats(p.sqlQuery, p.startTime, p.args)()
	}
	return p.Rows.Next()
}

func (p *pgxRows) Scan(dest ...interface{}) error {
	return p.Rows.Scan(dest...)
}

func (p *pgxRows) Err() error {
	return p.Rows.Err()
}

func (p *pgxRows) Close() {
	p.Rows.Close()
}

// calc SQL query execution time
func LogQueryStats(sql string, startTime time.Time, args ...interface{}) func() {
	if startTime.IsZero() {
		startTime = time.Now()
	}
	return func() {
		log.Debug("msg", fmt.Sprintf("time taken by SQL query: %s with args: %v is %v", sql, args, time.Since(startTime)))
	}
}

func (p *connImpl) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	defer LogQueryStats(sql, time.Time{}, args...)()
	return p.Conn.Exec(ctx, sql, args...)
}

func (p *connImpl) Query(ctx context.Context, sql string, args ...interface{}) (PgxRows, error) {
	startTime := time.Now()
	rows, err := p.Conn.Query(ctx, sql, args...)
	return &pgxRows{Rows: rows, sqlQuery: sql, args: args, startTime: startTime}, err
}

func (p *connImpl) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	defer LogQueryStats(sql, time.Time{}, args...)()
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
