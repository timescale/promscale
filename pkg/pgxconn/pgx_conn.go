package pgxconn

import (
	"context"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

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

func NewPgxConn(pool *pgxpool.Pool) PgxConn {
	return &connImpl{
		Conn: pool,
	}
}

type connImpl struct {
	Conn *pgxpool.Pool
}

func (p *connImpl) Close() {
	conn := p.Conn
	p.Conn = nil
	conn.Close()
}

func (p *connImpl) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	return p.Conn.Exec(ctx, sql, arguments...)
}

func (p *connImpl) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
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
