package testsupport

import (
	"context"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/timescale/promscale/pkg/pgxconn"
)

type MockRow struct{}

func (MockRow) Scan(dest ...interface{}) error { return nil }

type MockBatchResults struct{}

func (MockBatchResults) Exec() (pgconn.CommandTag, error) {
	return nil, nil
}

func (MockBatchResults) Query() (pgx.Rows, error) {
	return nil, nil
}
func (MockBatchResults) QueryRow() pgx.Row {
	return MockRow{}
}
func (MockBatchResults) QueryFunc(scans []interface{}, f func(pgx.QueryFuncRow) error) (pgconn.CommandTag, error) {
	return nil, nil
}
func (MockBatchResults) Close() error { return nil }

type MockBatch struct{}

func (MockBatch) Queue(query string, arguments ...interface{}) {}
func (MockBatch) Len() int {
	return 0
}

type MockPgxConn struct{}

func (MockPgxConn) Close() {}
func (MockPgxConn) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (MockPgxConn) Query(ctx context.Context, sql string, args ...interface{}) (pgxconn.PgxRows, error) {
	return nil, nil
}
func (MockPgxConn) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return MockRow{}
}
func (MockPgxConn) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return 0, nil
}
func (MockPgxConn) CopyFromRows(rows [][]interface{}) pgx.CopyFromSource { return nil }
func (MockPgxConn) NewBatch() pgxconn.PgxBatch                           { return MockBatch{} }
func (MockPgxConn) SendBatch(ctx context.Context, b pgxconn.PgxBatch) (pgx.BatchResults, error) {
	return MockBatchResults{}, nil
}
func (MockPgxConn) Acquire(ctx context.Context) (*pgxpool.Conn, error) { return nil, nil }
func (MockPgxConn) BeginTx(ctx context.Context) (pgx.Tx, error)        { return nil, nil }
