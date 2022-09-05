// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgxconn

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
)

// msUntilCtxDeadline returns the time in milliseconds remaining
// before the context deadline is reached.
//
// ok=false is returned if the given context doesn't have a deadline set.
func msUntilCtxDeadline(ctx context.Context) (ms int64, ok bool) {
	t, ok := ctx.Deadline()
	if !ok {
		return 0, false
	}
	return time.Until(t).Milliseconds(), true
}

// QueryWithTimeout wraps the given query in a batch along with
// statement_timeout set to the given timeout value. The timeout value is taken
// as milliseconds.
//
// The results is returned as if it was read with PgxConn.Query.
//
// A value of zero disables the timeout.
//
// The returned closeFn closes the batch operation. This must be called before
// the underlying connection can be used again. It's safe to call multiple
// times.
//
// All the queries in a batch are executed in an implicit transaction by using
// `set local statement_timeout` the timeout it's only applied to the queries
// belonging to the transaction and not to the connection session.
//
// There's a bug with the use of `set local` and implicit transactions.
// Even though the statements are executed correctly and apply the desire
// effects the following warning message is logged:
//
// `SET LOCAL can only be used in transaction blocks`
//
// https://www.postgresql.org/message-id/flat/16988-58edba102adb5128@postgresql.org
// https://github.com/npgsql/npgsql/issues/3688
//
// For more contect on statement_timeout:
// https://www.postgresql.org/docs/current/runtime-config-client.html
//
// statement_timeout (integer)
//
// Abort any statement that takes more than the specified amount of time. If
// log_min_error_statement is set to ERROR or lower, the statement that timed
// out will also be logged. If this value is specified without units, it is
// taken as milliseconds. A value of zero (the default) disables the timeout.
//
// The timeout is measured from the time a command arrives at the server until
// it is completed by the server. If multiple SQL statements appear in a single
// simple-Query message, the timeout is applied to each statement separately.
// (PostgreSQL versions before 13 usually treated the timeout as applying to
// the whole query string.) In extended query protocol, the timeout starts
// running when any query-related message (Parse, Bind, Execute, Describe)
// arrives, and it is canceled by completion of an Execute or Sync message.
//
// Setting statement_timeout in postgresql.conf is not recommended because it
// would affect all sessions.
func QueryWithTimeout(
	ctx context.Context,
	conn PgxConn,
	timeout int64,
	sql string,
	args ...interface{},
) (rows PgxRows, closeFn func() error, err error) {
	batchResults, err := sendQueryInBatchWithTimeout(ctx, conn, timeout, sql, args...)
	if err != nil {
		return nil, nil, err
	}

	// sql query
	rows, err = batchResults.Query()
	if err != nil {
		_ = batchResults.Close()
		return nil, nil, err
	}

	closeFn = func() error {
		rows.Close()
		return batchResults.Close()
	}

	return rows, closeFn, nil
}

// QueryWithTimeoutFromCtx executes the given query in a transaction that sets
// statement_timeout to the remaining time before the context expires. If the
// context doesn't have a deadline then the query is executed as is, without
// being wrapped in a transacation.
//
// The results is returned as if it was read with PgxConn.Query.
//
// For more information refer to the QueryWithTimeout docs.
func QueryWithTimeoutFromCtx(
	ctx context.Context,
	conn PgxConn,
	sql string,
	args ...interface{},
) (rows PgxRows, closeFn func() error, err error) {
	timeout, ok := msUntilCtxDeadline(ctx)
	if !ok {
		rows, err = conn.Query(ctx, sql, args...)
		closeFn = func() error {
			rows.Close()
			return nil
		}
		return rows, closeFn, err
	}

	return QueryWithTimeout(ctx, conn, timeout, sql, args...)
}

// QueryRowWithTimeout wraps the given query in a batch along with
// statement_timeout set to the given timeout value. The timeout value is taken
// as milliseconds.
//
// The results is returned as if it was read with PgxConn.QueryRow.
//
// A value of zero disables the timeout.
//
// The returned closeFn closes the batch operation. This must be called before
// the underlying connection can be used again. It's safe to call multiple
// times.
//
// For more context see the QueryRowWithTimeout docs.
func QueryRowWithTimeout(
	ctx context.Context,
	conn PgxConn,
	timeout int64,
	sql string,
	args ...interface{},
) (row pgx.Row, closeFn func() error, err error) {
	batchResults, err := sendQueryInBatchWithTimeout(ctx, conn, timeout, sql, args...)
	if err != nil {
		return nil, nil, err
	}
	row = batchResults.QueryRow()
	closeFn = func() error { return batchResults.Close() }
	return row, closeFn, nil
}

// QueryRowWithTimeoutFromCtx executes the given query in a transaction that sets
// statement_timeout to the remaining time before the context expires. If the
// context doesn't have a deadline then the query is executed as is, without
// being wrapped in a transacation.
//
// The results is returned as if it was read with PgxConn.QueryRow.
//
// For more information refer to the QueryRowWithTimeout docs.
func QueryRowWithTimeoutFromCtx(
	ctx context.Context,
	conn PgxConn,
	sql string,
	args ...interface{},
) (row pgx.Row, closeFn func() error, err error) {
	timeout, ok := msUntilCtxDeadline(ctx)
	if !ok {
		row = conn.QueryRow(ctx, sql, args...)
		// pgxRow doesn't have a close function and we are not using a batch
		// so there's nothing to cleanup.
		noopCloseFn := func() error {
			return nil
		}
		return row, noopCloseFn, err
	}

	return QueryRowWithTimeout(ctx, conn, timeout, sql, args...)
}

func sendQueryInBatchWithTimeout(
	ctx context.Context,
	conn PgxConn,
	timeout int64,
	sql string,
	args ...interface{},
) (pgx.BatchResults, error) {
	batch := NewBatchWithTimeout(conn, timeout)
	batch.Queue(sql, args...)
	return SendBatch(ctx, conn, batch, true)
}

// NewBatchWithTimeout returns a batch with its first item set to
// `set local statement_timeout=timeout`.
func NewBatchWithTimeout(conn PgxConn, timeout int64) PgxBatch {
	b := conn.NewBatch()
	b.Queue(fmt.Sprintf("set local statement_timeout = %d", timeout))
	return b
}

// NewBatchWithTimeoutFromCtx returns a batch that in its first item sets
// statement_timeout to the remaining time before the context expires. if
// the context doesn't have a deadline then an empty batch is returned.
//
// The returned sendBatchFn handles sending the batch and reading the result
// of the statement_timeout if it was set.
func NewBatchWithTimeoutFromCtx(
	ctx context.Context,
	conn PgxConn,
) (batch PgxBatch, sendBatchFn func() (pgx.BatchResults, error)) {

	batch = conn.NewBatch()
	timeout, withTimeout := msUntilCtxDeadline(ctx)
	if withTimeout {
		batch = NewBatchWithTimeout(conn, timeout)
	}
	sendBatchFn = func() (pgx.BatchResults, error) {
		return SendBatch(ctx, conn, batch, withTimeout)
	}
	return batch, sendBatchFn
}

// SendBatch is a convenience function it uses the given PgxConn to send the
// batch. If withTimeout=true then it's assume that the first item in the
// batch is a statement_timeout and its result is read before returning
// the pgx.BatchResults.
func SendBatch(
	ctx context.Context,
	conn PgxConn,
	batch PgxBatch,
	withTimeout bool,
) (pgx.BatchResults, error) {
	batchResults, err := conn.SendBatch(ctx, batch)
	if err != nil {
		return nil, err
	}
	if !withTimeout {
		return batchResults, nil
	}
	_, err = batchResults.Exec()
	if err != nil {
		_ = batchResults.Close()
		return nil, err
	}
	return batchResults, nil
}
