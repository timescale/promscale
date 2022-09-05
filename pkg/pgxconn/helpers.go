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

const (
	timeoutDisabled = 0
	minTimeoutMs    = 1
)

type rowsFromSingleQueryBatch struct {
	PgxRows
	batchResults pgx.BatchResults
}

func (b *rowsFromSingleQueryBatch) Close() {
	b.PgxRows.Close()
	_ = b.batchResults.Close()
}

// StatementTimeoutFromCtx returns the time in milliseconds remaining
// before the context expires to be used in a `statement_timeout`.
//
// If the context had a deadline set but the remaining time is lower than
// zero, a 1 Ms timeout value will be returned instead. This is because
// zero disables statement_timeout and it's also the minimum allowed value
// accepted by the function. Either way, at this point the context is
// most likely canceled and the request won't made.
func StatementTimeoutFromCtx(ctx context.Context) (int64, bool) {
	t, ok := ctx.Deadline()
	if !ok {
		return timeoutDisabled, false
	}
	d := time.Until(t).Milliseconds()
	if d < minTimeoutMs {
		return minTimeoutMs, true
	}
	return d, true
}

// QueryWithTimeout wraps the given query in a transaction that has
// statement_timeout set to the given timeout value. The timeout value is
// taken as milliseconds. A timeout less than 0 will disable the timeout.
//
// The returned `commit` functions returns the error, if any,
// from the Exec("COMMIT") part of the transaction. Since this is the
// final query from the underlying batch, subsequent calls will yield then
// `batch already close` error.
//
// For more contect. Taken from the postgreSQL docs:
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
) (rows PgxRows, commit func() error, err error) {
	noopCommit := func() error { return nil }
	if timeout < timeoutDisabled {
		timeout = timeoutDisabled
	}
	b := conn.NewBatch()
	b.Queue("BEGIN")
	b.Queue(fmt.Sprintf("set local statement_timeout = %d", timeout))
	b.Queue(sql, args...)
	b.Queue("COMMIT")
	r, err := conn.SendBatch(ctx, b)
	if err != nil {
		return nil, noopCommit, err
	}
	// BEGIN
	_, err = r.Exec()
	if err != nil {
		_ = r.Close()
		return nil, noopCommit, err
	}
	// set local statement_timeout
	_, err = r.Exec()
	if err != nil {
		_ = r.Close()
		return nil, noopCommit, err
	}

	// sql query
	rows, err = r.Query()
	if err != nil {
		_ = r.Close()
		return nil, noopCommit, err
	}
	commit = func() error {
		defer r.Close()
		// COMMIT
		_, err := r.Exec()
		if err != nil {
			return err
		}
		return nil
	}
	return &rowsFromSingleQueryBatch{rows, r}, commit, err
}

// QueryWithTimeoutFromCtx executes the given query in a transaction
// that sets statement_timeout to the remaining time before the context
// expires. If the context doesn't have a deadline then the query is
// executed as is, without being wrapped in a transacation.
//
// For more information refer to the QueryWithTimeout docs.
func QueryWithTimeoutFromCtx(
	ctx context.Context,
	conn PgxConn,
	sql string,
	args ...interface{},
) (rows PgxRows, commit func() error, err error) {
	timeout, ok := StatementTimeoutFromCtx(ctx)
	if !ok {
		noopCommit := func() error { return nil }
		rows, err = conn.Query(ctx, sql, args...)
		return rows, noopCommit, err
	}
	return QueryWithTimeout(ctx, conn, timeout, sql, args...)
}

func QueueTimeout(batch PgxBatch, timeout int64) {
	batch.Queue("BEGIN")
	batch.Queue(fmt.Sprintf("set local statement_timeout = %d", timeout))
}

func QueueCommit(batch PgxBatch) {
	batch.Queue("COMMIT")
}

func ExecBeginWithTimeout(batchResults pgx.BatchResults) error {
	// BEGIN
	_, err := batchResults.Exec()
	if err != nil {
		return err
	}
	// set local statement_timeout
	_, err = batchResults.Exec()
	if err != nil {
		return err
	}
	return nil
}

func ExecCommit(batchResults pgx.BatchResults) error {
	// COMMIT
	_, err := batchResults.Exec()
	return err
}
