// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgxconn"
)

func TestQueryRowWithTimeoutErr(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		c := pgxconn.NewPgxConn(db)
		defer c.Close()
		// Given a query that returns corretly
		row, closeFn, err := pgxconn.QueryRowWithTimeout(context.Background(), c, 1000, "SELECT 1")
		require.NoError(t, err)
		defer func() {
			_ = closeFn()
		}()
		var a int
		err = row.Scan(&a)
		require.NoError(t, err)
		assert.Equal(t, 1, a)

		// When the same query is given a very small timeout (1 ms)
		row2, closeFn2, err := pgxconn.QueryRowWithTimeout(context.Background(), c, 1, "SELECT pg_sleep(1)")
		require.NoError(t, err)
		defer func() {
			_ = closeFn2()
		}()
		err = row2.Scan(&a)
		// It's cancelled due to a timeout
		assert.ErrorContains(t, err, "ERROR: canceling statement due to statement timeout (SQLSTATE 57014)")
	})
}

func TestQueryWithTimeoutErr(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		c := pgxconn.NewPgxConn(db)
		defer c.Close()
		// Given a query that returns corretly
		rows, closeFn, err := pgxconn.QueryWithTimeout(context.Background(), c, 1000, "SELECT 1")
		require.NoError(t, err)
		defer func() {
			_ = closeFn()
		}()
		require.NoError(t, rows.Err())
		require.True(t, rows.Next())
		var queryR int
		_ = rows.Scan(&queryR)
		require.Equal(t, 1, queryR)

		// When the same query is given a very small timeout (1 ms)
		rows2, closeFn2, err := pgxconn.QueryWithTimeout(context.Background(), c, 1, "SELECT pg_sleep(1)")
		if err == nil {
			defer func() {
				_ = closeFn2()
			}()
			require.False(t, rows2.Next())
			err = rows2.Err()
		}
		// It's cancelled due to a timeout
		assert.ErrorContains(t, err, "ERROR: canceling statement due to statement timeout (SQLSTATE 57014)")
	})
}

func TestQueryWithCanceledCtx(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		defer db.Close()
		c := pgxconn.NewPgxConn(db)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
		defer cancel()
		_, _, err := pgxconn.QueryWithTimeoutFromCtx(ctx, c, "SELECT pg_sleep(1)")
		assert.ErrorContains(t, err, "timeout: context deadline exceeded")
	})
}

func TestBatchWithTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		defer db.Close()
		conn := pgxconn.NewPgxConn(db)
		batch := pgxconn.NewBatchWithTimeout(conn, 1)
		batch.Queue("SELECT pg_sleep(1)")
		r, err := pgxconn.SendBatch(context.Background(), conn, batch, true)
		require.NoError(t, err)
		defer func() {
			_ = r.Close()
		}()
		_, err = r.Exec()
		assert.ErrorContains(t, err, "ERROR: canceling statement due to statement timeout (SQLSTATE 57014)")
	})
}

func TestBatchWithTimeoutFromCtx(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		conn := pgxconn.NewPgxConn(db)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
		defer cancel()
		batch, sendBatchFn := pgxconn.NewBatchWithTimeoutFromCtx(ctx, conn)
		batch.Queue("SELECT pg_sleep(1)")
		_, err := sendBatchFn()
		assert.ErrorContains(t, err, "timeout: context deadline exceeded")
	})
}
