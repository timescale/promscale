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

func TestQueryWithTimeoutErr(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		defer db.Close()
		c := pgxconn.NewPgxConn(db)
		defer c.Close()
		// Given a query that returns corretly
		rows, commit, err := pgxconn.QueryWithTimeout(context.Background(), c, 1000, "SELECT 1")
		require.NoError(t, err)
		defer rows.Close()
		require.NoError(t, rows.Err())
		require.True(t, rows.Next())
		var queryR int
		_ = rows.Scan(&queryR)
		require.Equal(t, 1, queryR)
		require.NoError(t, commit())

		// When the same query is given a very small timeout (1 ms)
		rows, _, err = pgxconn.QueryWithTimeout(context.Background(), c, 1, "SELECT pg_sleep(1)")
		if err == nil {
			defer rows.Close()
			require.False(t, rows.Next())
			err = rows.Err()
		}
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
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		_, _, err := pgxconn.QueryWithTimeoutFromCtx(ctx, c, "SELECT pg_sleep(1)")
		assert.ErrorContains(t, err, "timeout: context deadline exceeded")
	})
}
