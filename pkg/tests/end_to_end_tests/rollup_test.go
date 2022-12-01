// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/timescale/promscale/pkg/internal/day"
	"github.com/timescale/promscale/pkg/rollup"
)

func TestRollupSync(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		rollupResolutions := rollup.Resolutions{
			"short": {
				Resolution: day.Duration(5 * time.Minute),
				Retention:  day.Duration(30 * 24 * time.Hour),
			},
		}

		pgCon, err := db.Acquire(context.Background())
		require.NoError(t, err)
		defer pgCon.Release()

		// Test 1: Check if 'short' rollup is created.
		err = rollup.Sync(context.Background(), pgCon.Conn(), rollupResolutions)
		require.NoError(t, err)

		verifyRollupExistence(t, pgCon.Conn(), "short",
			time.Duration(rollupResolutions["short"].Resolution), time.Duration(rollupResolutions["short"].Retention), false)

		rollupResolutions["long"] = rollup.Definition{
			Resolution: day.Duration(time.Hour),
			Retention:  day.Duration(395 * 24 * time.Hour),
		}

		// Test 2: Check if 'long' rollup is created.
		err = rollup.Sync(context.Background(), pgCon.Conn(), rollupResolutions)
		require.NoError(t, err)

		verifyRollupExistence(t, pgCon.Conn(), "long",
			time.Duration(rollupResolutions["long"].Resolution), time.Duration(rollupResolutions["long"].Retention), false)

		// Test 3: Update the resolution and check if error is returned.
		rollupResolutions["short"] = rollup.Definition{
			Resolution: day.Duration(4 * time.Minute),
			Retention:  day.Duration(30 * 24 * time.Hour),
		}
		err = rollup.Sync(context.Background(), pgCon.Conn(), rollupResolutions)
		require.Equal(t,
			"error on existing resolution mismatch: existing rollup resolutions cannot be updated. Either keep the resolution of existing rollup labels same or remove them",
			err.Error())
		// Reset back to original resolution.
		rollupResolutions["short"] = rollup.Definition{
			Resolution: day.Duration(5 * time.Minute),
			Retention:  day.Duration(30 * 24 * time.Hour),
		}

		// Test 4: Remove the first entry and see if the entry is removed or not.
		rollupResolutions["short"] = rollup.Definition{
			Resolution: day.Duration(5 * time.Minute),
			Retention:  day.Duration(30 * 24 * time.Hour),
			Delete:     true,
		}
		err = rollup.Sync(context.Background(), pgCon.Conn(), rollupResolutions)
		require.NoError(t, err)
		// Check if long exists.
		verifyRollupExistence(t, pgCon.Conn(), "long",
			time.Duration(rollupResolutions["long"].Resolution), time.Duration(rollupResolutions["long"].Retention), false)
		// Check if short does not exist.
		verifyRollupExistence(t, pgCon.Conn(), "short",
			time.Duration(rollupResolutions["short"].Resolution), time.Duration(rollupResolutions["short"].Retention), true)

		// Test 5: Update retention of long and check if the same is reflected in the DB.
		rollupResolutions["long"] = rollup.Definition{
			Resolution: day.Duration(time.Hour),
			Retention:  day.Duration(500 * 24 * time.Hour), // Updated retention duration.
		}
		err = rollup.Sync(context.Background(), pgCon.Conn(), rollupResolutions)
		require.NoError(t, err)
		verifyRollupExistence(t, pgCon.Conn(), "long",
			time.Duration(rollupResolutions["long"].Resolution), time.Duration(rollupResolutions["long"].Retention), false)
		// Short should still not exists.
		verifyRollupExistence(t, pgCon.Conn(), "short",
			time.Duration(rollupResolutions["short"].Resolution), time.Duration(rollupResolutions["short"].Retention), true)
	})
}

func verifyRollupExistence(t testing.TB, pgCon *pgx.Conn, name string, resolution, retention time.Duration, shouldError bool) {
	var (
		rName       string
		rResolution time.Duration
		rRetention  time.Duration
	)
	err := pgCon.QueryRow(context.Background(), "SELECT name, resolution, retention FROM _prom_catalog.rollup WHERE name = $1", name).Scan(&rName, &rResolution, &rRetention)
	if shouldError {
		require.Error(t, err)
		return
	}
	require.NoError(t, err)
	require.Equal(t, resolution, rResolution)
	require.Equal(t, retention, rRetention)
}
