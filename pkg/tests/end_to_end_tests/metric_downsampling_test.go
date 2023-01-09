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

	"github.com/timescale/promscale/pkg/downsample"
	"github.com/timescale/promscale/pkg/internal/day"
)

func TestMetricDownsampleSync(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		downsamplingCfgs := []downsample.Config{
			{Interval: day.Duration(time.Minute * 5), Retention: day.Duration(time.Hour * 24 * 30)},
		}

		pgCon, err := db.Acquire(context.Background())
		require.NoError(t, err)
		defer pgCon.Release()

		ctx := context.Background()
		pc := pgCon.Conn()

		// Test 1: Check if 'ds_5m' downsampling is created.
		err = downsample.Sync(ctx, pc, downsamplingCfgs)
		require.NoError(t, err)

		verifyDownsamplingExistence(t, pc, "ds_5m",
			downsamplingCfgs[0].Interval, downsamplingCfgs[0].Retention, false)

		downsamplingCfgs = append(downsamplingCfgs, downsample.Config{Interval: day.Duration(time.Hour), Retention: day.Duration(time.Hour * 24 * 365)})
		// Test 2: Check if 'ds_1h' downsampling is created.
		err = downsample.Sync(ctx, pc, downsamplingCfgs)
		require.NoError(t, err)

		verifyDownsamplingExistence(t, pc, "ds_1h",
			downsamplingCfgs[1].Interval, downsamplingCfgs[1].Retention, false)

		// Test 3: Remove the first entry and see if the entry is disabled or not.
		downsamplingCfgs = downsamplingCfgs[1:]
		err = downsample.Sync(ctx, pc, downsamplingCfgs)
		require.NoError(t, err)
		// Check if ds_1h exists.
		verifyDownsamplingExistence(t, pc, "ds_1h",
			downsamplingCfgs[0].Interval, downsamplingCfgs[0].Retention, false)
		// Check if ds_5m is disabled.
		verifyDownsamplingExistence(t, pc, "ds_5m",
			day.Duration(time.Minute*5), day.Duration(time.Hour*24*30), true)

		// Test 4: Update retention of ds_1h and check if the same is reflected in the DB.
		downsamplingCfgs[0].Retention = day.Duration(time.Hour * 24 * 500)
		err = downsample.Sync(ctx, pc, downsamplingCfgs)
		require.NoError(t, err)
		verifyDownsamplingExistence(t, pc, "ds_1h",
			downsamplingCfgs[0].Interval, downsamplingCfgs[0].Retention, false)
		// ds_5m should still be disabled.
		verifyDownsamplingExistence(t, pc, "ds_5m",
			day.Duration(time.Minute*5), day.Duration(time.Hour*24*30), true)

		// Test 5: Enable the ds_5m downsampling that was already in the database.
		downsamplingCfgs = append(downsamplingCfgs, downsample.Config{Interval: day.Duration(time.Minute * 5), Retention: day.Duration(time.Hour * 24 * 30)})
		err = downsample.Sync(ctx, pc, downsamplingCfgs)
		require.NoError(t, err)
		verifyDownsamplingExistence(t, pc, "ds_5m",
			downsamplingCfgs[1].Interval, downsamplingCfgs[1].Retention, false)

		// Test 6: Add a resolution similar to 5m, but with different unit. This should error.
		downsamplingCfgs = append(downsamplingCfgs, downsample.Config{Interval: day.Duration(time.Second * 300), Retention: day.Duration(time.Hour * 24 * 30)})
		err = downsample.Sync(ctx, pc, downsamplingCfgs)
		require.Error(t, err)
	})
}

func verifyDownsamplingExistence(t testing.TB, pgCon *pgx.Conn, schemaName string, interval, retention day.Duration, shouldBeDisabled bool) {
	var (
		dSchemaName    string
		dInterval      time.Duration
		dRetention     time.Duration
		dShouldRefresh bool
	)
	err := pgCon.QueryRow(context.Background(), "SELECT schema_name, ds_interval, retention, should_refresh FROM _prom_catalog.downsample WHERE schema_name = $1", schemaName).Scan(&dSchemaName, &dInterval, &dRetention, &dShouldRefresh)
	require.NoError(t, err)
	require.Equal(t, schemaName, dSchemaName)
	require.Equal(t, time.Duration(interval), dInterval)
	require.Equal(t, time.Duration(retention), dRetention)
	require.Equal(t, shouldBeDisabled, !dShouldRefresh)
}
