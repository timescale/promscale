// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/rollup"
)

func TestRollupQueryHelper(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		_, err := db.Exec(context.Background(), "SELECT prom_api.set_automatic_downsample($1)", true)
		require.NoError(t, err)
		_, err = db.Exec(context.Background(), "CALL _prom_catalog.create_rollup('short', interval '5 minutes', interval '30 days')")
		require.NoError(t, err)
		_, err = db.Exec(context.Background(), "CALL _prom_catalog.create_rollup('medium', interval '15 minutes', interval '30 days')")
		require.NoError(t, err)
		_, err = db.Exec(context.Background(), "CALL _prom_catalog.create_rollup('long', interval '1 hour', interval '30 days')")
		require.NoError(t, err)
		_, err = db.Exec(context.Background(), "CALL _prom_catalog.create_rollup('very_long', interval '1 week', interval '30 days')")
		require.NoError(t, err)

		var numRollups int
		err = db.QueryRow(context.Background(), "SELECT count(*) FROM _prom_catalog.rollup").Scan(&numRollups)
		require.NoError(t, err)
		require.Equal(t, 4, numRollups)

		helper, err := rollup.NewDecider(context.Background(), pgxconn.NewPgxConn(db), rollup.DefaultScrapeInterval)
		require.NoError(t, err)
		require.NotNil(t, helper)

		const originalSchema = "prom_data"

		tcs := []struct {
			name               string
			min                time.Duration
			max                time.Duration
			expectedSchemaName string
		}{
			{
				name:               "1 sec",
				min:                0,
				max:                time.Second,
				expectedSchemaName: originalSchema,
			}, {
				name:               "5 min",
				min:                0,
				max:                5 * time.Minute,
				expectedSchemaName: originalSchema,
			}, {
				name:               "30 mins",
				min:                0,
				max:                30 * time.Minute,
				expectedSchemaName: originalSchema,
			}, {
				name:               "1 hour",
				min:                0,
				max:                time.Hour,
				expectedSchemaName: originalSchema,
			}, {
				name:               "1 day",
				min:                0,
				max:                24 * time.Hour,
				expectedSchemaName: originalSchema,
			},
			{
				name:               "7 days",
				min:                0,
				max:                7 * 24 * time.Hour,
				expectedSchemaName: "ps_short",
			},
			{
				name:               "30 days",
				min:                0,
				max:                30 * 24 * time.Hour,
				expectedSchemaName: "ps_medium",
			}, {
				name:               "1 year",
				min:                0,
				max:                12 * 30 * 24 * time.Hour,
				expectedSchemaName: "ps_very_long",
			}, {
				name:               "100 years",
				min:                0,
				max:                100 * 12 * 30 * 24 * time.Hour,
				expectedSchemaName: "ps_very_long",
			},
		}
		for _, tc := range tcs {
			recommendedSchema := helper.Decide(int64(tc.min.Seconds()), int64(tc.max.Seconds()))
			require.Equal(t, tc.expectedSchemaName, recommendedSchema, tc.name)
		}
	})
}
