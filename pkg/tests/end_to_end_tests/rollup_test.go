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

func TestRollupCreationDeletion(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		rollupResolutions := []rollup.DownsampleResolution{
			{
				Label:      "short",
				Resolution: day.Duration(5 * time.Minute),
				Retention:  day.Duration(30 * 24 * time.Hour),
			},
		}

		pgCon, err := db.Acquire(context.Background())
		require.NoError(t, err)
		defer pgCon.Release()

		err = rollup.EnsureRollupWith(pgCon.Conn(), rollupResolutions)
		require.NoError(t, err)

		verifyRollupExistence(t, pgCon.Conn(), rollupResolutions[0].Label, time.Duration(rollupResolutions[0].Resolution), time.Duration(rollupResolutions[0].Retention), false)

		rollupResolutions = append(rollupResolutions, rollup.DownsampleResolution{
			Label:      "long",
			Resolution: day.Duration(time.Hour),
			Retention:  day.Duration(395 * 24 * time.Hour),
		})

		err = rollup.EnsureRollupWith(pgCon.Conn(), rollupResolutions)
		require.NoError(t, err)

		verifyRollupExistence(t, pgCon.Conn(), rollupResolutions[1].Label, time.Duration(rollupResolutions[1].Resolution), time.Duration(rollupResolutions[1].Retention), false)

		// Remove the first entry and see if the entry is removed or not.
		newRes := rollupResolutions[1:]
		err = rollup.EnsureRollupWith(pgCon.Conn(), newRes)
		require.NoError(t, err)
		// Check if long exists.
		verifyRollupExistence(t, pgCon.Conn(), rollupResolutions[1].Label, time.Duration(rollupResolutions[1].Resolution), time.Duration(rollupResolutions[1].Retention), false)
		// Check if short does not exist.
		verifyRollupExistence(t, pgCon.Conn(), rollupResolutions[0].Label, time.Duration(rollupResolutions[0].Resolution), time.Duration(rollupResolutions[0].Retention), true)
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
