package end_to_end_tests

import (
	"context"
	"sort"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/timescale/promscale/pkg/pgmodel/cache"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgxconn"
)

func TestSeriesCacheRefreshOnStaleSeries(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		conn := pgxconn.NewPgxConn(db)
		ingestor, err := ingstr.NewPgxIngestorForTests(conn, nil)
		require.NoError(t, err)
		defer ingestor.Close()

		ts := copyMetrics(generateLargeTimeseries())
		samples, metadata, err := ingestor.Ingest(context.Background(), newWriteRequestWithTs(ts))
		require.NoError(t, err)
		require.NotEqual(t, 0, samples)
		require.NotEqual(t, 0, metadata)

		sCache := ingestor.SeriesCache().(*cache.SeriesCacheImpl)
		require.Equal(t, 9, sCache.Len()) // 9 series are present in generateLargeTimeseries

		// Mark some series in the db as stale by assigning some epoch.
		_, err = db.Exec(context.Background(), "UPDATE _prom_catalog.series SET delete_epoch = 1 WHERE id < 3")
		require.NoError(t, err)

		staleSeriesIds, err := ingstr.GetStaleSeriesIDs(conn)
		require.NoError(t, err)

		staleSeriesIdsInt := int64Toint(staleSeriesIds)
		sort.Ints(staleSeriesIdsInt)
		require.Equal(t, []int{1, 2}, staleSeriesIdsInt)

		// Clean up the stale series in the cache.
		require.Equal(t, 2, sCache.ClearStaleSeries(staleSeriesIds))

		// Stale series should be equal to the number of series with epoch in the db.
		require.Equal(t, 2, sCache.StaleSeries())
	})
}

func int64Toint(a []int64) []int {
	var b []int
	for _, elem := range a {
		b = append(b, int(elem))
	}
	return b
}
