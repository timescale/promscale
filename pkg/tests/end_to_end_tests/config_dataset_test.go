package end_to_end_tests

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/dataset"
)

func TestDatasetConfigApply(t *testing.T) {

	withDB(t, *testDatabase, func(dbOwner *pgxpool.Pool, t testing.TB) {
		conn, err := dbOwner.Acquire(context.Background())
		require.NoError(t, err)
		defer conn.Release()

		pgxConn := conn.Conn()
		require.Equal(t, getDefaultChunkInterval(t, pgxConn), 8*time.Hour)

		cfg := dataset.Config{Metrics: dataset.Metrics{ChunkInterval: 4 * time.Hour}}

		err = cfg.Apply(pgxConn)
		require.NoError(t, err)

		require.Equal(t, getDefaultChunkInterval(t, pgxConn), 4*time.Hour)

		// Set to default if chunk interval is not specified.
		cfg.Metrics.ChunkInterval = 0

		err = cfg.Apply(pgxConn)
		require.NoError(t, err)

		require.Equal(t, getDefaultChunkInterval(t, pgxConn), 8*time.Hour)
	})
}

func getDefaultChunkInterval(t testing.TB, conn *pgx.Conn) (chunkInterval time.Duration) {
	err := conn.QueryRow(context.Background(), "SELECT _prom_catalog.get_default_chunk_interval()").Scan(&chunkInterval)
	if err != nil {
		t.Fatal("error getting default chunk interval", err)
	}
	return chunkInterval
}
