package end_to_end_tests

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/dataset"
	"github.com/timescale/promscale/pkg/pgmodel/model"
)

func TestDatasetConfigApply(t *testing.T) {
	withDB(t, *testDatabase, func(dbOwner *pgxpool.Pool, t testing.TB) {
		conn, err := dbOwner.Acquire(context.Background())
		require.NoError(t, err)
		defer conn.Release()
		disableCompression := false

		pgxConn := conn.Conn()
		require.NoError(t, model.RegisterCustomPgTypes(context.Background(), pgxConn))
		require.Equal(t, 8*time.Hour, getMetricsDefaultChunkInterval(t, pgxConn))
		require.Equal(t, true, getMetricsDefaultCompressionSetting(t, pgxConn))
		require.Equal(t, 10*time.Second, getMetricsDefaultHALeaseRefresh(t, pgxConn))
		require.Equal(t, 1*time.Minute, getMetricsDefaultHALeaseTimeout(t, pgxConn))
		require.Equal(t, 90*24*time.Hour, getMetricsDefaultRetention(t, pgxConn))
		require.Equal(t, 30*24*time.Hour, getTracesDefaultRetention(t, pgxConn))

		cfg := dataset.Config{
			Metrics: dataset.Metrics{
				ChunkInterval:   dataset.DayDuration(4 * time.Hour),
				Compression:     &disableCompression,
				HALeaseRefresh:  dataset.DayDuration(15 * time.Second),
				HALeaseTimeout:  dataset.DayDuration(2 * time.Minute),
				RetentionPeriod: dataset.DayDuration(15 * 24 * time.Hour),
			},
			Traces: dataset.Traces{
				RetentionPeriod: dataset.DayDuration(10 * 24 * time.Hour),
			},
		}

		err = cfg.Apply(pgxConn)
		require.NoError(t, err)

		require.Equal(t, 4*time.Hour, getMetricsDefaultChunkInterval(t, pgxConn))
		require.Equal(t, false, getMetricsDefaultCompressionSetting(t, pgxConn))
		require.Equal(t, 15*time.Second, getMetricsDefaultHALeaseRefresh(t, pgxConn))
		require.Equal(t, 2*time.Minute, getMetricsDefaultHALeaseTimeout(t, pgxConn))
		require.Equal(t, 15*24*time.Hour, getMetricsDefaultRetention(t, pgxConn))
		require.Equal(t, 10*24*time.Hour, getTracesDefaultRetention(t, pgxConn))

		// Set to default if chunk interval is not specified.
		cfg = dataset.Config{}

		err = cfg.Apply(pgxConn)
		require.NoError(t, err)

		require.Equal(t, 8*time.Hour, getMetricsDefaultChunkInterval(t, pgxConn))
		require.Equal(t, true, getMetricsDefaultCompressionSetting(t, pgxConn))
		require.Equal(t, 10*time.Second, getMetricsDefaultHALeaseRefresh(t, pgxConn))
		require.Equal(t, 1*time.Minute, getMetricsDefaultHALeaseTimeout(t, pgxConn))
		require.Equal(t, 90*24*time.Hour, getMetricsDefaultRetention(t, pgxConn))
		require.Equal(t, 30*24*time.Hour, getTracesDefaultRetention(t, pgxConn))
	})
}

func getMetricsDefaultChunkInterval(t testing.TB, conn *pgx.Conn) (chunkInterval time.Duration) {
	err := conn.QueryRow(context.Background(), "SELECT _prom_catalog.get_default_chunk_interval()").Scan(&chunkInterval)
	if err != nil {
		t.Fatal("error getting default metric chunk interval", err)
	}
	return chunkInterval
}
func getMetricsDefaultCompressionSetting(t testing.TB, conn *pgx.Conn) (compressionSetting bool) {
	err := conn.QueryRow(context.Background(), "SELECT _prom_catalog.get_default_compression_setting()").Scan(&compressionSetting)
	if err != nil {
		t.Fatal("error getting default metric compression setting", err)
	}
	return compressionSetting
}
func getMetricsDefaultHALeaseRefresh(t testing.TB, conn *pgx.Conn) (haRefresh time.Duration) {
	err := conn.QueryRow(context.Background(), "SELECT _prom_catalog.get_default_value('ha_lease_refresh')::interval").Scan(&haRefresh)
	if err != nil {
		t.Fatal("error getting default metric HA lease refresh duration", err)
	}
	return haRefresh
}
func getMetricsDefaultHALeaseTimeout(t testing.TB, conn *pgx.Conn) (haTimeout time.Duration) {
	err := conn.QueryRow(context.Background(), "SELECT _prom_catalog.get_default_value('ha_lease_timeout')::interval").Scan(&haTimeout)
	if err != nil {
		t.Fatal("error getting default metric HA lease timeout duration", err)
	}
	return haTimeout
}
func getMetricsDefaultRetention(t testing.TB, conn *pgx.Conn) (retention time.Duration) {
	err := conn.QueryRow(context.Background(), "SELECT _prom_catalog.get_default_retention_period()").Scan(&retention)
	if err != nil {
		t.Fatal("error getting default metric retention period", err)
	}
	return retention
}
func getTracesDefaultRetention(t testing.TB, conn *pgx.Conn) (retention time.Duration) {
	err := conn.QueryRow(context.Background(), "SELECT ps_trace.get_trace_retention_period()").Scan(&retention)
	if err != nil {
		t.Fatal("error getting default metric retention period", err)
	}
	return retention
}
