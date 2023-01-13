package end_to_end_tests

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/timescale/promscale/pkg/internal/testhelpers"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgmodel/metrics/database"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/tests/testdata"
	"github.com/timescale/promscale/pkg/util"
)

func TestDatabaseMetrics(t *testing.T) {
	withDB(t, *testDatabase, func(dbOwner *pgxpool.Pool, t testing.TB) {
		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_writer")
		defer db.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		dbMetrics := database.NewEngine(ctx, pgxconn.NewPgxConn(db))

		// Before updating the metrics.
		compressionStatus := getMetricValue(t, "compression_status")
		require.Equal(t, float64(0), compressionStatus)
		numMaintenanceJobs := getMetricValue(t, "worker_maintenance_job")
		require.Equal(t, float64(0), numMaintenanceJobs)
		metricsJobDuration := getMetricValue(t, "worker_maintenance_job_metrics_compression_last_duration_seconds")
		require.Equal(t, float64(0), metricsJobDuration)
		chunksCount := getMetricValue(t, "chunks_count")
		require.Equal(t, float64(0), chunksCount)
		chunksCompressedCount := getMetricValue(t, "chunks_compressed_count")
		require.Equal(t, float64(0), chunksCompressedCount)
		chunksMUncompressedCount := getMetricValue(t, "chunks_metrics_uncompressed_count")
		require.Equal(t, float64(0), chunksMUncompressedCount)
		chunksMExpiredCount := getMetricValue(t, "chunks_metrics_expired_count")
		require.Equal(t, float64(0), chunksMExpiredCount)
		chunksTUncompressedCount := getMetricValue(t, "chunks_traces_uncompressed_count")
		require.Equal(t, float64(0), chunksTUncompressedCount)
		chunksTExpiredCount := getMetricValue(t, "chunks_traces_expired_count")
		require.Equal(t, float64(0), chunksTExpiredCount)

		// Update the metrics.
		require.NoError(t, dbMetrics.Update())

		// After updating the metrics.
		compressionStatus = getMetricValue(t, "compression_status")
		require.Equal(t, float64(1), compressionStatus)
		numMaintenanceJobs = getMetricValue(t, "worker_maintenance_job")
		require.GreaterOrEqual(t, numMaintenanceJobs, float64(1))
		metricsJobDuration = getMetricValue(t, "worker_maintenance_job_metrics_compression_last_duration_seconds")
		require.Equal(t, float64(0), metricsJobDuration)
		chunksCount = getMetricValue(t, "chunks_count")
		require.Equal(t, float64(0), chunksCount)
		chunksCompressedCount = getMetricValue(t, "chunks_compressed_count")
		require.Equal(t, float64(0), chunksCompressedCount)
		chunksMUncompressedCount = getMetricValue(t, "chunks_metrics_uncompressed_count")
		require.Equal(t, float64(0), chunksMUncompressedCount)
		chunksMExpiredCount = getMetricValue(t, "chunks_metrics_expired_count")
		require.Equal(t, float64(0), chunksMExpiredCount)
		chunksTUncompressedCount = getMetricValue(t, "chunks_traces_uncompressed_count")
		require.Equal(t, float64(0), chunksTUncompressedCount)
		chunksTExpiredCount = getMetricValue(t, "chunks_traces_expired_count")
		require.Equal(t, float64(0), chunksTExpiredCount)

		// Ingest some data and then see check the metrics to ensure proper updating.
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()

		require.NoError(t, ingestor.IngestTraces(context.Background(), testdata.GenerateTestTrace()))

		// Update the metrics again.
		require.NoError(t, dbMetrics.Update())

		chunksCount = getMetricValue(t, "chunks_count")
		require.Equal(t, float64(3), chunksCount)
		chunksCompressedCount = getMetricValue(t, "chunks_compressed_count")
		// we expect these metrics to be delayed by customPollConf
		require.Equal(t, float64(0), chunksCompressedCount)
		chunksMUncompressedCount = getMetricValue(t, "chunks_metrics_uncompressed_count")
		require.Equal(t, float64(0), chunksMUncompressedCount)
		chunksMExpiredCount = getMetricValue(t, "chunks_metrics_expired_count")
		require.Equal(t, float64(0), chunksMExpiredCount)
		chunksTUncompressedCount = getMetricValue(t, "chunks_traces_uncompressed_count")
		require.Equal(t, float64(0), chunksTUncompressedCount)
		chunksTExpiredCount = getMetricValue(t, "chunks_traces_expired_count")
		require.Equal(t, float64(0), chunksTExpiredCount)
	})
}

func TestDatabaseMetricsAfterCompression(t *testing.T) {
	ts := generateSmallTimeseries()
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()
		_, _, err = ingestor.IngestMetrics(context.Background(), newWriteRequestWithTs(copyMetrics(ts)))
		require.NoError(t, err)
		err = ingestor.CompleteMetricCreation(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		dbMetrics := database.NewEngine(ctx, pgxconn.NewPgxConn(db))

		// Update the metrics.
		require.NoError(t, dbMetrics.Update())
		// Get metrics before compressing the firstMetric metric chunk.
		compressionStatus := getMetricValue(t, "compression_status")
		require.Equal(t, float64(1), compressionStatus)
		chunksCount := getMetricValue(t, "chunks_count")
		require.Equal(t, float64(2), chunksCount)
		chunksCompressedCount := getMetricValue(t, "chunks_compressed_count")
		require.Equal(t, float64(0), chunksCompressedCount)
		chunksMUncompressedCount := getMetricValue(t, "chunks_metrics_uncompressed_count")
		// we expect these metrics to be delayed by customPollConf
		require.Equal(t, float64(0), chunksMUncompressedCount)
		chunksMExpiredCount := getMetricValue(t, "chunks_metrics_expired_count")
		require.Equal(t, float64(0), chunksMExpiredCount)
		chunksTUncompressedCount := getMetricValue(t, "chunks_traces_uncompressed_count")
		require.Equal(t, float64(0), chunksTUncompressedCount)
		chunksTExpiredCount := getMetricValue(t, "chunks_traces_expired_count")
		require.Equal(t, float64(0), chunksTExpiredCount)

		_, err = db.Exec(context.Background(), `SELECT public.compress_chunk(i) from public.show_chunks('prom_data."firstMetric"') i;`)
		require.NoError(t, err)

		// Update the metrics after compression.
		require.NoError(t, dbMetrics.Update())
		chunksCount = getMetricValue(t, "chunks_count")
		require.Equal(t, float64(2), chunksCount)
		chunksCompressedCount = getMetricValue(t, "chunks_compressed_count")
		require.Equal(t, float64(1), chunksCompressedCount)
		chunksMUncompressedCount = getMetricValue(t, "chunks_metrics_uncompressed_count")
		require.Equal(t, float64(0), chunksMUncompressedCount)
		chunksMExpiredCount = getMetricValue(t, "chunks_metrics_expired_count")
		require.Equal(t, float64(0), chunksMExpiredCount)
		chunksTUncompressedCount = getMetricValue(t, "chunks_traces_uncompressed_count")
		require.Equal(t, float64(0), chunksTUncompressedCount)
		chunksTExpiredCount = getMetricValue(t, "chunks_traces_expired_count")
		require.Equal(t, float64(0), chunksTExpiredCount)
	})
}

func getMetricValue(t testing.TB, name string) float64 {
	metric, err := database.GetMetric(name)
	require.NoError(t, err)

	val, err := util.ExtractMetricValue(metric)
	require.NoError(t, err)
	return val
}
