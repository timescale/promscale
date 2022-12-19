package end_to_end_tests

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgmodel/metrics"
	"github.com/timescale/promscale/pkg/pgxconn"
)

func TestMetricsDuplicateInsert(t *testing.T) {
	ctx := context.Background()
	ts := generateSmallTimeseries()
	withDB(t, "metrics_duplicate_insert_test", func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()
		_, _, err = ingestor.IngestMetrics(ctx, newWriteRequestWithTs(copyMetrics(ts)))
		require.NoError(t, err)
		// Previous tests might have ingested duplicates.
		duplicatesBefore := testutil.ToFloat64(metrics.IngestorDuplicates.With(prometheus.Labels{"type": "metric", "kind": "sample"}))
		_, _, err = ingestor.IngestMetrics(ctx, newWriteRequestWithTs(copyMetrics(ts)))
		require.NoError(t, err)
		require.Greater(t, testutil.ToFloat64(metrics.IngestorDuplicates.With(prometheus.Labels{"type": "metric", "kind": "sample"})), duplicatesBefore, "duplicates insert must have occurred")
	})
}
