package end_to_end_tests

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/jaeger/store"
	jaegerstore "github.com/timescale/promscale/pkg/jaeger/store"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgxconn"
)

// Similar to TestQueryTraces, but uses Jaeger span ingestion interface.
func TestJaegerSpanIngestion(t *testing.T) {
	withDB(t, "jaeger_span_store_e2e", func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()

		jaegerStore := jaegerstore.New(pgxconn.NewQueryLoggingPgxConn(db), ingestor, &store.DefaultConfig)

		fixtures, err := getTracesFixtures()
		if err != nil {
			require.NoError(t, err)
		}
		for _, b := range fixtures.batches {
			for _, s := range b.Spans {
				err = jaegerStore.SpanWriter().WriteSpan(context.Background(), s)
				require.NoError(t, err)
			}
		}

		getOperationsTest(t, jaegerStore)
		findTraceTest(t, jaegerStore, fixtures)
		getDependenciesTest(t, jaegerStore)
	})
}
