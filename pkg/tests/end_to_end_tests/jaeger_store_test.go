package end_to_end_tests

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	jaegertranslator "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger"
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

		fixtures, err := getFindTraceTestFixtures()
		if err != nil {
			require.NoError(t, err)
		}
		batch, err := jaegertranslator.ProtoFromTraces(fixtures.traces)
		require.NoError(t, err)
		for _, b := range batch {
			for _, s := range b.Spans {
				// ProtoFromTraces doesn't populates span.Process because it is already been exposed by batch.Process.
				// See https://github.com/jaegertracing/jaeger-idl/blob/05fe64e9c305526901f70ff692030b388787e388/proto/api_v2/model.proto#L152-L160
				s.Process = b.Process
				err = jaegerStore.SpanWriter().WriteSpan(context.Background(), s)
				require.NoError(t, err)
			}
		}

		getOperationsTest(t, jaegerStore)
		findTraceTest(t, jaegerStore, fixtures)
		getDependenciesTest(t, jaegerStore)
	})
}
