package end_to_end_tests

import (
	"runtime"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"

	jaeger_integration_tests "github.com/jaegertracing/jaeger/plugin/storage/integration"
	"github.com/timescale/promscale/pkg/jaeger/store"
	jaegerstore "github.com/timescale/promscale/pkg/jaeger/store"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/ingestor"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgxconn"
)

// Similar to TestQueryTraces, but uses Jaeger span ingestion interface.
func TestJaegerStorageIntegration(t *testing.T) {
	cases := []struct {
		name      string
		streaming bool
	}{
		{
			name:      "sequential",
			streaming: false,
		},
		{
			name:      "streaming",
			streaming: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			withDB(t, "jaeger_storage_integration_tests", func(db *pgxpool.Pool, t testing.TB) {
				cfg := &ingestor.Cfg{
					InvertedLabelsCacheSize: cache.DefaultConfig.InvertedLabelsCacheSize,
					NumCopiers:              runtime.NumCPU() / 2,
					TracesAsyncAcks:         true,
				}
				ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), cfg)
				require.NoError(t, err)
				defer ingestor.Close()

				jaegerStore := jaegerstore.New(pgxconn.NewQueryLoggingPgxConn(db), ingestor, &store.DefaultConfig)
				writer := jaegerStore.SpanWriter()
				if c.streaming {
					writer = jaegerStore.StreamingSpanWriter()
				}
				si := jaeger_integration_tests.StorageIntegration{
					SpanReader: jaegerStore.SpanReader(),
					SpanWriter: writer,
					CleanUp:    func() error { return nil },
					Refresh:    func() error { return nil },
				}
				si.IntegrationTestAll(t.(*testing.T))
			})
		})
	}
}
