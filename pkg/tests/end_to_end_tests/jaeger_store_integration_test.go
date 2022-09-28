package end_to_end_tests

import (
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/timescale/promscale/pkg/jaeger/store"
	jaegerstore "github.com/timescale/promscale/pkg/jaeger/store"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/ingestor"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgxconn"
	jaeger_integration_tests "github.com/timescale/promscale/pkg/tests/end_to_end_tests/jaeger_store_integration_tests"
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
					TracesAsyncAcks:         true, // To make GetLargeSpans happy, otherwise it takes quite a few time to ingest.
				}
				ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), cfg)
				require.NoError(t, err)
				defer ingestor.Close()

				jaegerStore := jaegerstore.New(pgxconn.NewQueryLoggingPgxConn(db), ingestor, &store.Config{
					MaxTraceDuration: 17 * time.Hour, // FindTraces/Trace_spans_over_multiple_indices test has events which has timestamp difference of ~17hrs when comparing to span.
				})
				writer := jaegerStore.SpanWriter()
				if c.streaming {
					writer = jaegerStore.StreamingSpanWriter()
				}
				si := jaeger_integration_tests.StorageIntegration{
					SpanReader: jaegerStore.SpanReader(),
					SpanWriter: writer,
					CleanUp:    func() error { return nil },
					Refresh:    func() error { return nil },
					SkipList: []string{
						"GetLargeSpans",
						"FindTraces/Tags_in_one_spot_-_Tags",
						"FindTraces/Tags_in_one_spot_-_Logs",
						"FindTraces/Tags_in_one_spot_-_Process",
						"FindTraces/default",
						"FindTraces/Tags_\\+_Operation_name$",
						"FindTraces/Tags_\\+_Operation_name_\\+_max_Duration$",
						"FindTraces/Tags_\\+_Operation_name_\\+_Duration_range$",
						"FindTraces/Tags_\\+_Duration_range$",
						"FindTraces/Tags_\\+_max_Duration$",
						"FindTraces/Multi-spot_Tags_\\+_Operation_name_\\+_max_Duration",
						"FindTraces/Multi-spot_Tags_\\+_Operation_name_\\+_Duration_range",
						"FindTraces/Multi-spot_Tags_\\+_Duration_range",
						"FindTraces/Multi-spot_Tags_\\+_max_Duration",
						"FindTraces/Multiple_Traces",
					},
				}
				si.IntegrationTestAll(t.(*testing.T))
			})
		})
	}
}
