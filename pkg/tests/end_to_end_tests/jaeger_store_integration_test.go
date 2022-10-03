package end_to_end_tests

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/stretchr/testify/require"

	"github.com/timescale/promscale/pkg/jaeger/store"
	jaegerstore "github.com/timescale/promscale/pkg/jaeger/store"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/ingestor"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgxconn"
	jaeger_integration_tests "github.com/timescale/promscale/pkg/tests/end_to_end_tests/jaeger_store_integration_tests"
)

type spanCopyingWriter struct {
	w spanstore.Writer
}

// Copies Tags to avoid altering test expectation.
func (f spanCopyingWriter) WriteSpan(ctx context.Context, span *model.Span) error {
	spanCopy := *span
	spanCopy.Tags = append([]model.KeyValue{}, span.Tags...)
	spanCopy.References = append([]model.SpanRef{}, span.References...)
	process := *span.Process
	spanCopy.Process = &process
	spanCopy.Process.Tags = append([]model.KeyValue{}, span.Process.Tags...)
	spanCopy.Logs = append([]model.Log{}, span.Logs...)
	for i := range span.Logs {
		spanCopy.Logs[i].Fields = append([]model.KeyValue{}, span.Logs[i].Fields...)
	}
	return f.w.WriteSpan(ctx, &spanCopy)
}

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
					SpanWriter: spanCopyingWriter{writer},
					CleanUp: func() error {
						_, err := db.Exec(context.Background(), `TRUNCATE TABLE _ps_trace.span`)
						return err
					},
					Refresh: func() error { return nil },
					SkipList: []string{
						"FindTraces/Tags_\\+_Operation_name$",
					},
				}
				si.IntegrationTestAll(t.(*testing.T))
			})
		})
	}
}
