package end_to_end_tests

import (
	"context"
	"runtime"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/ingestor"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/tests/testdata"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func BenchmarkTracesIngest(b *testing.B) {
	cases := []struct {
		name  string
		async bool
	}{
		{
			name:  "wait for DB write/ack",
			async: false,
		},
		{
			name:  "don't wait for DB write/ack",
			async: true,
		},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			withDB(b, "trace_ingest_bench", func(db *pgxpool.Pool, t testing.TB) {
				cfg := &ingestor.Cfg{
					InvertedLabelsCacheSize: cache.DefaultConfig.InvertedLabelsCacheSize,
					NumCopiers:              runtime.NumCPU() / 2,
					TracesAsyncAcks:         c.async,
				}
				ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), cfg)
				require.NoError(t, err)
				defer func() {
					ingestor.Close()
					var cnt int
					err = db.QueryRow(context.Background(), `SELECT count(*) FROM _ps_trace.span`).Scan(&cnt)
					require.Equal(t, 248778, cnt)
				}()
				traces := testdata.LoadTraces(t, 1000)
				b.ResetTimer()
				b.ReportAllocs()
				maxParallelReqs := make(chan ptrace.Traces, 50) // simulating Jaeger sending parallel reqs
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					// send traces
					for i := 0; i < len(traces); i++ {
						maxParallelReqs <- traces[i]
					}
					close(maxParallelReqs)
				}()
				for i := 0; i < 50; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for r := range maxParallelReqs {
							if err := ingestor.IngestTraces(context.Background(), r); err != nil {
								t.Error(err)
							}
						}
					}()
				}
				wg.Wait()
			})
		})
	}
}
