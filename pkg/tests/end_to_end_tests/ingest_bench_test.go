package end_to_end_tests

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgxconn"
)

func BenchmarkTracingIngest(b *testing.B) {
	b.StopTimer()
	withDB(b, "bench_tracing_ingest", func(db *pgxpool.Pool, t testing.TB) {
		b.StopTimer()
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()
		traces := generateAllTraces(t)
		traceCount := len(traces)
		b.StartTimer()
		for n := 0; n < b.N; n++ {
			if n >= traceCount {
				break
			}
			err = ingestor.IngestTraces(context.Background(), traces[n])
			require.NoError(t, err)
		}
		b.StopTimer()
	})
}
