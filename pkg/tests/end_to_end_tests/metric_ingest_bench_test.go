// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/tests/testsupport"
	"github.com/walle/targz"
)

var prometheusDataGzip = "../testdata/prometheus-data.tar.gz"

func TestPromLoader(t *testing.T) {
	data, err := extractPrometheusData(prometheusDataGzip, t.TempDir())
	require.NoError(t, err, "failed to extract prometheus data")
	loader, err := testsupport.NewPromLoader(data)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		require.NoError(t, loader.Close())
	}()
	it := loader.Iterator()
	sampleCounter := 0
	for it.Next() && sampleCounter < 10 {
		sample := it.Get()
		sampleCounter++

		t.Logf("%v , %v, %v", sample.Val.Samples[0].Timestamp, sample.Val.Samples[0].Value, sample.Val.Labels)
	}
	require.Equal(t, 10, sampleCounter, fmt.Sprintf("unexpected sample counter: %d", sampleCounter))
}

func extractPrometheusData(gzPath string, tmpDir string) (string, error) {
	return tmpDir + "/data", targz.Extract(gzPath, tmpDir)
}

func BenchmarkMetricIngest(b *testing.B) {
	data, err := extractPrometheusData(prometheusDataGzip, b.TempDir())
	if err != nil {
		b.Fatalf("failed to extract prometheus data: %v", err)
	}
	loader, err := testsupport.NewPromLoader(data)
	require.NoError(b, err)
	defer func() {
		if err := loader.Close(); err != nil {
			b.Fatal(err)
		}
	}()

	sampleLoader := testsupport.NewSampleIngestor(10, loader.Iterator(), 2000, 0)

	withDB(b, "bench_e2e_metric_ingest", func(db *pgxpool.Pool, t testing.TB) {
		b.StopTimer()
		metricsIngestor, err := ingestor.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), &ingestor.Cfg{NumCopiers: 8})
		require.NoError(t, err)
		defer metricsIngestor.Close()
		b.ResetTimer()
		b.ReportAllocs()
		b.StartTimer()
		sampleLoader.Run(metricsIngestor.Ingest)
		b.StopTimer()
	})
}
