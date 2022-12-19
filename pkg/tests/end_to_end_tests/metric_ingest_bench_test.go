// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/walle/targz"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/tests/testsupport"
)

var prometheusDataGzip = "../testdata/prometheus-data.tar.gz"

func TestPromLoader(t *testing.T) {
	data, err := extractPrometheusData(prometheusDataGzip, t.TempDir())
	require.NoError(t, err, "failed to extract prometheus data")
	loader, err := testsupport.NewPromLoader(data, false)
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
	loader, err := testsupport.NewPromLoader(data, true) // load whole dataset in memory so we can better track allocations during ingest
	require.NoError(b, err)
	defer func() {
		if err := loader.Close(); err != nil {
			b.Fatal(err)
		}
	}()

	sampleLoader := testsupport.NewSampleIngestor(10, loader.Iterator(), 2000, 0)

	withDB(b, "bench_e2e_metric_ingest", func(db *pgxpool.Pool, t testing.TB) {
		b.StopTimer()
		metricsIngestor, err := ingestor.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), &ingestor.Cfg{
			NumCopiers:              8,
			InvertedLabelsCacheSize: cache.DefaultConfig.InvertedLabelsCacheSize,
		})
		require.NoError(t, err)
		defer metricsIngestor.Close()
		b.ResetTimer()
		b.ReportAllocs()
		b.StartTimer()
		sampleLoader.Run(metricsIngestor.IngestMetrics)
		b.StopTimer()
	})
}

func BenchmarkNewSeriesIngestion(b *testing.B) {
	benchmarks := []struct {
		name               string
		numMetrics         int
		numSeriesPerMetric int
		numLabelsPerMetric int
		batchSize          int
	}{
		{
			name:               "small series",
			numMetrics:         100,
			numSeriesPerMetric: 10,
			numLabelsPerMetric: 4,
			batchSize:          100,
		},
		{
			name:               "medium series",
			numMetrics:         100,
			numSeriesPerMetric: 100,
			numLabelsPerMetric: 4,
			batchSize:          1000,
		},
		{
			name:               "large series",
			numMetrics:         100,
			numSeriesPerMetric: 500,
			numLabelsPerMetric: 4,
			batchSize:          10000,
		},
	}

	for _, benchmark := range benchmarks {
		benchName := fmt.Sprintf("%s metrics %d seriesPerMetric %d labels %d batchSize %d", benchmark.name, benchmark.numMetrics, benchmark.numSeriesPerMetric, benchmark.numLabelsPerMetric, benchmark.batchSize)
		b.Run(benchName, func(b *testing.B) {
			seriesGen, err := testsupport.NewSeriesGenerator(benchmark.numMetrics, benchmark.numSeriesPerMetric, benchmark.numLabelsPerMetric)
			require.NoError(b, err)

			ts := seriesGen.GetTimeseriesInBatch(benchmark.batchSize)

			for i := 0; i < b.N; i++ {
				withDB(b, "bench_e2e_new_series_ingest", func(db *pgxpool.Pool, t testing.TB) {
					metricsIngestor, err := ingestor.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), &ingestor.Cfg{
						NumCopiers:              8,
						InvertedLabelsCacheSize: cache.DefaultConfig.InvertedLabelsCacheSize,
					})
					require.NoError(b, err)
					defer metricsIngestor.Close()

					b.ReportAllocs()
					b.ResetTimer()
					for _, t := range ts {
						_, _, _ = metricsIngestor.IngestMetrics(context.Background(), &prompb.WriteRequest{Timeseries: t})
					}
					b.StopTimer()

					numSeries := 0
					require.NoError(b, db.QueryRow(context.Background(), "SELECT count(*) FROM _prom_catalog.series").Scan(&numSeries))
					require.Equal(b, benchmark.numMetrics*benchmark.numSeriesPerMetric, numSeries)
				})
			}
		})
	}
}
