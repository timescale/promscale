package pgmodel

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/util"
)

var (
	duplicateSamples = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Name:      "duplicate_samples_total",
			Help:      "Total number of processed samples which where duplicates",
		},
	)
	duplicateWrites = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Name:      "duplicate_writes_total",
			Help:      "Total number of writes that contained duplicates",
		},
	)
	decompressCalls = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Name:      "decompress_calls_total",
			Help:      "Total number of calls to decompress_chunks_after",
		},
	)
	decompressEarliest = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Name:      "decompress_min_unix_time",
			Help:      "Earliest decdompression time",
		}, []string{"table"})
	numInsertsPerBatch = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Name:      "inserts_per_batch",
			Help:      "number of INSERTs in a single transaction",
			Buckets:   []float64{1, 2, 3, 4, 5, 10, 20, 30, 40, 50, 75, 100},
		},
	)
)

func init() {
	prometheus.MustRegister(
		duplicateSamples,
		duplicateWrites,
		decompressCalls,
		decompressEarliest,
		numInsertsPerBatch,
	)
}
