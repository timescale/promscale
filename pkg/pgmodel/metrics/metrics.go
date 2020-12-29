// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/util"
)

var (
	DuplicateSamples = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Name:      "duplicate_samples_total",
			Help:      "Total number of processed samples which where duplicates",
		},
	)
	DuplicateWrites = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Name:      "duplicate_writes_total",
			Help:      "Total number of writes that contained duplicates",
		},
	)
	DuplicateMetrics = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Name:      "duplicate_metrics_total",
			Help:      "Total number of affected metrics due to duplicate samples.",
		},
	)
	DecompressCalls = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Name:      "decompress_calls_total",
			Help:      "Total number of calls to decompress_chunks_after",
		},
	)
	DecompressEarliest = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Name:      "decompress_min_unix_time",
			Help:      "Earliest decdompression time",
		}, []string{"table"})
	NumInsertsPerBatch = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Name:      "inserts_per_batch",
			Help:      "number of INSERTs in a single transaction",
			Buckets:   []float64{1, 2, 3, 4, 5, 10, 20, 30, 40, 50, 75, 100},
		},
	)
	NumRowsPerBatch = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Name:      "rows_per_batch",
			Help:      "number of rows inserted in a single transaction",
			Buckets:   prometheus.LinearBuckets(100, 500, 10),
		},
	)
	DbBatchInsertDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Name:      "db_batch_insert_duration_seconds",
			Help:      "Duration of sample batch insert calls to the DB.",
			Buckets:   prometheus.DefBuckets,
		},
	)
	HAClusterLeaderDetails = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Name:      "ha_cluster_leader_info",
			Help:      "Info on HA clusters and respective leaders.",
		},
		[]string{"cluster", "replica"})
	NumOfHAClusterLeaderChanges = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Name:      "ha_cluster_leader_changes_total",
			Help:      "Total number of times leader changed per cluster.",
		},
		[]string{"cluster"})
)

func init() {
	prometheus.MustRegister(
		DuplicateSamples,
		DuplicateWrites,
		DecompressCalls,
		DecompressEarliest,
		DuplicateMetrics,
		NumInsertsPerBatch,
		NumRowsPerBatch,
		DbBatchInsertDuration,
		HAClusterLeaderDetails,
		NumOfHAClusterLeaderChanges,
	)
}
