// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/util"
)

var (
	MetricBatcherChCap = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Name:      "metric_batcher_channel_cap",
			Help:      "Capacity of metric batcher channel",
		},
	)

	MetricBatcherChLen = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Name:      "metric_batcher_channel_len",
			Help:      "Length of metric batcher channels",
			Buckets:   util.HistogramBucketsSaturating(0, 2, MetricBatcherChannelCap),
		},
	)

	MetricBatcherFlushSeries = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Name:      "metric_batcher_flush_series",
			Help:      "Number of series batched by the batcher",
			Buckets:   util.HistogramBucketsSaturating(1, 2, flushSize),
		},
	)

	NumInsertsPerBatch = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Name:      "copier_inserts_per_batch",
			Help:      "number of INSERTs in a single transaction",
			Buckets:   util.HistogramBucketsSaturating(1, 2, maxCopyRequestsPerTxn),
		},
	)
	NumRowsPerBatch = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Name:      "copier_rows_per_batch",
			Help:      "number of rows inserted in a single transaction",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 15),
		},
	)
	NumRowsPerInsert = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Name:      "copier_rows_per_insert",
			Help:      "number of rows inserted in a single insert statement",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 13),
		},
	)
	DbBatchInsertDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Name:      "copier_insert_duration_seconds",
			Help:      "Duration of sample batch insert calls to the DB.",
			Buckets:   prometheus.DefBuckets,
		},
	)
)

func init() {
	prometheus.MustRegister(
		MetricBatcherChCap,
		MetricBatcherChLen,
		MetricBatcherFlushSeries,
		NumInsertsPerBatch,
		NumRowsPerBatch,
		NumRowsPerInsert,
		DbBatchInsertDuration,
	)

	MetricBatcherChCap.Set(MetricBatcherChannelCap)
}
