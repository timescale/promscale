// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/util"
)

var (
	// MaxSentTimestamp is the max timestamp sent to the database.
	MaxSentTimestamp   = int64(0)
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
			Buckets:   util.HistogramBucketsSaturating(1, 2, maxInsertStmtPerTxn),
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
			Buckets:   append(prometheus.DefBuckets, []float64{60, 120, 300}...),
		},
	)

	MetadataBatchInsertDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Name:      "metadata_insert_duration_seconds",
			Help:      "Duration of a single metadata batch to insert into the DB.",
			Buckets:   append(prometheus.DefBuckets, []float64{60, 120, 300}...),
		})

	SamplesCopierChCap = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Name:      "samples_copier_channel_cap",
			Help:      "Capacity of samples copier channel",
		},
	)

	copierChannelMutex sync.Mutex

	SamplesCopierChannelToMonitor chan readRequest
	SamplesCopierChLen            = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Name:      "samples_copier_channel_len",
			Help:      "Length of samples copier channel",
		},
		func() float64 { return float64(len(SamplesCopierChannelToMonitor)) },
	)
	activeWriteRequests = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Name:      "write_requests_processing",
			Help:      "Number of active ingestion occurring in Promscale at the moment.",
		},
	)
)

func setCopierChannelToMonitor(toSamplesCopiers chan readRequest) {
	copierChannelMutex.Lock()
	defer copierChannelMutex.Unlock()

	SamplesCopierChCap.Set(float64(cap(toSamplesCopiers)))
	SamplesCopierChannelToMonitor = toSamplesCopiers
}

func init() {
	prometheus.MustRegister(
		MetricBatcherChCap,
		MetricBatcherChLen,
		MetricBatcherFlushSeries,
		NumInsertsPerBatch,
		NumRowsPerBatch,
		NumRowsPerInsert,
		DbBatchInsertDuration,
		MetadataBatchInsertDuration,
		SamplesCopierChCap,
		SamplesCopierChLen,
		activeWriteRequests,
	)

	MetricBatcherChCap.Set(MetricBatcherChannelCap)
}
