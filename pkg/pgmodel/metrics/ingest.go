// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package metrics

import (
	"os"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/util"
)

// Keep these const values in pgmodel/metrics to avoid cyclic import since 'trace' may also need this.
const (
	MetricBatcherChannelCap = 1000
	// FlushSize defines the batch size. It is the maximum number of samples/exemplars per insert batch.
	// This translates to the max array size that we pass into `insert_metric_row`
	FlushSize           = 2000
	MaxInsertStmtPerTxn = 10000
)

var (
	// MaxSentTs is the max timestamp sent to the database.
	MaxSentTs      = int64(0)
	BatchingPoints = int64(0)

	BatchingPointsGauge = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace:   util.PromNamespace,
			Subsystem:   "ingest",
			Name:        "batch_points_total",
			Help:        "Amount of points being batched",
			ConstLabels: map[string]string{"type": "metric", "subsystem": "metric_batcher", "kind": "sample"},
		},
		func() float64 {
			return float64(atomic.LoadInt64(&BatchingPoints))
		},
	)

	IngestorBatchTargetPoints = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "batch_target_points",
			Help:      "Number of rows targeted for copier batch",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 17),
		}, []string{"type", "subsystem"},
	)

	IngestorDuplicates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "duplicates_total",
			Help:      "Total number of processed samples/write_requests_to_db/metrics which where duplicates.",
		}, []string{"type", "kind"},
	)
	IngestorDecompressCalls = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "decompress_calls_total",
			Help:      "Total number of calls to decompress_chunks_after.",
		}, []string{"type", "kind"},
	)
	IngestorDecompressEarliest = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "decompress_min_unix_time",
			Help:      "Earliest decompression time in unix.",
		}, []string{"type", "kind", "table"})
	IngestorMaxSentTimestamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "max_sent_timestamp_milliseconds",
			Help:      "Maximum sample timestamp for samples that Promscale sent to the database.",
		}, []string{"type"},
	)
	IngestorChannelCap = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "channel_cap",
			Help:      "Capacity of the ingest channel.",
		}, []string{"type", "subsystem", "kind"},
	)
	IngestorChannelLenBatcher = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace:   util.PromNamespace,
			Subsystem:   "ingest",
			Name:        "channel_len",
			Help:        "Length of the ingestor channel.",
			ConstLabels: map[string]string{"type": "metric", "subsystem": "metric_batcher", "kind": "sample"},
		},
	)
	IngestorFlushSeries = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "flush_series",
			Help:      "Number of series batched by the ingestor.",
			Buckets:   util.HistogramBucketsSaturating(1, 2, FlushSize),
		}, []string{"type", "subsystem"},
	)
	IngestorInsertsPerBatch = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "inserts_per_batch",
			Help:      "Number of inserts in a single transaction.",
			Buckets:   util.HistogramBucketsSaturating(1, 2, MaxInsertStmtPerTxn),
		}, []string{"type", "subsystem"},
	)
	IngestorRowsPerBatch = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "rows_per_batch",
			Help:      "Number of rows inserted in a single transaction.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 17),
		}, []string{"type", "subsystem"},
	)
	IngestorRowsPerInsert = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "rows_per_insert",
			Help:      "Number of rows inserted in a single insert statement.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 13),
		}, []string{"type", "subsystem"},
	)
	IngestorInsertDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "insert_duration_seconds",
			Help:      "Duration of sample/exemplar batch insert calls to the database.",
			Buckets:   append(prometheus.DefBuckets, []float64{60, 120, 300}...),
		}, []string{"type", "subsystem", "kind"},
	)
	IngestorActiveWriteRequests = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "active_write_requests",
			Help:      "Number of active ingestion occurring in Promscale at the moment.",
		}, []string{"type", "kind"},
	)
	IngestorDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "duration_seconds",
			Help:      "Time taken (processing + db insert) for ingestion of sample/exemplar/span.",
			Buckets:   append(prometheus.DefBuckets, []float64{60, 120, 300}...),
		}, []string{"type", "code"},
	)
	IngestorItems = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "items_total",
			Help:      "Total number of items (sample/metadata/span) ingested",
		}, []string{"type", "kind", "subsystem"},
	)
	IngestorItemsReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "items_received_total",
			Help:      "Total items (samples/exemplars/spans) received.",
		}, []string{"type", "kind"},
	)
	IngestorBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "requests_bytes_total",
			Help:      "Total requests bytes ingested for traces or metrics",
		}, []string{"type"},
	)
	IngestorRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "requests_total",
			Help:      "Total number of requests to ingestor.",
		}, []string{"type", "code"},
	)
	InsertBatchSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "insert_batch_size",
			Help:      "Number of records inserted into database",
			Buckets:   []float64{10, 50, 100, 200, 500, 1000, 2000, 4000, 6000, 8000, 10000, 20000},
		},
		[]string{"type", "kind"},
	)
	IngestorBatchFlushTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "batch_flush_total",
			Help:      "Number of batch flushes by reason (size or timeout).",
		}, []string{"type", "reason"},
	)
	IngestorPendingBatches = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "pending_batches",
			Help:      "Number of batches waiting to be written",
		}, []string{"type"},
	)
	IngestorRequestsQueued = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "requests_queued",
			Help:      "Number of active user requests in queue.",
		}, []string{"type", "queue_idx"},
	)
)

func init() {
	IngestorChannelCap.With(prometheus.Labels{"type": "metric", "subsystem": "metric_batcher", "kind": "sample"}).Set(MetricBatcherChannelCap)
	prometheus.MustRegister(
		IngestorDuplicates,
		IngestorDecompressCalls,
		IngestorDecompressEarliest,
		IngestorMaxSentTimestamp,
		IngestorChannelCap,
		IngestorChannelLenBatcher,
		IngestorFlushSeries,
		IngestorInsertsPerBatch,
		IngestorRowsPerBatch,
		IngestorRowsPerInsert,
		IngestorInsertDuration,
		IngestorActiveWriteRequests,
		IngestorDuration,
		IngestorItems,
		IngestorBytes,
		IngestorItemsReceived,
		IngestorRequests,
		InsertBatchSize,
		IngestorBatchFlushTotal,
		IngestorPendingBatches,
		IngestorRequestsQueued,
	)
}

// RegisterCopierChannelLenMetric creates and registers the copier channel len metric with a callback
// that should return the length of the channel.
//
// Note: ingestorChannelLenCopier metric depends on prometheus call to /metrics hence we need to update with
// a callback. This is an odd one out from the other metrics in the ingestor as other metrics
// are async to prometheus calls.
func RegisterCopierChannelLenMetric(updater func() float64) {
	r := prometheus.DefaultRegisterer
	if val := os.Getenv("IS_TEST"); val == "true" {
		r = prometheus.NewRegistry()
	}
	ingestorChannelLenCopier := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace:   util.PromNamespace,
			Subsystem:   "ingest",
			Name:        "channel_len",
			Help:        "Length of the ingestor channel.",
			ConstLabels: map[string]string{"type": "metric", "subsystem": "copier", "kind": "sample"},
		}, updater,
	)
	r.MustRegister(ingestorChannelLenCopier)
}
