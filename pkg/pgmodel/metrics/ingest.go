// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/util"
)

// Keep these const values in pgmodel/metrics to avoid cyclic import since 'trace' may also need this.
const (
	MetricBatcherChannelCap = 1000
	// FlushSize defines the batch size. It is the maximum number of samples/exemplars per insert batch.
	// This translates to the max array size that we pass into `insert_metric_row`
	FlushSize           = 2000
	MaxInsertStmtPerTxn = 100
)

var (
	// MaxSentTs is the max timestamp sent to the database.
	MaxSentTs          = int64(0)
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
			Name:      "metric_batch_flush_series",
			Help:      "Number of series batched by the ingestor.",
			Buckets:   util.HistogramBucketsSaturating(1, 2, FlushSize),
		}, []string{"type", "subsystem"},
	)
	IngestorFlushInsertables = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "metric_batch_flush_insertables_total",
			Help:      "Number of insertables batched by the ingestor.",
			Buckets:   append(util.HistogramBucketsSaturating(1, 2, FlushSize), 1.2*FlushSize, 2*FlushSize),
		}, []string{"type", "subsystem"},
	)
	IngestorBatchDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "metric_batch_duration_seconds",
			Help:      "Number of seconds that metrics were batched together",
			Buckets:   prometheus.DefBuckets,
		}, []string{"type", "subsystem"},
	)
	IngestorPipelineTime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "pipeline_time_seconds",
			Help:      "Time that it took to reach the subsystem, from beginning of request",
			Buckets:   prometheus.DefBuckets,
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
	IngestorInsertDurationPerRow = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "insert_duration_seconds_per_row",
			Help:      "Duration of sample/exemplar batch insert call per row to the database.",
			Buckets:   append([]float64{0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001}, prometheus.DefBuckets...),
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
			Help:      "Number of batch flushes by reason (size, timeout, requested).",
		}, []string{"type", "reason", "subsystem"},
	)
	IngestorBatchRemainingAfterFlushTotal = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "batch_remaining_after_flush_total",
			Help:      "Number of items remaining after a flush. Mostly only applies if batch flush reason was size",
			Buckets:   []float64{0, 10, 50, 100, 200, 500, 1000, 2000, 4000, 6000, 8000, 10000, 20000},
		}, []string{"type", "subsystem"},
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
	IngestorWaitForBatchSleepSeconds = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "wait_for_batch_sleep_seconds",
			Help:      "Number of seconds sleeping while waiting for batch",
		}, []string{"type", "subsystem"},
	)
	IngestorWaitForBatchSleepTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "wait_for_batch_sleep_total",
			Help:      "Number of times sleeping while waiting for batch",
		}, []string{"type", "subsystem"},
	)
	IngestorWaitForCopierSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "wait_for_copier_seconds",
			Help:      "Number of seconds waiting for copier get batch",
			Buckets:   prometheus.DefBuckets,
		}, []string{"type", "subsystem"},
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
		IngestorInsertDurationPerRow,
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
		IngestorWaitForBatchSleepSeconds,
		IngestorWaitForBatchSleepTotal,
		IngestorBatchDuration,
		IngestorPipelineTime,
		IngestorBatchRemainingAfterFlushTotal,
		IngestorWaitForCopierSeconds,
	)
}
