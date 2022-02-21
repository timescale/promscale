// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package metrics

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/util"
)

// Keep these const values in pgmodel/metrics to avoid cyclic import since 'trace' may also need this.
const (
	MetricBatcherChannelCap = 1000
	// FlushSize is the maximum number of insertDataRequests that should be buffered before the
	// insertHandler flushes to the next layer. We don't want too many as this
	// increases the number of lost writes if the connector dies. This number
	// was chosen arbitrarily.
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
			Name:      "flush_series",
			Help:      "Number of series batched by the ingestor.",
			Buckets:   util.HistogramBucketsSaturating(1, 2, FlushSize),
		}, []string{"type", "subsystem"},
	)
	IngestorInsertablesIngested = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "inserted_total",
			Help:      "Total insertables (samples/exemplars/spans) inserted into the database.",
		}, []string{"type", "kind"},
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
			Buckets:   prometheus.ExponentialBuckets(1, 2, 15),
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
			Help:      "Total number of insertables (sample/metadata) received",
		}, []string{"type", "kind"},
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
		IngestorInsertablesIngested,
		IngestorInsertsPerBatch,
		IngestorRowsPerBatch,
		IngestorRowsPerInsert,
		IngestorInsertDuration,
		IngestorActiveWriteRequests,
		IngestorDuration,
		IngestorItems,
		IngestorRequests,
		InsertBatchSize,
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
