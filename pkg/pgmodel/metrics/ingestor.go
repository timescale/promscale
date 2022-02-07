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
	IngestorMaxSentTimestamp = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "max_sent_timestamp_milliseconds",
			Help:      "Maximum sample timestamp for samples that Promscale sent to the database.",
		},
	)
	IngestorChannelCap = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "channel_cap",
			Help:      "Capacity of the ingest channel.",
		}, []string{"type", "subsystem", "kind"},
	)
	IngestorChannelLen = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "channel_len",
			Help:      "Length of the ingestor channel.",
			Buckets:   util.HistogramBucketsSaturating(0, 2, MetricBatcherChannelCap),
		}, []string{"type", "subsystem", "kind"},
	)
	SampleCopierChannelLengthFunc func() float64
	IngestorChannelLenCopier      = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace:   util.PromNamespace,
			Subsystem:   "ingest",
			Name:        "channel_len",
			Help:        "Length of the ingestor channel.",
			ConstLabels: map[string]string{"type": "metric", "subsystem": "copier", "kind": "sample"},
		}, SampleCopierChannelLengthFunc,
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
			Help:      "Total insertables (samples/exemplars) inserted into the database.",
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

	// Used in pkg/api
	IngestorInsertables = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "insertables_total",
			Help:      "Total number of insertables (sample/metadata) ingested.",
		}, []string{"type", "kind"},
	)
	IngestorRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "requests_total",
			Help:      "Total number of requests to ingestor.",
		}, []string{"type", "kind", "code"},
	)
	IngestorDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "duration_seconds",
			Help:      "Time taken (processing + db insert) for ingestion of sample/exemplar.",
			Buckets:   append(prometheus.DefBuckets, []float64{60, 120, 300}...),
		}, []string{"type", "kind"},
	)
)

func init() {
	IngestorChannelCap.With(prometheus.Labels{"type": "metric", "subsystem": "metric_batcher", "kind": "sample"}).Set(MetricBatcherChannelCap)
	prometheus.MustRegister(
		IngestorDuplicates,
		IngestorDecompressCalls,
		IngestorDecompressEarliest,
		HAClusterLeaderDetails,
		NumOfHAClusterLeaderChanges,
		IngestorMaxSentTimestamp,
		IngestorChannelCap,
		IngestorChannelLen,
		IngestorFlushSeries,
		IngestorInsertablesIngested,
		IngestorInsertsPerBatch,
		IngestorRowsPerBatch,
		IngestorRowsPerInsert,
		IngestorInsertDuration,
		IngestorActiveWriteRequests,
		IngestorInsertables,
		IngestorDuration,
		IngestorRequests,
	)
}
