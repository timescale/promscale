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
	StaleMaxSentTimestamp = prometheus.NewGauge( // This will be deprecated in the renaming PR.
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Name:      "max_sent_timestamp_milliseconds",
			Help:      "Maximum sample timestamp that Promscale sent to the database.",
		},
	)
	IngestedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "ingested_total",
			Help:      "Total number of insertables ingested in the database.",
		},
		[]string{"type"},
	)
	ActiveWriteRequests = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "active_write_requests",
			Help:      "Number of write requests that are active in the ingestion pipeline.",
		},
		[]string{"subsystem"},
	)
	InsertDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "insert_duration_seconds",
			Help:      "Time taken to insert a batch of samples or traces into the database.",
			Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 50, 100, 250, 500, 1000, 2500},
		},
		[]string{"subsystem"},
	)
	IngestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "duration_seconds",
			Help:      "Time taken to process (including filling up caches) and insert a batch of samples or traces into the database.",
			Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 50, 100, 250, 500, 1000, 2500},
		},
		[]string{"subsystem"},
	)
	MaxSentTimestamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ingest",
			Name:      "max_sent_timestamp_milliseconds",
			Help:      "Maximum sent timestamp into the database. For samples, it is the sample timestamp and for traces, it is the maximum end timestamp.",
		},
		[]string{"subsystem"},
	)
	RequestsDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Subsystem: "query",
			Name:      "duration_seconds",
			Help:      "Time taken by function to respond to query.",
			Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 50, 100, 250, 500, 1000, 2500},
		},
		[]string{"subsystem", "handler"},
	)
	RequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Subsystem: "query",
			Name:      "requests_total",
			Help:      "Total query requests.",
		},
		[]string{"subsystem", "handler", "code"},
	)
)

func init() {
	prometheus.MustRegister(
		DuplicateSamples,
		DuplicateWrites,
		DecompressCalls,
		DecompressEarliest,
		DuplicateMetrics,
		HAClusterLeaderDetails,
		NumOfHAClusterLeaderChanges,
		StaleMaxSentTimestamp,
		IngestedTotal,
		ActiveWriteRequests,
		InsertDuration,
		IngestDuration,
		MaxSentTimestamp,
		RequestsDuration,
		RequestsTotal,
	)
}
