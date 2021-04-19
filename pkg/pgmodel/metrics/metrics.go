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
	MaxSentTimestamp = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Name:      "max_sent_timestamp_milliseconds",
			Help:      "Maximum sample timestamp that Promscale sent to the database.",
		},
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
		MaxSentTimestamp,
	)
}
