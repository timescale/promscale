// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgclient

import (
	"math"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/util"
)

var (
	statementCacheLen prometheus.Histogram
	statementCacheCap = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Subsystem: "cache",
			Name:      "statement_per_connection_capacity",
			Help:      "Maximum number of statements in connection pool's statement cache",
		},
	)
	statementCacheEnabled = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace:   util.PromNamespace,
			Subsystem:   "cache",
			Name:        "enabled",
			Help:        "Cache is enabled or not.",
			ConstLabels: map[string]string{"type": "metric", "name": "statement_cache"},
		},
	)
)

func InitClientMetrics(client *Client) {
	statementCacheLen = createStatementCacheLengthHistogramMetric(client)
	prometheus.MustRegister(
		statementCacheEnabled,
		statementCacheCap,
		statementCacheLen,
	)
}

func createStatementCacheLengthHistogramMetric(client *Client) prometheus.Histogram {
	// we know the upper bound of the cache, so we want
	// to make that the last bucket of the histogram
	statementCacheUpperBound := client.metricCache.Cap() * 2
	// we want to increase the buckets by a factor of 2
	histogramBucketFactor := 2.0
	// we want 10 total buckets
	totalBuckets := 10
	// If we take the last bucket of the histogram
	// to be 2 to the power of some x, then
	// 2^maxFactor=statementCacheUpperBound -> log_2(statementCacheUpperBound) = maxFactor
	maxFactor := math.Floor(math.Log2(float64(statementCacheUpperBound)))
	// Each bucket is calculated as 2^x, x being incremented by 1 for each bucket
	// ending with 2^maxFactor. To find the start bucket of the histogram,
	// We need to find minFactor so we can end up with 10 buckets
	minFactor := maxFactor - float64(totalBuckets)
	minFactor = math.Max(minFactor, 1) // in case maxFactor <= 10

	histogramStartBucket := math.Pow(histogramBucketFactor, minFactor)
	return prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace:   util.PromNamespace,
			Subsystem:   "cache",
			Name:        "elements_histogram",
			Help:        "Number of elements in cache in terms of elements count.",
			Buckets:     prometheus.ExponentialBuckets(histogramStartBucket, histogramBucketFactor, totalBuckets),
			ConstLabels: map[string]string{"type": "metric", "name": "statement_cache"},
		},
	)
}
