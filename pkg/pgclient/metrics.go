// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgclient

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/util"
	"math"
)

var (
	cachedMetricNames         prometheus.GaugeFunc
	cachedLabels              prometheus.GaugeFunc
	metricNamesCacheCap       prometheus.GaugeFunc
	metricNamesCacheEvictions prometheus.CounterFunc
	labelsCacheCap            prometheus.GaugeFunc
	labelsCacheEvictions      prometheus.CounterFunc
	seriesCacheCap            prometheus.GaugeFunc
	seriesCacheLen            prometheus.GaugeFunc
	seriesCacheEvictions      prometheus.CounterFunc
	statementCacheLen         prometheus.Histogram
	statementCacheCap         = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Name:      "statement_cache_per_connection_capacity",
			Help:      "Maximum number of statements in connection pool's statement cache",
		},
	)
	statementCacheEnabled = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Name:      "statement_cache_enabled",
			Help:      "Is the database connection pool's statement cache enabled",
		},
	)
)

func InitClientMetrics(client *Client) {
	// Only initialize once.
	if cachedMetricNames != nil {
		return
	}

	cachedMetricNames = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: util.PromNamespace,
		Name:      "metric_name_cache_elements_stored",
		Help:      "Total number of metric names in the metric name cache.",
	}, func() float64 {
		return float64(client.NumCachedMetricNames())
	})

	metricNamesCacheCap = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: util.PromNamespace,
		Name:      "metric_name_cache_capacity",
		Help:      "Maximum number of elements in the metric names cache.",
	}, func() float64 {
		return float64(client.MetricNamesCacheCapacity())
	})

	metricNamesCacheEvictions = prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: util.PromNamespace,
		Name:      "metric_name_cache_evictions_total",
		Help:      "Evictions in the metric names cache.",
	}, func() float64 {
		return float64(client.metricCache.Evictions())
	})

	cachedLabels = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: util.PromNamespace,
		Name:      "label_cache_elements_stored",
		Help:      "Total number of label-id to label mappings cache.",
	}, func() float64 {
		return float64(client.NumCachedLabels())
	})

	labelsCacheCap = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: util.PromNamespace,
		Name:      "label_cache_capacity",
		Help:      "Total number of label-id to label mappings cache.",
	}, func() float64 {
		return float64(client.LabelsCacheCapacity())
	})

	labelsCacheEvictions = prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: util.PromNamespace,
		Name:      "label_cache_evictions_total",
		Help:      "Total number of evictions in the label-id to label mappings cache.",
	}, func() float64 {
		return float64(client.labelsCache.Evictions())
	})

	seriesCacheLen = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: util.PromNamespace,
		Name:      "series_cache_elements_stored",
		Help:      "Total number of series stored in cache",
	}, func() float64 {
		return float64(client.seriesCache.Len())
	})

	seriesCacheCap = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: util.PromNamespace,
		Name:      "series_cache_capacity",
		Help:      "Total size of series cache.",
	}, func() float64 {
		return float64(client.seriesCache.Cap())
	})

	seriesCacheEvictions = prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: util.PromNamespace,
		Name:      "series_cache_evictions_total",
		Help:      "Total number of series cache evictions.",
	}, func() float64 {
		return float64(client.seriesCache.Evictions())
	})

	statementCacheLen = createStatementCacheLengthHistogramMetric(client)
	prometheus.MustRegister(
		cachedMetricNames,
		metricNamesCacheCap,
		cachedLabels,
		labelsCacheCap,
		seriesCacheLen,
		seriesCacheCap,
		seriesCacheEvictions,
		metricNamesCacheEvictions,
		labelsCacheEvictions,
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
			Namespace: util.PromNamespace,
			Name:      "statement_cache_elements_stored",
			Help:      "Number of statements in connection pool's statement cache",
			Buckets:   prometheus.ExponentialBuckets(histogramStartBucket, histogramBucketFactor, totalBuckets),
		},
	)
}
