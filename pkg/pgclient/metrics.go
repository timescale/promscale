// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgclient

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/util"
)

var (
	cachedMetricNames   prometheus.GaugeFunc
	cachedLabels        prometheus.GaugeFunc
	metricNamesCacheCap prometheus.GaugeFunc
	labelsCacheCap      prometheus.GaugeFunc
	seriesCacheCap      prometheus.GaugeFunc
	seriesCacheLen      prometheus.GaugeFunc
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

	prometheus.MustRegister(
		cachedMetricNames,
		metricNamesCacheCap,
		cachedLabels,
		labelsCacheCap,
		seriesCacheLen,
		seriesCacheCap,
	)
}
