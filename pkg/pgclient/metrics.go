package pgclient

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/util"
)

var (
	cachedMetricNames   prometheus.CounterFunc
	cachedLabels        prometheus.CounterFunc
	metricNamesCacheCap prometheus.GaugeFunc
	labelsCacheCap      prometheus.GaugeFunc
)

func InitClientMetrics(client *Client) {
	// Only initialize once.
	if cachedMetricNames != nil {
		return
	}

	cachedMetricNames = prometheus.NewCounterFunc(prometheus.CounterOpts{
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

	cachedLabels = prometheus.NewCounterFunc(prometheus.CounterOpts{
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

	prometheus.MustRegister(
		cachedMetricNames,
		metricNamesCacheCap,
		cachedLabels,
		labelsCacheCap,
	)
}
