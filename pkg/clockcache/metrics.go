package clockcache

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/util"
)

type MetricOptions struct {
	Name, Module string
}

// WithMetrics attaches the default cache metrics like _enabled, _capacity, _size, _elements, _evictions_total and
// the perf metrics like _query_hits, _queries, _query_latency by function.
// The module must be either 'metric' or 'trace'.
func WithMetrics(cacheName, module string, max uint64) *Cache {
	cache := WithMax(max)
	RegisterBasicMetrics(cacheName, module, cache)

	perf := new(perfMetrics)
	perf.createAndRegister(cacheName, module)
	cache.applyPerfMetric(perf)
	return cache
}

type perfMetrics struct {
	isApplied      bool
	hitsTotal      prometheus.Counter
	queriesTotal   prometheus.Counter
	queriesLatency *prometheus.HistogramVec
}

func (pm *perfMetrics) createAndRegister(name, module string) {
	pm.isApplied = true
	pm.hitsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace:   "promscale",
			Subsystem:   "cache",
			Name:        "query_hits_total",
			Help:        "Total query hits in clockcache.",
			ConstLabels: map[string]string{"type": module, "name": name},
		},
	)
	pm.queriesTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace:   "promscale",
			Subsystem:   "cache",
			Name:        "queries_total",
			Help:        "Total query requests to the clockcache.",
			ConstLabels: map[string]string{"type": module, "name": name},
		},
	)
	pm.queriesLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   "promscale",
			Subsystem:   "cache",
			Name:        "query_latency_microseconds",
			Help:        "Query latency for the clockcache.",
			ConstLabels: map[string]string{"type": module, "name": name},
			Buckets:     prometheus.LinearBuckets(1, 500, 20),
		}, []string{"method"},
	)
	prometheus.MustRegister(pm.hitsTotal, pm.queriesTotal, pm.queriesLatency)
}

func (pm *perfMetrics) Inc(c prometheus.Counter) {
	if pm.isApplied {
		c.Inc()
	}
}

func (pm *perfMetrics) Observe(method string, d time.Duration) {
	if pm.isApplied {
		pm.queriesLatency.WithLabelValues(method).Observe(float64(d.Microseconds()))
	}
}

// RegisterBasicMetrics registers and creates basic metrics for cache like:
// 1. promscale_cache_enabled
// 2. promscale_cache_elements
// 3. promscale_cache_size
// 4. promscale_cache_capacity
// 5. promscale_cache_evictions_total
// Note: the moduleType refers to which module the cache belongs. Valid options: ["metric", "trace"].
func RegisterBasicMetrics(cacheName, moduleType string, c *Cache) {
	if !(moduleType == "metric" || moduleType == "trace") {
		panic("moduleType can only be either 'metric' or 'trace'")
	}
	enabled := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Subsystem: "cache",
			Name:      "enabled",
			Help:      "Cache is enabled or not.",
			ConstLabels: map[string]string{ // type => ["trace" or "metric"] and name => name of the cache i.e., metric cache, series cache, schema cache, etc.
				"type": "trace",
				"name": cacheName,
			},
		},
	)
	enabled.Set(1)
	count := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace:   util.PromNamespace,
			Subsystem:   "cache",
			Name:        "elements",
			Help:        "Number of elements in cache in terms of elements count.",
			ConstLabels: map[string]string{"type": "trace", "name": cacheName},
		}, func() float64 {
			return float64(c.Len())
		},
	)
	size := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace:   util.PromNamespace,
			Subsystem:   "cache",
			Name:        "capacity_bytes",
			Help:        "Cache size in bytes.",
			ConstLabels: map[string]string{"type": "trace", "name": cacheName},
		}, func() float64 {
			return float64(c.SizeBytes())
		},
	)
	capacity := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace:   util.PromNamespace,
			Subsystem:   "cache",
			Name:        "capacity_elements",
			Help:        "Total cache capacity in terms of elements count.",
			ConstLabels: map[string]string{"type": "trace", "name": cacheName},
		}, func() float64 {
			return float64(c.Cap())
		},
	)
	evictions := prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Namespace:   util.PromNamespace,
			Subsystem:   "cache",
			Name:        "evictions_total",
			Help:        "Total evictions in a clockcache.",
			ConstLabels: map[string]string{"type": "trace", "name": cacheName},
		}, func() float64 {
			return float64(c.Evictions())
		},
	)
	prometheus.MustRegister(enabled, count, size, capacity, evictions)
}
