package clockcache

import (
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/util"
)

// WithMetrics attaches the default cache metrics like _enabled, _capacity, _size, _elements, _evictions_total and
// the perf metrics like _query_hits, _queries, _query_latency by function.
// The module must be either 'metric' or 'trace'.
func WithMetrics(cacheName, module string, max uint64) *Cache {
	cache := WithMax(max)
	registerMetrics(cacheName, module, cache)
	return cache
}

type perfMetrics struct {
	isApplied      bool
	hitsTotal      prometheus.Counter
	queriesTotal   prometheus.Counter
	queriesLatency *prometheus.HistogramVec
}

func (pm *perfMetrics) createAndRegister(r prometheus.Registerer, name, module string) {
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
			Buckets:     prometheus.LinearBuckets(1, 500, 10),
		}, []string{"method"},
	)

	r.MustRegister(pm.hitsTotal, pm.queriesTotal, pm.queriesLatency)
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

// Note: the moduleType refers to which module the cache belongs. Valid options: ["metric", "trace"].
func registerMetrics(cacheName, moduleType string, c *Cache) {
	if !(moduleType == "metric" || moduleType == "trace") {
		panic("moduleType can only be either 'metric' or 'trace'")
	}

	r := prometheus.DefaultRegisterer
	if isTest := os.Getenv("IS_TEST"); isTest == "true" {
		// Use new registry for each cache creation in e2e tests to
		// avoid duplicate prometheus.MustRegister() calls.
		r = prometheus.NewRegistry()
	}

	perf := new(perfMetrics)
	perf.createAndRegister(r, cacheName, moduleType)
	c.applyPerfMetric(perf)

	enabled := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Subsystem: "cache",
			Name:      "enabled",
			Help:      "Cache is enabled or not.",
			ConstLabels: map[string]string{ // type => ["trace" or "metric"] and name => name of the cache i.e., metric cache, series cache, schema cache, etc.
				"type": moduleType,
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
			ConstLabels: map[string]string{"type": moduleType, "name": cacheName},
		}, func() float64 {
			return float64(c.Len())
		},
	)
	size := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace:   util.PromNamespace,
			Subsystem:   "cache",
			Name:        "bytes",
			Help:        "Cache size in bytes.",
			ConstLabels: map[string]string{"type": moduleType, "name": cacheName},
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
			ConstLabels: map[string]string{"type": moduleType, "name": cacheName},
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
			ConstLabels: map[string]string{"type": moduleType, "name": cacheName},
		}, func() float64 {
			return float64(c.Evictions())
		},
	)
	maxEvictionTs := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace:   "promscale",
			Subsystem:   "cache",
			Name:        "evicted_element_min_age_seconds",
			Help:        "Minimum age in seconds between element last usage and eviction",
			ConstLabels: map[string]string{"type": moduleType, "name": cacheName},
		}, func() float64 {
			return float64(time.Now().Unix() - int64(c.MaxEvictionTs()))
		},
	)
	r.MustRegister(enabled, count, size, capacity, evictions, maxEvictionTs)
}
