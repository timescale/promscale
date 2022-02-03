package trace

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/util"
)

const (
	urlCacheSize       = 10000
	operationCacheSize = 10000
	instLibCacheSize   = 10000
	tagCacheSize       = 10000
)

// Only for tests: Metrics get registered twice in E2E tests, leading to panic.
// Hence, we must register only once.
var regSchema, regOp, regInst, regTag sync.Once

func newSchemaCache() *clockcache.Cache {
	c := clockcache.WithMax(urlCacheSize)
	regSchema.Do(func() {
		registerMetrics("schema", c)
	})
	return c
}

func newOperationCache() *clockcache.Cache {
	c := clockcache.WithMax(operationCacheSize)
	regOp.Do(func() {
		registerMetrics("operation", c)
	})
	return c
}

func newInstrumentationLibraryCache() *clockcache.Cache {
	c := clockcache.WithMax(instLibCacheSize)
	regInst.Do(func() {
		registerMetrics("instrumentation_lib", c)
	})
	return c
}

func newTagCache() *clockcache.Cache {
	c := clockcache.WithMax(tagCacheSize)
	regTag.Do(func() {
		registerMetrics("tag", c)
	})
	return c
}

func registerMetrics(cacheName string, c *clockcache.Cache) {
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
			Namespace: util.PromNamespace,
			Subsystem: "cache",
			Name:      "elements",
			Help:      "Number of elements in cache in terms of elements count.",
			ConstLabels: map[string]string{
				"type": "trace",
				"name": cacheName,
			},
		}, func() float64 {
			return float64(c.Len())
		},
	)
	size := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Subsystem: "cache",
			Name:      "bytes",
			Help:      "Total cache capacity in bytes.",
			ConstLabels: map[string]string{
				"type": "trace",
				"name": cacheName,
			},
		}, func() float64 {
			return float64(c.SizeBytes())
		},
	)
	capacity := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Subsystem: "cache",
			Name:      "capacity_elements",
			Help:      "Total cache capacity in terms of elements count.",
			ConstLabels: map[string]string{
				"type": "trace",
				"name": cacheName,
			},
		}, func() float64 {
			return float64(c.Cap())
		},
	)
	evictions := prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Subsystem: "cache",
			Name:      "evictions_total",
			Help:      "Total evictions in a clockcache.",
			ConstLabels: map[string]string{
				"type": "trace",
				"name": cacheName,
			},
		}, func() float64 {
			return float64(c.Evictions())
		},
	)
	prometheus.MustRegister(enabled, count, size, capacity, evictions)
}
