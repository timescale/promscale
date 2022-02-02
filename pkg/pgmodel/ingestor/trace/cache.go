package trace

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/clockcache"
	pgmodelCache "github.com/timescale/promscale/pkg/pgmodel/cache"
)

const (
	urlCacheSize       = 10000
	operationCacheSize = 10000
	instLibCacheSize   = 10000
	tagCacheSize       = 10000
)

func newSchemaCache() *clockcache.Cache {
	return clockcache.WithMax(urlCacheSize)
}

func newOperationCache() *clockcache.Cache {
	return clockcache.WithMax(operationCacheSize)
}

func newInstrumentationLibraryCache() *clockcache.Cache {
	return clockcache.WithMax(instLibCacheSize)
}

func newTagCache() *clockcache.Cache {
	return clockcache.WithMax(tagCacheSize)
}

func registerToMetrics(cacheKind string, c *clockcache.Cache) {
	pgmodelCache.Enabled.With(prometheus.Labels{"subsystem": "trace", "name": cacheKind})
	pgmodelCache.RegisterUpdateFunc(pgmodelCache.Cap, func(collector prometheus.Collector) {
		metric := collector.(*prometheus.GaugeVec)
		metric.With(prometheus.Labels{"subsystem": "trace", "name": cacheKind}).Set(float64(c.Cap()))
	})
	pgmodelCache.RegisterUpdateFunc(pgmodelCache.Size, func(collector prometheus.Collector) {
		metric := collector.(*prometheus.GaugeVec)
		metric.With(prometheus.Labels{"subsystem": "trace", "name": cacheKind}).Set(float64(c.SizeBytes()))
	})
	pgmodelCache.RegisterUpdateFunc(pgmodelCache.Evict, func(collector prometheus.Collector) {
		metric := collector.(*prometheus.CounterVec)
		metric.With(prometheus.Labels{"subsystem": "trace", "name": cacheKind}).Add(float64(c.Evictions()))
	})
}
