package trace

import "github.com/timescale/promscale/pkg/clockcache"

const (
	urlCacheSize       = 10000
	operationCacheSize = 10000
	instLibCacheSize   = 10000
	tagCacheSize       = 100000
)

func newSchemaCache() *clockcache.Cache {
	return clockcache.WithMetrics("schema", "trace", urlCacheSize)
}

func newOperationCache() *clockcache.Cache {
	return clockcache.WithMetrics("operation", "trace", operationCacheSize)
}

func newInstrumentationLibraryCache() *clockcache.Cache {
	return clockcache.WithMetrics("instrumentation_lib", "trace", instLibCacheSize)
}

func newTagCache() *clockcache.Cache {
	return clockcache.WithMetrics("tag", "trace", tagCacheSize)
}
