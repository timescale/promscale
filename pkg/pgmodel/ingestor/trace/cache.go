package trace

import (
	"github.com/timescale/promscale/pkg/clockcache"
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
