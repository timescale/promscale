package trace

import (
	"github.com/jackc/pgtype"
	"github.com/timescale/promscale/pkg/clockcache"
)

const (
	urlCacheSize       = 10000
	operationCacheSize = 10000
	instLibCacheSize   = 10000
	tagCacheSize       = 100000
)

func newSchemaCache() *clockcache.Cache[schemaURL, pgtype.Int8] {
	return clockcache.WithMetrics[schemaURL, pgtype.Int8]("schema", "trace", urlCacheSize)
}

func newOperationCache() *clockcache.Cache[operation, pgtype.Int8] {
	return clockcache.WithMetrics[operation, pgtype.Int8]("operation", "trace", operationCacheSize)
}

func newInstrumentationLibraryCache() *clockcache.Cache[instrumentationLibrary, pgtype.Int8] {
	return clockcache.WithMetrics[instrumentationLibrary, pgtype.Int8]("instrumentation_lib", "trace", instLibCacheSize)
}

func newTagCache() *clockcache.Cache[tag, tagIDs] {
	return clockcache.WithMetrics[tag, tagIDs]("tag", "trace", tagCacheSize)
}
