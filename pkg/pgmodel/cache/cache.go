// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package cache

import (
	"fmt"

	"github.com/timescale/promscale/pkg/clockcache"
	promscale_errors "github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/model"
)

const (
	DefaultMetricCacheSize = 10000
)

type LabelsCache interface {
	// GetValues tries to get a batch of keys and store the corresponding values is valuesOut
	// returns the number of keys that were actually found.
	// NOTE: this function does _not_ preserve the order of keys; the first numFound
	//       keys will be the keys whose values are present, while the remainder
	//       will be the keys not present in the cache
	GetValues(keys []interface{}, valuesOut []interface{}) (numFound int)
	// InsertBatch inserts a batch of keys with their corresponding values.
	// This function will _overwrite_ the keys and values slices with their
	// canonical versions.
	// returns the number of elements inserted, is lower than len(keys) if insertion
	// starved
	InsertBatch(keys []interface{}, values []interface{}, sizes []uint64) (numInserted int)
	// Len returns the number of labels cached in the system.
	Len() int
	// Cap returns the capacity of the labels cache.
	Cap() int
	Evictions() uint64
}

type key struct {
	schema, metric string
	isExemplar     bool
}

func (k key) len() int {
	return len(k.schema) + len(k.metric)
}

// MetricCache provides a caching mechanism for metric table names.
type MetricCache interface {
	Get(schema, metric string, isExemplar bool) (model.MetricInfo, error)
	Set(schema, metric string, mInfo model.MetricInfo, isExemplar bool) error
	// Len returns the number of metrics cached in the system.
	Len() int
	// Cap returns the capacity of the metrics cache.
	Cap() int
	Evictions() uint64
}

// MetricNameCache stores and retrieves metric table names in an in-memory cache.
type MetricNameCache struct {
	Metrics *clockcache.Cache
}

func NewMetricCache(config Config) *MetricNameCache {
	return &MetricNameCache{Metrics: clockcache.WithMetrics("metric_name", "metric", config.MetricsCacheSize)}
}

// Get fetches the table name for specified metric.
func (m *MetricNameCache) Get(schema, metric string, isExemplar bool) (model.MetricInfo, error) {
	var (
		mInfo = model.MetricInfo{}
		key   = key{schema, metric, isExemplar}
	)
	result, ok := m.Metrics.Get(key)
	if !ok {
		return mInfo, promscale_errors.ErrEntryNotFound
	}

	mInfo, ok = result.(model.MetricInfo)
	if !ok {
		return mInfo, fmt.Errorf("invalid cache value stored")
	}

	return mInfo, nil
}

// Set stores metric info for specified metric with schema.
func (m *MetricNameCache) Set(schema, metric string, val model.MetricInfo, isExemplar bool) error {
	k := key{schema, metric, isExemplar}
	//size includes an 8-byte overhead for each string
	m.Metrics.Insert(k, val, uint64(k.len()+val.Len()+17))

	// If the schema inserted above was empty, also populate the cache with the real schema.
	if schema == "" {
		k = key{val.TableSchema, metric, isExemplar}
		//size includes an 8-byte overhead for each string
		m.Metrics.Insert(k, val, uint64(k.len()+val.Len()+17))
	}

	return nil
}

func (m *MetricNameCache) Len() int {
	return m.Metrics.Len()
}

func (m *MetricNameCache) Cap() int {
	return m.Metrics.Cap()
}

func (m *MetricNameCache) Evictions() uint64 {
	return m.Metrics.Evictions()
}

func NewLabelsCache(config Config) LabelsCache {
	return clockcache.WithMetrics("label", "metric", config.LabelsCacheSize)
}
