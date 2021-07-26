// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package cache

import (
	"fmt"

	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/model"
)

const (
	DefaultMetricCacheSize = 10000
)

// MetricCache provides a caching mechanism for metric table names.
type MetricCache interface {
	Get(schema, metric string) (model.MetricInfo, error)
	Set(schema, metric string, mInfo model.MetricInfo) error
	// Len returns the number of metrics cached in the system.
	Len() int
	// Cap returns the capacity of the metrics cache.
	Cap() int
	Evictions() uint64
}

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
}

func (k key) len() int {
	return len(k.schema) + len(k.metric)
}

// MetricNameCache stores and retrieves metric table names in a in-memory cache.
type MetricNameCache struct {
	Metrics *clockcache.Cache
}

// Get fetches the table name for specified metric.
func (m *MetricNameCache) Get(schema, metric string) (model.MetricInfo, error) {
	var (
		mInfo = model.MetricInfo{}
		key   = key{schema, metric}
	)
	result, ok := m.Metrics.Get(key)
	if !ok {
		return mInfo, errors.ErrEntryNotFound
	}

	mInfo, ok = result.(model.MetricInfo)
	if !ok {
		return mInfo, fmt.Errorf("invalid cache value stored")
	}

	return mInfo, nil
}

// Set stores metric info for specified metric with schema.
func (m *MetricNameCache) Set(schema, metric string, val model.MetricInfo) error {
	k := key{schema, metric}
	//size includes an 8-byte overhead for each string
	m.Metrics.Insert(k, val, uint64(k.len()+val.Len()+16))

	// If the schema inserted above was empty, also populate the cache with the real schema.
	if schema == "" {
		k = key{val.TableSchema, metric}
		//size includes an 8-byte overhead for each string
		m.Metrics.Insert(k, val, uint64(k.len()+val.Len()+16))
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

func NewMetricCache(config Config) *MetricNameCache {
	return &MetricNameCache{Metrics: clockcache.WithMax(config.MetricsCacheSize)}
}

func NewLabelsCache(config Config) LabelsCache {
	return clockcache.WithMax(config.LabelsCacheSize)
}
