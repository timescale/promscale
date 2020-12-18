// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package cache

import (
	"strings"

	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
)

const (
	DefaultMetricCacheSize = 10000
)

// MetricCache provides a caching mechanism for metric table names.
type MetricCache interface {
	Get(metric string) (string, error)
	Set(metric string, tableName string) error
	// Len returns the number of metrics cached in the system.
	Len() int
	// Cap returns the capacity of the metrics cache.
	Cap() int
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
	InsertBatch(keys []interface{}, values []interface{}) (numInserted int)
	// Len returns the number of labels cached in the system.
	Len() int
	// Cap returns the capacity of the labels cache.
	Cap() int
}

// MetricNameCache stores and retrieves metric table names in a in-memory cache.
type MetricNameCache struct {
	Metrics *clockcache.Cache
}

// Get fetches the table name for specified metric.
func (m *MetricNameCache) Get(metric string) (string, error) {
	result, ok := m.Metrics.Get(metric)
	if !ok {
		return "", errors.ErrEntryNotFound
	}
	return result.(string), nil
}

// Set stores table name for specified metric.
func (m *MetricNameCache) Set(metric string, tableName string) error {
	// deep copy the strings so the original memory doesn't need to stick around
	metricBuilder := strings.Builder{}
	metricBuilder.Grow(len(metric))
	metricBuilder.WriteString(metric)
	tableBuilder := strings.Builder{}
	tableBuilder.Grow(len(tableName))
	tableBuilder.WriteString(tableName)
	m.Metrics.Insert(metricBuilder.String(), tableBuilder.String())
	return nil
}

func (m *MetricNameCache) Len() int {
	return m.Metrics.Len()
}

func (m *MetricNameCache) Cap() int {
	return m.Metrics.Cap()
}
