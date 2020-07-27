// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgmodel

import (
	"fmt"
	"strings"

	"github.com/timescale/timescale-prometheus/pkg/clockcache"
)

const (
	DefaultMetricCacheSize = 10000
	DefaultSeriesCacheSize = 100000
)

var (
	// ErrEntryNotFound is returned when entry is not found.
	ErrEntryNotFound = fmt.Errorf("entry not found")
)

type seriesCache struct {
	series *clockcache.Cache
}

func (b *seriesCache) GetSeries(lset Labels) (SeriesID, error) {
	result, ok := b.series.Get(lset.String())
	if !ok {
		return 0, ErrEntryNotFound
	}
	return result.(SeriesID), nil
}

func (b *seriesCache) SetSeries(lset Labels, id SeriesID) error {
	b.series.Insert(lset.String(), id)
	return nil
}

// MetricNameCache stores and retrieves metric table names in a in-memory cache.
type MetricNameCache struct {
	Metrics *clockcache.Cache
}

// Get fetches the table name for specified metric.
func (m *MetricNameCache) Get(metric string) (string, error) {
	result, ok := m.Metrics.Get(metric)
	if !ok {
		return "", ErrEntryNotFound
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
