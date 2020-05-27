// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgmodel

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/allegro/bigcache"
	"github.com/timescale/timescale-prometheus/pkg/log"
)

const (
	defaultEvictionDuration = 10 * time.Minute
)

var (
	// ErrEntryNotFound is returned when entry is not found.
	ErrEntryNotFound = fmt.Errorf("entry not found")
)

type bCache struct {
	series *bigcache.BigCache
}

func (b *bCache) GetSeries(lset Labels) (SeriesID, error) {
	result, err := b.series.Get(lset.String())
	if err != nil {
		if err == bigcache.ErrEntryNotFound {
			return 0, ErrEntryNotFound
		}
		return 0, err
	}
	return SeriesID(binary.LittleEndian.Uint64(result)), nil
}

func (b *bCache) SetSeries(lset Labels, id SeriesID) error {
	byteID := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteID, uint64(id))
	return b.series.Set(lset.String(), byteID)
}

// MetricNameCache stores and retrieves metric table names in a in-memory cache.
type MetricNameCache struct {
	Metrics *bigcache.BigCache
}

// Get fetches the table name for specified metric.
func (m *MetricNameCache) Get(metric string) (string, error) {
	result, err := m.Metrics.Get(metric)
	if err != nil {
		if err == bigcache.ErrEntryNotFound {
			return "", ErrEntryNotFound
		}
		return "", err
	}
	return string(result), nil
}

// Set stores table name for specified metric.
func (m *MetricNameCache) Set(metric string, tableName string) error {
	// deep copy the strings so the original memory doesn't need to stick around
	metricBuilder := strings.Builder{}
	metricBuilder.Grow(len(metric))
	metricBuilder.WriteString(metric)
	table := make([]byte, len(tableName))
	copy(table, tableName)
	return m.Metrics.Set(metricBuilder.String(), table)
}

func DefaultCacheConfig() bigcache.Config {
	config := bigcache.DefaultConfig(defaultEvictionDuration)
	config.Logger = &log.CustomCacheLogger{}

	return config
}
