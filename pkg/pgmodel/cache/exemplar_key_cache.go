// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package cache

import (
	"unsafe"

	"github.com/timescale/promscale/pkg/clockcache"
)

// Make the cache size the same as the metric size, assuming every metric has an exemplar
const DefaultExemplarKeyPosCacheSize = DefaultMetricCacheSize

type PositionCache interface {
	// GetLabelPositions fetches the position of label keys (as index) that must be respected
	// while pushing exemplar label's values to the database.
	GetLabelPositions(metric string) (map[string]int, bool)
	// SetOrUpdateLabelPositions sets or updates the position of label (index) keys for the given metric.
	SetOrUpdateLabelPositions(metric string, index map[string]int)
}

type ExemplarLabelsPosCache struct {
	cache *clockcache.Cache[string, map[string]int]
}

// NewExemplarLabelsPosCache creates a cache of map[metric_name]LabelPositions where LabelPositions is
// map[LabelName]LabelPosition. This means that the cache stores positions of each label's value per metric basis,
// which is meant to preserve and reuse _prom_catalog.exemplar_label_position table's 'pos' column.
func NewExemplarLabelsPosCache(config Config) PositionCache {
	return &ExemplarLabelsPosCache{cache: clockcache.WithMetrics[string, map[string]int]("exemplar_labels", "metric", config.ExemplarKeyPosCacheSize)}
}

func (pos *ExemplarLabelsPosCache) GetLabelPositions(metric string) (map[string]int, bool) {
	labelPos, exists := pos.cache.Get(metric)
	if !exists {
		return nil, false
	}
	return labelPos, true
}

func (pos *ExemplarLabelsPosCache) SetOrUpdateLabelPositions(metric string, index map[string]int) {
	/* Sizeof only measures map header; not what's inside. Assume 100-length metric names in worst case */
	size := uint64(unsafe.Sizeof(index)) + uint64(len(index)*(100+4)) // #nosec
	pos.cache.Update(metric, index, size)
}
