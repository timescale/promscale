// todo (harkishen): file headers

package cache

import "github.com/timescale/promscale/pkg/clockcache"

type ExemplarLabelsPosCache struct {
	cache *clockcache.Cache
}

// NewExemplarLabelsPosCache creates a cache of map[metric_name]LabelPositions where LabelPositions is
// map[LabelName]LabelPosition. This means that the cache stores positions of each label's value per metric basis,
// which is meant to preserve and reuse _prom_catalog.exemplar_label_position table's 'pos' column.
func NewExemplarLabelsPosCache(config Config) *ExemplarLabelsPosCache {
	return &ExemplarLabelsPosCache{cache: clockcache.WithMax(config.ExemplarCacheSize)}
}

func (pos *ExemplarLabelsPosCache) GetLabelPositions(metricName string) (map[string]int, bool) {
	labelPos, exists := pos.cache.Get(metricName)
	if !exists {
		return nil, false
	}
	return labelPos.(map[string]int), true
}

// HasPositionsFor accepts keys. It checks if it has all the positions for the given keys. If position for any
// key does not exist, it returns false. With false as return, the caller is expected to re-fetch the positions for the metricName
// and update the existing entry for metricName.
func (pos *ExemplarLabelsPosCache) HasPositionsFor(metricName string, keys []string) (map[string]int, bool) {
	labelPos, exists := pos.GetLabelPositions(metricName)
	if !exists {
		return nil, exists
	}
	for i := range keys {
		if _, present := labelPos[keys[i]]; present {

		}
	}
}

func (pos *ExemplarLabelsPosCache) SetorUpdateLabelPositions(metricName string, keyPos map[string]int) {
	pos.cache.Update(metricName, keyPos, 0)
}
