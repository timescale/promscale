// todo (harkishen): file headers

package cache

import "github.com/timescale/promscale/pkg/clockcache"

type PositionCache interface {
	// GetLabelPositions fetches the position of label keys (as index) that must be respected
	// while pushing exemplar label's values to the database.
	GetLabelPositions(metric string) (map[string]int, bool)
	// SetorUpdateLabelPositions sets or updates the position of label (index) keys for the given metric.
	SetorUpdateLabelPositions(metric string, index map[string]int)
}

type ExemplarLabelsPosCache struct {
	cache *clockcache.Cache
}

// NewExemplarLabelsPosCache creates a cache of map[metric_name]LabelPositions where LabelPositions is
// map[LabelName]LabelPosition. This means that the cache stores positions of each label's value per metric basis,
// which is meant to preserve and reuse _prom_catalog.exemplar_label_position table's 'pos' column.
func NewExemplarLabelsPosCache(config Config) PositionCache {
	return &ExemplarLabelsPosCache{cache: clockcache.WithMax(config.ExemplarCacheSize)}
}

func (pos *ExemplarLabelsPosCache) GetLabelPositions(metric string) (map[string]int, bool) {
	labelPos, exists := pos.cache.Get(metric)
	if !exists {
		return nil, false
	}
	return labelPos.(map[string]int), true
}

func (pos *ExemplarLabelsPosCache) SetorUpdateLabelPositions(metric string, index map[string]int) {
	pos.cache.Update(metric, index, 0)
}
