package cache

import (
	"github.com/timescale/promscale/pkg/clockcache"
)

const DefaultInvertedLabelsCacheSize = 500000

type LabelInfo struct {
	LabelID int32 // id of label
	Pos     int32 // position of specific label within a specific metric.
}

type LabelKey struct {
	MetricName, Name, Value string
}

func NewLabelKey(metricName, name, value string) LabelKey {
	return LabelKey{MetricName: metricName, Name: name, Value: value}
}

func (lk LabelKey) len() int {
	return len(lk.MetricName) + len(lk.Name) + len(lk.Value)
}

func NewLabelInfo(lableID, pos int32) LabelInfo {
	return LabelInfo{LabelID: lableID, Pos: pos}
}

func (li LabelInfo) len() int {
	return 8
}

// (metric, label key-pair) -> (label id,label position) cache
// Used when creating series to avoid DB calls for labels
// Each label position is unique for a specific metric, meaning that
// one label can have different position for different metrics
type InvertedLabelsCache struct {
	*ResizableCache
}

// Cache is thread-safe
func NewInvertedLabelsCache(config Config, sigClose chan struct{}) *InvertedLabelsCache {
	cache := clockcache.WithMetrics("inverted_labels", "metric", config.InvertedLabelsCacheSize)
	return &InvertedLabelsCache{NewResizableCache(cache, config.InvertedLabelsCacheMaxBytes, sigClose)}
}

func (c *InvertedLabelsCache) GetLabelsId(key LabelKey) (LabelInfo, bool) {
	id, found := c.Get(key)
	if found {
		return id.(LabelInfo), found
	}
	return LabelInfo{}, false
}

func (c *InvertedLabelsCache) Put(key LabelKey, val LabelInfo) bool {
	_, added := c.Insert(key, val, uint64(key.len())+uint64(val.len())+17)
	return added
}
