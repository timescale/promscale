// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package cache

import (
	"fmt"
	"time"

	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/model"
)

const (
	DefaultMetricCacheSize = 10000
	DefaultLabelsCacheSize = 100000
	growCheckDuration      = time.Second * 5 // check whether to grow the series cache this often
	growFactor             = float64(2.0)    // multiply cache size by this factor when growing the cache
)

var evictionMaxAge = time.Minute * 5 // grow cache if we are evicting elements younger than `now - evictionMaxAge`

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
		return mInfo, errors.ErrEntryNotFound
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

type ResizableCache struct {
	*clockcache.Cache
	maxSizeBytes uint64
}

func NewResizableCache(cache *clockcache.Cache, maxBytes uint64, sigClose <-chan struct{}) *ResizableCache {
	rc := &ResizableCache{cache, maxBytes}
	if sigClose != nil {
		go rc.runSizeCheck(sigClose)
	}
	return rc
}

func (rc *ResizableCache) runSizeCheck(sigClose <-chan struct{}) {
	ticker := time.NewTicker(growCheckDuration)
	for {
		select {
		case <-ticker.C:
			if rc.shouldGrow() {
				rc.grow()
			}
		case <-sigClose:
			return
		}
	}
}

// shouldGrow allows cache growth if we are evicting elements that were recently used or inserted
// evictionMaxAge defines the interval
func (rc *ResizableCache) shouldGrow() bool {
	return rc.MaxEvictionTs()+int32(evictionMaxAge.Seconds()) > int32(time.Now().Unix())
}

func (rc *ResizableCache) grow() {
	sizeBytes := rc.SizeBytes()
	oldSize := rc.Cap()
	if float64(sizeBytes)*1.2 >= float64(rc.maxSizeBytes) {
		log.Warn("msg", "Cache is too small and cannot be grown",
			"current_size_bytes", float64(sizeBytes), "max_size_bytes", float64(rc.maxSizeBytes),
			"current_size_elements", oldSize, "check_interval", growCheckDuration,
			"eviction_max_age", evictionMaxAge)
		return
	}

	multiplier := growFactor
	if float64(sizeBytes)*multiplier >= float64(rc.maxSizeBytes) {
		multiplier = float64(rc.maxSizeBytes) / float64(sizeBytes)
	}
	if multiplier < 1.0 {
		return
	}

	newNumElements := int(float64(oldSize) * multiplier)
	log.Info("msg", "Growing the series cache",
		"new_size_elements", newNumElements, "current_size_elements", oldSize,
		"new_size_bytes", float64(sizeBytes)*multiplier, "max_size_bytes", float64(rc.maxSizeBytes),
		"multiplier", multiplier,
		"eviction_max_age", evictionMaxAge)
	rc.ExpandTo(newNumElements)
}
