// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package cache

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
	"math"
	"sort"
	"sync"
	"time"
)

//this seems like a good initial size for /active/ series. Takes about 32MB
const DefaultSeriesCacheSize = 250000

const GrowCheckDuration = time.Minute // check whether to grow the series cache this often
const GrowEvictionThreshold = 0.2     // grow when evictions more than 20% of cache size
const GrowFactor = float64(2.0)       // multiply cache size by this factor when growing the cache

func GenerateKey(labels []prompb.Label) (key *model.SeriesCacheKey, metricName string, error error) {
	builder := keyPool.Get().(*bytes.Buffer)
	builder.Reset()
	defer keyPool.Put(builder)
	metricName, err := generateKey(labels, builder)
	if err != nil {
		return nil, "", err
	}

	k := builder.String()

	return model.NewSeriesCacheKey(k), metricName, nil
}

// generateKey gets a string representation for hashing and comparison
// This representation is guaranteed to uniquely represent the underlying label
// set, though need not human-readable, or indeed, valid utf-8
func generateKey(labels []prompb.Label, builder *bytes.Buffer) (metricName string, error error) {
	if len(labels) == 0 {
		return "", nil
	}

	comparator := func(i, j int) bool {
		return labels[i].Name < labels[j].Name
	}

	//this is equivalent to sort.SliceIsSorted but avoids the interface conversion
	//and resulting allocation.
	//We really shouldn't find unsorted labels. This code is to be extra safe.
	for i := len(labels) - 1; i > 0; i-- {
		if labels[i].Name < labels[i-1].Name {
			sort.Slice(labels, comparator)
			break
		}
	}

	expectedStrLen := len(labels) * 4 // 2 for the length of each key, and 2 for the length of each value
	for i := range labels {
		l := labels[i]
		expectedStrLen += len(l.Name) + len(l.Value)
	}

	// BigCache cannot handle cases where the key string has a size greater than
	// 16bits, so we error on such keys here. Since we are restricted to a 16bit
	// total length anyway, we only use 16bits to store the legth of each substring
	// in our string encoding
	if expectedStrLen > math.MaxUint16 {
		return metricName, fmt.Errorf("series too long, combined series has length %d, max length %d", expectedStrLen, math.MaxUint16)
	}

	// the string representation is
	//   (<key-len>key <val-len> val)* (<key-len>key <val-len> val)?
	// that is a series of the a sequence of key values pairs with each string
	// prefixed with it's length as a little-endian uint16
	builder.Grow(expectedStrLen)

	lengthBuf := make([]byte, 2)
	for i := range labels {
		l := labels[i]
		key := l.Name

		if l.Name == model.MetricNameLabelName {
			metricName = l.Value
		}
		// this cast is safe since we check that the combined length of all the
		// strings fit within a uint16, each string's length must also fit
		binary.LittleEndian.PutUint16(lengthBuf, uint16(len(key)))
		builder.WriteByte(lengthBuf[0])
		builder.WriteByte(lengthBuf[1])
		builder.WriteString(key)

		val := l.Value

		// this cast is safe since we check that the combined length of all the
		// strings fit within a uint16, each string's length must also fit
		binary.LittleEndian.PutUint16(lengthBuf, uint16(len(val)))
		builder.WriteByte(lengthBuf[0])
		builder.WriteByte(lengthBuf[1])
		builder.WriteString(val)
	}

	return metricName, nil
}

type UnresolvedSeriesCache interface {
	GetSeries(key *model.SeriesCacheKey, labels []prompb.Label) (series *model.UnresolvedSeries, err error)
}

type StoredSeriesCache interface {
	PutSeries(key *model.SeriesCacheKey, series *model.StoredSeries)
	GetSeries(key *model.SeriesCacheKey) (series *model.StoredSeries, ok bool)
	EvictWithIndex(idxKey model.SeriesID)
	Reset()
	Len() int
	Cap() int
	Evictions() uint64
}

type StoredSeriesCacheImpl struct {
	cache        *clockcache.IndexedCache
	seriesIndex  map[*model.SeriesID]int
	maxSizeBytes uint64
}

func NewStoredSeriesCache(config Config, sigClose <-chan struct{}) *StoredSeriesCacheImpl {
	cache := &StoredSeriesCacheImpl{
		clockcache.IndexedCacheWithMetrics("series", "metric", config.SeriesCacheInitialSize),
		make(map[*model.SeriesID]int, config.SeriesCacheInitialSize),
		config.SeriesCacheMemoryMaxBytes,
	}

	if sigClose != nil {
		go cache.runSizeCheck(sigClose)
	}
	return cache
}

func (t *StoredSeriesCacheImpl) PutSeries(key *model.SeriesCacheKey, series *model.StoredSeries) {
	t.cache.Insert(key, series, uint64(len(key.String())+8))
}

func (t *StoredSeriesCacheImpl) GetSeries(key *model.SeriesCacheKey) (series *model.StoredSeries, present bool) {
	elem, present := t.cache.Get(key)
	if !present {
		return nil, false
	}
	return elem.(*model.StoredSeries), true
}

func (t *StoredSeriesCacheImpl) EvictWithIndex(idxKey model.SeriesID) {
	t.cache.EvictWithIndex(idxKey)
}

func (t *StoredSeriesCacheImpl) runSizeCheck(sigClose <-chan struct{}) {
	prev := uint64(0)
	ticker := time.NewTicker(GrowCheckDuration)
	for {
		select {
		case <-ticker.C:
			current := t.cache.Evictions()
			newEvictions := current - prev
			evictionsThresh := uint64(float64(t.Len()) * GrowEvictionThreshold)
			prev = current
			if newEvictions > evictionsThresh {
				t.grow(newEvictions)
			}
		case <-sigClose:
			return
		}
	}
}

func (t *StoredSeriesCacheImpl) grow(newEvictions uint64) {
	sizeBytes := t.cache.SizeBytes()
	oldSize := t.cache.Cap()
	if float64(sizeBytes)*1.2 >= float64(t.maxSizeBytes) {
		log.Warn("msg", "StoredSeries cache is too small and cannot be grown",
			"current_size_bytes", float64(sizeBytes), "max_size_bytes", float64(t.maxSizeBytes),
			"current_size_elements", oldSize, "check_interval", GrowCheckDuration,
			"new_evictions", newEvictions, "new_evictions_percent", 100*(float64(newEvictions)/float64(oldSize)))
		return
	}

	multiplier := GrowFactor
	if float64(sizeBytes)*multiplier >= float64(t.maxSizeBytes) {
		multiplier = float64(t.maxSizeBytes) / float64(sizeBytes)
	}
	if multiplier < 1.0 {
		return
	}

	newNumElements := int(float64(oldSize) * multiplier)
	log.Info("msg", "Growing the series cache",
		"new_size_elements", newNumElements, "current_size_elements", oldSize,
		"new_size_bytes", float64(sizeBytes)*multiplier, "max_size_bytes", float64(t.maxSizeBytes),
		"multiplier", multiplier,
		"new_evictions", newEvictions, "new_evictions_percent", 100*(float64(newEvictions)/float64(oldSize)))
	t.cache.ExpandTo(newNumElements)
}

func (t *StoredSeriesCacheImpl) Len() int {
	return t.cache.Len()
}

func (t *StoredSeriesCacheImpl) Cap() int {
	return t.cache.Cap()
}

func (t *StoredSeriesCacheImpl) Evictions() uint64 {
	return t.cache.Evictions()
}

// Reset should be concurrency-safe
func (t *StoredSeriesCacheImpl) Reset() {
	t.cache.Reset()
}

var keyPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}
