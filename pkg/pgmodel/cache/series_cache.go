// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package cache

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

//this seems like a good initial size for /active/ series. Takes about 32MB
const DefaultSeriesCacheSize = 250000

const GrowCheckDuration = time.Minute //check whether to grow the series cache this often
const GrowEvictionThreshold = 0.2     // grow when evictions more than 20% of cache size
const GrowFactor = float64(2.0)       // multiply cache size by this factor when growing the cache

type SeriesCache interface {
	Reset()
	GetSeriesFromProtos(labelPairs []prompb.Label) (series *model.Series, metricName string, err error)
	Len() int
	Cap() int
	Evictions() uint64
}

type SeriesCacheImpl struct {
	cache        *clockcache.Cache
	maxSizeBytes uint64
}

func NewSeriesCache(config Config, sigClose <-chan struct{}) *SeriesCacheImpl {
	cache := &SeriesCacheImpl{
		clockcache.WithMax(config.SeriesCacheInitialSize),
		config.SeriesCacheMemoryMaxBytes,
	}

	if sigClose != nil {
		go cache.runSizeCheck(sigClose)
	}
	return cache
}

func (t *SeriesCacheImpl) runSizeCheck(sigClose <-chan struct{}) {
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

func (t *SeriesCacheImpl) grow(newEvictions uint64) {
	sizeBytes := t.cache.SizeBytes()
	oldSize := t.cache.Cap()
	if float64(sizeBytes)*1.2 >= float64(t.maxSizeBytes) {
		log.Warn("msg", "Series cache is too small and cannot be grown",
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

func (t *SeriesCacheImpl) Len() int {
	return t.cache.Len()
}

func (t *SeriesCacheImpl) Cap() int {
	return t.cache.Cap()
}

func (t *SeriesCacheImpl) Evictions() uint64 {
	return t.cache.Evictions()
}

//ResetStoredLabels should be concurrency-safe
func (t *SeriesCacheImpl) Reset() {
	t.cache.Reset()
}

// Get the canonical version of a series if one exists.
// input: the string representation of a Labels as defined by generateKey()
// This function should not be called directly, use labelProtosToLabels() or
// LabelsFromSlice() instead.
func (t *SeriesCacheImpl) loadSeries(str string) (l *model.Series) {
	val, ok := t.cache.Get(str)
	if !ok {
		return nil
	}
	return val.(*model.Series)
}

// Try to set a series as the canonical Series for a given string
// representation, returning the canonical version (which can be different in
// the even of multiple goroutines setting labels concurrently).
// This function should not be called directly, use labelProtosToLabels() or
// LabelsFromSlice() instead.
func (t *SeriesCacheImpl) setSeries(str string, lset *model.Series) *model.Series {
	//str not counted twice in size since the key and lset.str will point to same thing.
	val, _ := t.cache.Insert(str, lset, lset.FinalSizeBytes())
	return val.(*model.Series)
}

// Get a string representation for hashing and comparison
// This representation is guaranteed to uniquely represent the underlying label
// set, though need not human-readable, or indeed, valid utf-8
func generateKey(labels []prompb.Label) (key string, metricName string, error error) {
	if len(labels) == 0 {
		return "", "", nil
	}

	comparator := func(i, j int) bool {
		return labels[i].Name < labels[j].Name
	}

	if !sort.SliceIsSorted(labels, comparator) {
		sort.Slice(labels, comparator)
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
		return "", metricName, fmt.Errorf("series too long, combined series has length %d, max length %d", expectedStrLen, ^uint16(0))
	}

	// the string representation is
	//   (<key-len>key <val-len> val)* (<key-len>key <val-len> val)?
	// that is a series of the a sequence of key values pairs with each string
	// prefixed with it's length as a little-endian uint16
	builder := strings.Builder{}
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

	return builder.String(), metricName, nil
}

// GetSeriesFromLabels converts a labels.Labels to a canonical Labels object
func (t *SeriesCacheImpl) GetSeriesFromLabels(ls labels.Labels) (*model.Series, error) {
	ll := make([]prompb.Label, len(ls))
	for i := range ls {
		ll[i].Name = ls[i].Name
		ll[i].Value = ls[i].Value
	}
	l, _, err := t.GetSeriesFromProtos(ll)
	return l, err
}

// GetSeriesFromProtos converts a prompb.Label to a canonical Labels object
func (t *SeriesCacheImpl) GetSeriesFromProtos(labelPairs []prompb.Label) (*model.Series, string, error) {
	key, metricName, err := generateKey(labelPairs)
	if err != nil {
		return nil, "", err
	}
	series := t.loadSeries(key)
	if series == nil {
		series = model.NewSeries(key, labelPairs)
		series = t.setSeries(key, series)
	}

	return series, metricName, nil

}
