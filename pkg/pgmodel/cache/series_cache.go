// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package cache

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"sync"
	"unsafe"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

// this seems like a good default size for /active/ series. This results in Promscale using around 360MB on start.
const DefaultSeriesCacheSize = 1000000

// SeriesCache is a cache of model.Series entries.
type SeriesCache interface {
	Reset()
	GetSeriesFromProtos(labelPairs []prompb.Label) (series *model.Series, metricName string, err error)
	Len() int
	Cap() int
	Evictions() uint64
}

type SeriesCacheImpl struct {
	*ResizableCache
}

func NewSeriesCache(config Config, sigClose <-chan struct{}) *SeriesCacheImpl {
	return &SeriesCacheImpl{NewResizableCache(
		clockcache.WithMetrics("series", "metric", config.SeriesCacheInitialSize),
		config.SeriesCacheMemoryMaxBytes,
		sigClose)}
}

// Get the canonical version of a series if one exists.
// input: the string representation of a Labels as defined by generateKey()
func (t *SeriesCacheImpl) loadSeries(str string) (l *model.Series) {
	val, ok := t.Get(str)
	if !ok {
		return nil
	}
	return val.(*model.Series)
}

// Try to set a series as the canonical Series for a given string
// representation, returning the canonical version (which can be different in
// the even of multiple goroutines setting labels concurrently).
func (t *SeriesCacheImpl) setSeries(str string, lset *model.Series) *model.Series {
	//str not counted twice in size since the key and lset.str will point to same thing.
	val, inCache := t.Insert(str, lset, lset.FinalSizeBytes())
	if !inCache {
		// It seems that cache was full and eviction failed to remove
		// element due to starvation caused by a lot of concurrent gets
		// This is a signal to grow our cache
		log.Info("growing cache because of eviction starvation")
		t.grow()
	}
	return val.(*model.Series)
}

// generateKey takes an array of Prometheus labels, and a byte buffer. It
// stores a string representation (for hashing and comparison) of the labels
// array in the byte buffer, and returns the metric name which it found in the
// array of Prometheus labels.
//
// The string representation is guaranteed to uniquely represent the underlying
// label set, though need not be human-readable, or indeed, valid utf-8.
//
// The `labels` parameter contains an array of (name, value) pairs.
// The key which this function returns is the concatenation of sorted kv pairs
// where each of the key and value is prefixed by the little-endian
// representation of the two bytes of the uint16 length of the key/value string:
//
//	<key1-len>key1<val1-len>val1<key2-len>key2<val2-len>val2
//
// This formatting ensures isomorphism, hence preventing collisions.
// An example for how this transform works is as follows:
// label1 = ("hell", "oworld"), label2 = ("hello", "world").
// => "\x04\x00hell\x06\x00oworld\x05\x00hello\x05\x00world"
func generateKey(labels []prompb.Label, keyBuffer *bytes.Buffer) (metricName string, error error) {
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
	// total length anyway, we only use 16bits to store the length of each substring
	// in our string encoding
	if expectedStrLen > math.MaxUint16 {
		return metricName, fmt.Errorf("series too long, combined series has length %d, max length %d", expectedStrLen, math.MaxUint16)
	}

	keyBuffer.Grow(expectedStrLen)

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
		keyBuffer.WriteByte(lengthBuf[0])
		keyBuffer.WriteByte(lengthBuf[1])
		keyBuffer.WriteString(key)

		val := l.Value

		// this cast is safe since we check that the combined length of all the
		// strings fit within a uint16, each string's length must also fit
		binary.LittleEndian.PutUint16(lengthBuf, uint16(len(val)))
		keyBuffer.WriteByte(lengthBuf[0])
		keyBuffer.WriteByte(lengthBuf[1])
		keyBuffer.WriteString(val)
	}

	return metricName, nil
}

var keyPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// GetSeriesFromLabels converts a labels.Labels to a canonical model.Series object
func (t *SeriesCacheImpl) GetSeriesFromLabels(ls labels.Labels) (*model.Series, error) {
	ll := make([]prompb.Label, len(ls))
	for i := range ls {
		ll[i].Name = ls[i].Name
		ll[i].Value = ls[i].Value
	}
	l, _, err := t.GetSeriesFromProtos(ll)
	return l, err
}

// useByteAsStringNoCopy uses a byte slice as a string in a function callback
// without allocation of a copy. This is potentially unsafe and should only
// be used for performance critical code. The string passed to the function is
// only valid during the function call. Thus, you should make sure the callback
// function does not save the string through closures or inside other datastructures.
// the callback should not modify the byte slice.
//
// This usage convention guarantees the byte slice is valid during the callback
// call but makes no other guarantees.
func useByteAsStringNoCopy(b []byte, useAsStringFunc func(string)) {
	//use a byte to String conversion directly without allocation
	str := *(*string)(unsafe.Pointer(&b)) // #nosec
	useAsStringFunc(str)
}

// GetSeriesFromProtos returns a model.Series entry given a list of Prometheus
// prompb.Label.
// If the desired entry is not in the cache, a "placeholder" model.Series entry
// is constructed and put into the cache. It is not populated with database IDs
// until a later phase, see model.Series.SetSeriesID.
func (t *SeriesCacheImpl) GetSeriesFromProtos(labelPairs []prompb.Label) (*model.Series, string, error) {
	builder := keyPool.Get().(*bytes.Buffer)
	builder.Reset()
	defer keyPool.Put(builder)
	metricName, err := generateKey(labelPairs, builder)
	if err != nil {
		return nil, "", err
	}
	var series *model.Series
	useByteAsStringNoCopy(builder.Bytes(), func(key string) {
		series = t.loadSeries(key)
	})
	if series == nil {
		//this will allocate the key
		key := builder.String()
		series = model.NewSeries(key, labelPairs)
		series = t.setSeries(key, series)
	}

	return series, metricName, nil
}
