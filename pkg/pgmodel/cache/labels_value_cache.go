// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package cache

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/timescale/promscale/pkg/clockcache"
)

type LabelIDInfo struct {
	labelID int32
	Pos     int32
}

func (info *LabelIDInfo) FinalSizeBytes() uint64 {
	return 4
}

type LabelValueCache interface {
	Reset()
	GetLabelIDInfo(lbl labels.Label) (*LabelIDInfo, error)
	SetLabelIDInfo(lbl labels.Label, info *LabelIDInfo) (*LabelIDInfo, error)
	Len() int
	Cap() int
	Evictions() uint64
}

type labelValueCacheImpl struct {
	cache *clockcache.Cache
}

func NewLabelValueCache(config Config, sigClose <-chan struct{}) *labelValueCacheImpl {
	cache := &labelValueCacheImpl{
		clockcache.WithMax(config.LabelsCacheSize),
	}
	return cache
}

func (t *labelValueCacheImpl) Len() int {
	return t.cache.Len()
}

func (t *labelValueCacheImpl) Cap() int {
	return t.cache.Cap()
}

func (t *labelValueCacheImpl) Evictions() uint64 {
	return t.cache.Evictions()
}

//ResetStoredLabels should be concurrency-safe
func (t *labelValueCacheImpl) Reset() {
	t.cache.Reset()
}

// Get the canonical version of a series if one exists.
// input: the string representation of a Labels as defined by generateKey()
// This function should not be called directly, use labelProtosToLabels() or
// LabelsFromSlice() instead.
func (t *labelValueCacheImpl) loadlabelValue(str string) (l *LabelIDInfo) {
	val, ok := t.cache.Get(str)
	if !ok {
		return nil
	}
	return val.(*LabelIDInfo)
}

// Try to set a series as the canonical Series for a given string
// representation, returning the canonical version (which can be different in
// the even of multiple goroutines setting labels concurrently).
// This function should not be called directly, use labelProtosToLabels() or
// LabelsFromSlice() instead.
func (t *labelValueCacheImpl) setLabelValue(str string, info *LabelIDInfo) *LabelIDInfo {
	val, _ := t.cache.Insert(str, info, info.FinalSizeBytes()+uint64(len(str)))
	return val.(*LabelIDInfo)
}

// Get a string representation for hashing and comparison
// This representation is guaranteed to uniquely represent the underlying label
// set, though need not human-readable, or indeed, valid utf-8
func generateLabelKey(lbls labels.Label) (key string, error error) {
	expectedStrLen := 4 + len(lbls.Name) + len(lbls.Value) // 2 for the length of each key, and 2 for the length of each value

	// BigCache cannot handle cases where the key string has a size greater than
	// 16bits, so we error on such keys here. Since we are restricted to a 16bit
	// total length anyway, we only use 16bits to store the legth of each substring
	// in our string encoding
	if expectedStrLen > math.MaxUint16 {
		return "", fmt.Errorf("labels too long, combined series has length %d, max length %d", expectedStrLen, ^uint16(0))
	}

	// the string representation is
	//   <key-len>key <val-len> val
	// that is a series of the a sequence of key values pairs with each string
	// prefixed with it's length as a little-endian uint16
	builder := strings.Builder{}
	builder.Grow(expectedStrLen)

	lengthBuf := make([]byte, 2)

	name := lbls.Name

	// this cast is safe since we check that the combined length of all the
	// strings fit within a uint16, each string's length must also fit
	binary.LittleEndian.PutUint16(lengthBuf, uint16(len(name)))
	builder.WriteByte(lengthBuf[0])
	builder.WriteByte(lengthBuf[1])
	builder.WriteString(name)

	val := lbls.Value

	// this cast is safe since we check that the combined length of all the
	// strings fit within a uint16, each string's length must also fit
	binary.LittleEndian.PutUint16(lengthBuf, uint16(len(val)))
	builder.WriteByte(lengthBuf[0])
	builder.WriteByte(lengthBuf[1])
	builder.WriteString(val)

	return builder.String(), nil
}

// GetSeriesFromProtos converts a prompb.Label to a canonical Labels object
func (t *labelValueCacheImpl) GetLabelIDInfo(lbl labels.Label) (*LabelIDInfo, error) {
	key, err := generateLabelKey(lbl)
	if err != nil {
		return nil, err
	}
	info := t.loadlabelValue(key)

	return info, nil
}

func (t *labelValueCacheImpl) SetLabelIDInfo(lbl labels.Label, info *LabelIDInfo) (*LabelIDInfo, error) {
	key, err := generateLabelKey(lbl)
	if err != nil {
		return nil, err
	}
	infoCanonical := t.setLabelValue(key, info)

	return infoCanonical, nil
}
