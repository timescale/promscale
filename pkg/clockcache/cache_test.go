// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package clockcache

import (
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestWriteAndGetOnCache(t *testing.T) {
	t.Parallel()

	cache := WithMax(100)

	cache.Insert("1", 1, 8+1+8)
	val, found := cache.Get("1")

	// then
	if !found {
		t.Error("no value found")
	}
	if val != 1 {
		t.Errorf("expected %d found %d", 1, val)
	}
}

func TestEntryNotFound(t *testing.T) {
	t.Parallel()

	cache := WithMax(100)

	val, found := cache.Get("nonExistingKey")
	if found {
		t.Errorf("found %d for noexistent key", val)
	}

	cache.Insert("key", 1, 8+1+8)

	val, found = cache.Get("nonExistingKey")
	if found {
		t.Errorf("found %d for noexistent key", val)
	}
}

func TestUpdate(t *testing.T) {
	t.Parallel()

	cache := WithMax(100)

	val, found := cache.Get("nonExistingKey")
	if found {
		t.Errorf("found %d for noexistent key", val)
	}

	cache.Insert("key", 1, 8+1+8)
	val, found = cache.Get("key")
	if !found {
		t.Errorf("not found for 'key'")
	}
	if val.(int) != 1 {
		t.Fatal("wrong value received")
	}

	canonicalVal := cache.Update("key", 2, 8+1+8)
	if !reflect.DeepEqual(canonicalVal, 2) {
		t.Errorf("canonical value returned was not updated")
	}
	val, found = cache.Get("key")
	if !found {
		t.Errorf("not found for 'key'")
	}
	if val != 2 {
		t.Errorf("updated value does not match for 'key'")
	}
}

func TestEviction(t *testing.T) {
	t.Parallel()

	cache := WithMax(10)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%d", i)
		cache.Insert(key, int64(i), uint64(8+len(key)+8))
		if i != 5 {
			cache.Get(key)
		}
	}

	cache.Insert("100", 100, 8+3+8)
	cache.Get("100")

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%d", i)
		val, found := cache.Get(key)
		if i != 5 && (!found || val != int64(i)) {
			t.Errorf("missing value %d, got %d", i, val)
		} else if i == 5 && found {
			t.Errorf("5 not evicted")
		}
		if i == 2 {
			cache.unmark(key)
		}
	}
	if cache.Evictions() != 1 {
		t.Errorf("Got wrong number of evictions %v", cache.Evictions())
	}

	val, found := cache.Get("100")
	if !found || val != 100 {
		t.Errorf("missing value 100, got %d", val)
	}

	cache.Insert("101", 101, 8+3+8)
	cache.Get("101")

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%d", i)
		val, found := cache.Get(key)
		if i != 5 && i != 2 && (!found || val != int64(i)) {
			t.Errorf("missing value %d, (found: %v) got %d", i, found, val)
		} else if (i == 5 || i == 2) && found {
			t.Errorf("%d not evicted", i)
		}
	}
	if cache.Evictions() != 2 {
		t.Errorf("Got wrong number of evictions %v", cache.Evictions())
	}
	val, found = cache.Get("100")
	if !found || val != 100 {
		t.Errorf("missing value 100, got %d", val)
	}
	val, found = cache.Get("101")
	if !found || val != 101 {
		t.Errorf("missing value 101, got %d", val)
	}
}

func TestCacheGetRandomly(t *testing.T) {
	t.Parallel()

	cache := WithMax(10000)
	var wg sync.WaitGroup
	var ntest = 800000
	wg.Add(2)
	go func() {
		for i := 0; i < ntest; i++ {
			r := rand.Int63() % 20000
			key := fmt.Sprintf("%d", r)
			cache.Insert(key, r+1, uint64(8+len(key)+8))
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < ntest; i++ {
			r := rand.Int63()
			key := fmt.Sprintf("%d", r)
			if val, found := cache.Get(key); found && val != r+1 {
				t.Errorf("got %s ->\n %x\n expected:\n %x\n ", key, val, r+1)
			}
		}
		wg.Done()
	}()
	wg.Wait()
}

func TestBatch(t *testing.T) {
	t.Parallel()

	cache := WithMax(10)

	cache.InsertBatch([]interface{}{3, 6, 9, 12}, []interface{}{4, 7, 10, 13}, []uint64{16, 16, 16, 16})

	keys := []interface{}{1, 2, 3, 6, 9, 12, 13}
	vals := make([]interface{}, len(keys))
	numFound := cache.GetValues(keys, vals)

	if numFound != 4 {
		t.Errorf("found incorrect number of values: expected 4, found %d\n\tkeys: %v\n\t%v", numFound, keys, vals)
	}

	expectedKeys := []interface{}{12, 9, 3, 6, 2, 13, 1}
	if !reflect.DeepEqual(keys, expectedKeys) {
		t.Errorf("unexpected keys:\nexpected\n\t%v\nfound\n\t%v", keys, expectedKeys)
	}

	expectedVals := []interface{}{13, 10, 4, 7, nil, nil, nil}
	if !reflect.DeepEqual(vals, expectedVals) {
		t.Errorf("unexpected values:\nexpected\n\t%v\nfound\n\t%v", expectedVals, vals)
	}
}

func TestExpand(t *testing.T) {
	cache := WithMax(3)
	cache.Insert(1, 1, 16)
	cache.Get(1)

	cache.Insert(2, 2, 16)

	cache.Insert(3, 3, 16)
	cache.Get(3)

	expected := "[1: 1, 2: 2, 3: 3, ]"
	if cache.debugString() != expected {
		t.Errorf("unexpected cache\nexpected\n\t%s\nfound\n\t%s\n", expected, cache.debugString())
	}

	if cache.SizeBytes() != 3*120+3*16 {
		t.Errorf("Unexpected size %v", cache.SizeBytes())
	}

	require.Equal(t, int32(0), cache.MaxEvictionTs(), "unexpected eviction")

	cache.Insert(4, 4, 20)
	if cache.maxEvictionTs == 0 {
		t.Errorf("Eviction timestamp missing")
	}
	expected = "[1: 1, 4: 4, 3: 3, ]"
	if cache.debugString() != expected {
		t.Errorf("unexpected cache\nexpected\n\t%s\nfound\n\t%s\n", expected, cache.debugString())
	}

	if cache.SizeBytes() != 3*120+2*16+20 {
		t.Errorf("Unexpected size %v", cache.SizeBytes())
	}

	cache.ExpandTo(5)
	require.Equal(t, int32(0), cache.MaxEvictionTs(), "max eviction timestamp not reset")
	expected = "[1: 1, 4: 4, 3: 3, ]"
	if cache.debugString() != expected {
		t.Errorf("unexpected cache\nexpected\n\t%s\nfound\n\t%s\n", expected, cache.debugString())
	}

	if cache.SizeBytes() != 5*120+2*16+20 {
		t.Errorf("Unexpected size %v", cache.SizeBytes())
	}

	cache.Insert(5, 5, 16)
	cache.Get(5)

	cache.Insert(6, 6, 16)
	expected = "[1: 1, 4: 4, 3: 3, 5: 5, 6: 6, ]"
	if cache.debugString() != expected {
		t.Errorf("unexpected cache\nexpected\n\t%s\nfound\n\t%s\n", expected, cache.debugString())
	}
	if cache.SizeBytes() != 5*120+4*16+20 {
		t.Errorf("Unexpected size %v", cache.SizeBytes())
	}

	cache.Insert(7, 7, 16)
	expected = "[1: 1, 4: 4, 3: 3, 5: 5, 7: 7, ]"
	if cache.debugString() != expected {
		t.Errorf("unexpected cache\nexpected\n\t%s\nfound\n\t%s\n", expected, cache.debugString())
	}
	if cache.SizeBytes() != 5*120+4*16+20 {
		t.Errorf("Unexpected size %v", cache.SizeBytes())
	}
}

func TestReset(t *testing.T) {
	cache := WithMax(3)
	cache.Insert(1, 1, 16)
	cache.Get(1)

	cache.Insert(2, 2, 16)

	cache.Insert(3, 3, 16)
	cache.Get(3)

	expected := "[1: 1, 2: 2, 3: 3, ]"
	if cache.debugString() != expected {
		t.Errorf("unexpected cache\nexpected\n\t%s\nfound\n\t%s\n", expected, cache.debugString())
	}

	cache.Reset()
	expected = "[]"
	if cache.debugString() != expected {
		t.Errorf("unexpected cache\nexpected\n\t%s\nfound\n\t%s\n", expected, cache.debugString())
	}

	cache.Insert(5, 5, 1)
	cache.Get(5)
	expected = "[5: 5, ]"
	if cache.debugString() != expected {
		t.Errorf("unexpected cache\nexpected\n\t%s\nfound\n\t%s\n", expected, cache.debugString())
	}

	cache.Insert(6, 6, 1)
	cache.Insert(7, 7, 1)
	expected = "[5: 5, 6: 6, 7: 7, ]"
	if cache.debugString() != expected {
		t.Errorf("unexpected cache\nexpected\n\t%s\nfound\n\t%s\n", expected, cache.debugString())
	}

	cache.Insert(8, 8, 1)
	expected = "[5: 5, 8: 8, 7: 7, ]"
	if cache.debugString() != expected {
		t.Errorf("unexpected cache\nexpected\n\t%s\nfound\n\t%s\n", expected, cache.debugString())
	}

	cache.Reset()
	cache.Reset()
	expected = "[]"
	if cache.debugString() != expected {
		t.Errorf("unexpected cache\nexpected\n\t%s\nfound\n\t%s\n", expected, cache.debugString())
	}

	if cache.Len() != 0 {
		t.Error("Incorrrect len")
	}
	if cache.Cap() != 3 {
		t.Error("Incorrrect len")
	}
}

func TestElementCacheAligned(t *testing.T) {
	elementSize := unsafe.Sizeof(element{})
	if elementSize%64 != 0 {
		t.Errorf("unaligned element size: %d", elementSize)
	}
	if elementSize != 64 {
		t.Errorf("unexpected element size: %d", elementSize)
	}
}

func TestCacheEvictionOnWraparound(t *testing.T) {
	elems := 3
	cache := WithMax(uint64(elems))
	cachedItems := make([]int, elems)

	// Prepopulate cache
	for i := 0; i < 3; i++ {
		cache.Insert(i, i, 1)
		cachedItems[i%3] = i
	}

	assertCacheContains := func(cache *Cache, pseudoCache []int) {
		cacheKeys := make([]int, elems)
		i := 0
		for k := range cache.elements {
			cacheKeys[i] = k.(int)
			i += 1
		}
		require.ElementsMatch(t, cacheKeys, pseudoCache)
	}

	// Let's cause a bunch of evictions
	for i := 0; i < 100; i++ {
		cache.Insert(i, i, 1)
		// Track what we expect the cache to contain
		cachedItems[i%3] = i
		// Assert that our expectations are met
		assertCacheContains(cache, cachedItems)
	}
}

func TestBitPacking(t *testing.T) {
	timestamp := int32(time.Date(2020, time.January, 1, 1, 0, 0, 0, time.Local).UTC().Unix())
	used := true
	packed := packUsedAndTimestamp(used, int32(timestamp))
	usedExt := extractUsed(packed)
	require.Equal(t, used, usedExt, "used flag not properly extracted")
	timestampExt := extractTimestamp(packed)
	require.Equal(t, timestamp, timestampExt, "timestamp not properly extracted")
	packed = setUsed(false, packed)
	usedExt = extractUsed(packed)
	require.Equal(t, false, usedExt, "used flag not properly set")
}
