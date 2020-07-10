package clockcache

import (
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"unsafe"
)

func TestWriteAndGetOnCache(t *testing.T) {
	t.Parallel()

	cache := WithMax(100)

	cache.Insert("1", 1)
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

	cache.Insert("key", 1)

	val, found = cache.Get("nonExistingKey")
	if found {
		t.Errorf("found %d for noexistent key", val)
	}
}

func TestEviction(t *testing.T) {
	t.Parallel()

	cache := WithMax(10)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%d", i)
		cache.Insert(key, int64(i))
		if i != 5 {
			cache.Get(key)
		}
	}

	cache.Insert("100", 100)
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

	val, found := cache.Get("100")
	if !found || val != 100 {
		t.Errorf("missing value 100, got %d", val)
	}

	cache.Insert("101", 101)
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
			cache.Insert(key, r+1)
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

	cache.InsertBatch([]interface{}{3, 6, 9, 12}, []interface{}{4, 7, 10, 13})

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
	cache.Insert(1, 1)
	cache.Get(1)

	cache.Insert(2, 2)

	cache.Insert(3, 3)
	cache.Get(3)

	expected := "[1: 1, 2: 2, 3: 3, ]"
	if cache.debugString() != expected {
		t.Errorf("unexpected cache\nexpected\n\t%s\nfound\n\t%s\n", expected, cache.debugString())
	}

	cache.Insert(4, 4)
	expected = "[1: 1, 4: 4, 3: 3, ]"
	if cache.debugString() != expected {
		t.Errorf("unexpected cache\nexpected\n\t%s\nfound\n\t%s\n", expected, cache.debugString())
	}

	cache.ExpandTo(5)
	expected = "[1: 1, 4: 4, 3: 3, ]"
	if cache.debugString() != expected {
		t.Errorf("unexpected cache\nexpected\n\t%s\nfound\n\t%s\n", expected, cache.debugString())
	}

	cache.Insert(5, 5)
	cache.Get(5)

	cache.Insert(6, 6)
	expected = "[1: 1, 4: 4, 3: 3, 5: 5, 6: 6, ]"
	if cache.debugString() != expected {
		t.Errorf("unexpected cache\nexpected\n\t%s\nfound\n\t%s\n", expected, cache.debugString())
	}

	cache.Insert(7, 7)
	expected = "[1: 1, 4: 4, 3: 3, 5: 5, 7: 7, ]"
	if cache.debugString() != expected {
		t.Errorf("unexpected cache\nexpected\n\t%s\nfound\n\t%s\n", expected, cache.debugString())
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
