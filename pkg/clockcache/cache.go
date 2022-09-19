// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package clockcache

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

// CLOCK based approximate LRU storing designed for concurrent usage.
// Gets only require a read lock, while Inserts take at least one write lock.
type Cache struct {
	metrics *perfMetrics
	// guards elements and all fields except for `used` in Element, must have at
	// least a read-lock to access, and a write-lock to insert/update/delete.
	elementsLock sync.RWMutex
	// stores indexes into storage
	elements map[interface{}]*element
	storage  []element
	// Information elements:
	// size of everything stored by storage, does not include size of the cache structure itself
	dataSize uint64
	// number of evictions
	evictions uint64
	// max eviction timestamp in unix seconds
	maxEvictionTs int32

	// guards next, and len(storage) and ensures that at most one eviction
	// occurs at a time, always grabbed _before_ elementsLock
	insertLock sync.Mutex
	// CLOCK sweep state, must have the insertLock
	next int
}

type element struct {
	// The value stored with this element.
	key   interface{}
	value interface{}

	// CLOCK marker if this element is recently used together with timestamp
	// To avoid memory waste we pack together `used` flag with timestamp into 32 bits
	// Left most bit represents if element was used (1 or 0). The rest (31 bits) represent timestamp
	// Timestamp is helping us figure out if we should attempt to grow cache or not
	// Timestamp is stored in Unix seconds (31 bits should be enough for the next 15 years)
	usedWithTs uint32
	size       uint64

	// pad Elements out to be cache-aligned, see BenchmarkCacheFalseSharing
	_ [16]byte
}

func WithMax(max uint64) *Cache {
	return &Cache{
		metrics:  &perfMetrics{}, // Unregistered metrics.
		elements: make(map[interface{}]*element, max),
		storage:  make([]element, 0, max),
	}
}

func (self *Cache) applyPerfMetric(m *perfMetrics) {
	self.metrics = m
}

// Insert a key/value mapping into the cache if the key is not already present,
// The sizeBytes represents the in-memory size of the key and value (used to estimate cache size).
// returns the canonical version of the value
// and if the value is in the map
func (self *Cache) Insert(key interface{}, value interface{}, sizeBytes uint64) (canonicalValue interface{}, in_cache bool) {
	self.insertLock.Lock()
	defer self.insertLock.Unlock()

	elem, _, inCache := self.insert(key, value, sizeBytes)
	return elem.value, inCache
}

// InsertBatch inserts a batch of keys with their corresponding values.
// This function will _overwrite_ the keys and values slices with their
// canonical versions.
// sizesBytes is the in-memory size of the key+value of each element.
// returns the number of elements inserted, is lower than len(keys) if insertion
// starved
func (self *Cache) InsertBatch(keys []interface{}, values []interface{}, sizesBytes []uint64) int {
	if len(keys) != len(values) {
		panic(fmt.Sprintf("keys and values are not the same len. %d keys, %d values", len(keys), len(values)))
	}
	values = values[:len(keys)]
	self.insertLock.Lock()
	defer self.insertLock.Unlock()

	for idx := range keys {
		elem, _, inCache := self.insert(keys[idx], values[idx], sizesBytes[idx])
		keys[idx], values[idx] = elem.key, elem.value
		if !inCache {
			return idx
		}
	}
	return len(keys)
}

func (self *Cache) insert(key interface{}, value interface{}, size uint64) (existingElement *element, inserted bool, inCache bool) {
	elem, present := self.elements[key]
	if present {
		// we'll count a double-insert as a hit. See the comment in get
		if !extractUsed(atomic.LoadUint32(&elem.usedWithTs)) {
			atomic.StoreUint32(&elem.usedWithTs, packUsedAndTimestamp(true, int32(time.Now().Unix())))
		}
		return elem, false, true
	}

	var insertLocation *element
	newElement := element{key: key, value: value, size: size, usedWithTs: packUsedAndTimestamp(false, int32(time.Now().Unix()))}
	if len(self.storage) >= cap(self.storage) {
		insertLocation = self.evict()
		if insertLocation == nil {
			return &newElement, false, false
		}
		self.elementsLock.Lock()
		defer self.elementsLock.Unlock()
		delete(self.elements, insertLocation.key)
		self.dataSize -= insertLocation.size
		self.dataSize += size
		self.evictions++
		evictionTs := extractTimestamp(insertLocation.usedWithTs)
		if self.maxEvictionTs < evictionTs {
			self.maxEvictionTs = evictionTs
		}
		*insertLocation = newElement
	} else {
		self.elementsLock.Lock()
		defer self.elementsLock.Unlock()
		self.storage = append(self.storage, newElement)
		self.dataSize += size
		insertLocation = &self.storage[len(self.storage)-1]
	}

	self.elements[key] = insertLocation
	return insertLocation, true, true
}

// Update updates the cache entry at key position with the new value and size. It inserts the key if not found and
// returns the 'inserted' as true.
func (self *Cache) Update(key, value interface{}, size uint64) (canonicalValue interface{}) {
	self.insertLock.Lock()
	defer self.insertLock.Unlock()
	existingElement, inserted, inCache := self.insert(key, value, size)
	// Avoid EQL value comparisons as value is always an interface. Incase, the value is of type map, EQL operator will panic.
	if !inserted && inCache && (existingElement.size != size || !reflect.DeepEqual(existingElement.value, value)) {
		// If it is inserted (instead of updated), we don't need to go into this block, as the props are already updated.
		self.elementsLock.Lock()
		defer self.elementsLock.Unlock()
		existingElement.value = value
		existingElement.size = size
	}
	return existingElement.value
}

func (self *Cache) evict() (insertPtr *element) {
	// this code goes around storage in a ring searching for the first element
	// not marked as used, which it will evict. The code has two unusual
	// features:
	//  1. it will go through storage at most twice before giving up. Concurrent
	//     gets can starve out the evictor, in which case the cache is too small
	//  2. it divides the walk through storage into two loops, one walk through
	//     all the elements after the last place the evictor stopped, one
	//     through all elements before that location. This is due to a limitation
	//     in go's bounds check elimination, where it will only eliminate checks
	//     based off an induction variable e.g. `next := range slice`,
	//     if the value is merely guarded by e.g. `if next >= len(slice) { next = 0 }`
	//     the bounds check will not be elided. Doing the walk like this lowers
	//     eviction time by about a third
	startLoc := self.next
	postStart := self.storage[startLoc:]
	preStart := self.storage[:startLoc]
	for i := 0; i < 2; i++ {
		for next := range postStart {
			elem := &postStart[next]
			old := atomic.SwapUint32(&elem.usedWithTs, setUsed(false, elem.usedWithTs))
			if !extractUsed(old) {
				insertPtr = elem
			}

			if insertPtr != nil {
				self.next = (startLoc + next + 1) % self.Len()
				return
			}
		}
		for next := range preStart {
			elem := &preStart[next]
			old := atomic.SwapUint32(&elem.usedWithTs, setUsed(false, elem.usedWithTs))
			if !extractUsed(old) {
				insertPtr = elem
			}

			if insertPtr != nil {
				self.next = next + 1
				return
			}
		}
	}

	return
}

// tries to get a batch of keys and store the corresponding values is valuesOut
// returns the number of keys that were actually found.
// NOTE: this function does _not_ preserve the order of keys; the first numFound
//       keys will be the keys whose values are present, while the remainder
//       will be the keys not present in the cache
func (self *Cache) GetValues(keys []interface{}, valuesOut []interface{}) (numFound int) {
	start := time.Now()
	defer func() { self.metrics.Observe("Get_Values", time.Since(start)) }()

	if len(keys) != len(valuesOut) {
		panic(fmt.Sprintf("keys and values are not the same len. %d keys, %d values", len(keys), len(valuesOut)))
	}
	valuesOut = valuesOut[:len(keys)]
	n := len(keys)
	idx := 0

	self.elementsLock.RLock()
	defer self.elementsLock.RUnlock()

	for idx < n {
		value, found := self.get(keys[idx])
		if !found {
			if n == 0 {
				return 0
			}
			// no value found for key, swap the key with the last element, and shrink n
			n -= 1
			keys[n], keys[idx] = keys[idx], keys[n]
			continue
		}
		valuesOut[idx] = value
		idx += 1
	}
	return n
}

func (self *Cache) Get(key interface{}) (interface{}, bool) {
	start := time.Now()
	defer func() { self.metrics.Observe("Get", time.Since(start)) }()

	self.elementsLock.RLock()
	defer self.elementsLock.RUnlock()
	return self.get(key)
}

func (self *Cache) get(key interface{}) (interface{}, bool) {
	self.metrics.Inc(self.metrics.queriesTotal)
	elem, present := self.elements[key]
	if !present {
		return 0, false
	}

	// While logically this is a CompareAndSwap, this code has an important
	// advantage: in the common case of the element already being marked as used,
	// this is a read-only operation, and doesn't trash the cache line that used
	// is stored on. The lack of atomicity of the update doesn't matter for our
	// use case.
	if !extractUsed(atomic.LoadUint32(&elem.usedWithTs)) {
		atomic.StoreUint32(&elem.usedWithTs, packUsedAndTimestamp(true, int32(time.Now().Unix())))
	}
	self.metrics.Inc(self.metrics.hitsTotal)

	return elem.value, true
}

func (self *Cache) unmark(key string) bool {
	self.elementsLock.RLock()
	defer self.elementsLock.RUnlock()

	elem, present := self.elements[key]
	if !present {
		return false
	}

	// While logically this is a CompareAndSwap, this code has an important
	// advantage: in the common case of the element already being marked as used,
	// this is a read-only operation, and doesn't trash the cache line that used
	// is stored on. The lack of atomicity of the update doesn't matter for our
	// use case.
	if extractUsed(atomic.LoadUint32(&elem.usedWithTs)) {
		atomic.StoreUint32(&elem.usedWithTs, setUsed(false, elem.usedWithTs))
	}

	return true
}

func (self *Cache) ExpandTo(newMax int) {
	self.insertLock.Lock()
	defer self.insertLock.Unlock()

	oldMax := cap(self.storage)
	if newMax <= oldMax {
		return
	}

	newStorage := make([]element, 0, newMax)

	// cannot use copy here despite the data race on element.used
	for i := range self.storage {
		elem := &self.storage[i]
		newStorage = append(newStorage, element{
			key:        elem.key,
			value:      elem.value,
			usedWithTs: atomic.LoadUint32(&elem.usedWithTs),
		})
	}

	newElements := make(map[interface{}]*element, newMax)
	for i := range newStorage {
		elem := &newStorage[i]
		newElements[elem.key] = elem
	}

	self.elementsLock.Lock()
	defer self.elementsLock.Unlock()

	self.elements = newElements
	self.storage = newStorage
	self.maxEvictionTs = 0
	self.evictions = 0
}

func (self *Cache) Reset() {
	self.insertLock.Lock()
	defer self.insertLock.Unlock()
	oldSize := cap(self.storage)

	newElements := make(map[interface{}]*element, oldSize)
	newStorage := make([]element, 0, oldSize)

	self.elementsLock.Lock()
	defer self.elementsLock.Unlock()
	self.elements = newElements
	self.storage = newStorage
	self.next = 0
	self.maxEvictionTs = 0
	self.evictions = 0
}

func (self *Cache) Len() int {
	self.elementsLock.RLock()
	defer self.elementsLock.RUnlock()
	return len(self.storage)
}

func (self *Cache) Evictions() uint64 {
	self.elementsLock.RLock()
	defer self.elementsLock.RUnlock()
	return self.evictions
}

func (self *Cache) MaxEvictionTs() int32 {
	self.elementsLock.RLock()
	defer self.elementsLock.RUnlock()
	return self.maxEvictionTs
}

func (self *Cache) SizeBytes() uint64 {
	self.elementsLock.RLock()
	defer self.elementsLock.RUnlock()
	//derived from BenchmarkMemoryEmptyCache 120 bytes per element
	cacheSize := cap(self.storage) * 120
	return uint64(cacheSize) + self.dataSize
}

func (self *Cache) Cap() int {
	self.elementsLock.RLock()
	defer self.elementsLock.RUnlock()
	return cap(self.storage)
}

func (self *Cache) debugString() string {
	self.elementsLock.RLock()
	defer self.elementsLock.RUnlock()
	str := "["
	for i := range self.storage {
		elem := &self.storage[i]
		str = fmt.Sprintf("%s%v: %v, ", str, elem.key, elem.value)
	}
	return fmt.Sprintf("%s]", str)
}

// extractTimestamp skips leftmost bit and converts the rest into timestamp (unix seconds)
func extractTimestamp(in uint32) int32 {
	return int32(in & ((1 << 31) - 1))
}

// extractUsed gets the first bit from the left
// checking if element has been used
func extractUsed(in uint32) bool {
	return 1 == (in >> 31)
}

// setUsed sets first bit from the left. This bit marks if element has been used
func setUsed(used bool, in uint32) uint32 {
	if used {
		return in | uint32(1<<31)
	}
	return in &^ uint32(1<<31)
}

// packUsedAndTimestamp packs bool flag and timestamp into 32 bits
func packUsedAndTimestamp(used bool, ts int32) uint32 {
	var usedBit int
	if used {
		usedBit = 1
	}
	return uint32(usedBit<<31) | uint32(ts)
}

// RemoveMatchingItems removes items whose value passes the check applied by
// the passed `matcher` function. It returns the number of entries which were
// removed from the cache.
// Note: Due to the locks that this function takes, it is best used when very
// few items stored in the cache are to be removed.
func (self *Cache) RemoveMatchingItems(matcher func(value interface{}) bool) int {
	// Note: we take an RLock to find matching items, then release it and take
	// a full Lock to remove them. This has two assumptions:
	// 1. len(matchingItems) << len(storage): taking a big lock should be fine.
	// 2. Missing the addition of an item to the cache is not problematic.
	//    Locking like this doesn't provide a consistent view of the cache
	//    contents. Either an entry is removed or added under our feet. An
	//    entry being removed is not a problem, we will just skip it. An entry
	//    being added is fine for the one consumer of this method (cache
	//    invalidation): if an item is added to the cache, then it was just
	//    created/resurrected in the DB, so it wouldn't have been a candidate
	//    for removal.

	self.elementsLock.RLock()
	matchingItems := make([]interface{}, 0)
	for k, v := range self.elements {
		if matcher(v.value) {
			matchingItems = append(matchingItems, k)
		}
	}
	self.elementsLock.RUnlock()
	self.elementsLock.Lock()
	for i := range matchingItems {
		self.remove(i)
	}
	self.elementsLock.Unlock()
	return len(matchingItems)
}

func (self *Cache) remove(key interface{}) {
	value, present := self.elements[key]
	if !present {
		// do nothing
		return
	}
	delete(self.elements, key)
	*value = element{}
}
