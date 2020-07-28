package clockcache

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// CLOCK based approximate LRU storing designed for concurrent usage.
// Gets only require a read lock, while Inserts take at least one write lock.
type Cache struct {
	// guards elements and all fields except for `used` in Element, must have at
	// least a read-lock to access, and a write-lock to insert/update/delete.
	elementsLock sync.RWMutex
	// stores indexes into storage
	elements map[interface{}]*element
	storage  []element

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

	// CLOCK marker if this is recently used
	used uint32

	// pad Elements out to be cache aligned
	_ [24]byte
}

func WithMax(max uint64) *Cache {
	return &Cache{
		elements: make(map[interface{}]*element, max),
		storage:  make([]element, 0, max),
	}
}

// Insert a key/value mapping into the cache if the key is not already present
// returns the canonical version of the value
// and if the value is in the map
func (self *Cache) Insert(key interface{}, value interface{}) (canonicalValue interface{}, in_cache bool) {
	self.insertLock.Lock()
	defer self.insertLock.Unlock()

	_, canonicalValue, in_cache = self.insert(key, value)
	return
}

// Insert a batch of keys with their corresponding values.
// This function will _overwrite_ the keys and values slices with their
// canonical versions.
// returns the number of elements inserted, is lower than len(keys) if insertion
// starved
func (self *Cache) InsertBatch(keys []interface{}, values []interface{}) int {
	if len(keys) != len(values) {
		panic(fmt.Sprintf("keys and values are not the same len. %d keys, %d values", len(keys), len(values)))
	}
	values = values[:len(keys)]
	self.insertLock.Lock()
	defer self.insertLock.Unlock()

	for idx := range keys {
		var inserted bool
		keys[idx], values[idx], inserted = self.insert(keys[idx], values[idx])
		if !inserted {
			return idx
		}
	}
	return len(keys)
}

func (self *Cache) insert(key interface{}, value interface{}) (canonicalKey interface{}, canonicalValue interface{}, inserted bool) {
	elem, present := self.elements[key]
	if present {
		// we'll count a double-insert as a hit. See the comment in get
		if atomic.LoadUint32(&elem.used) != 0 {
			atomic.StoreUint32(&elem.used, 1)
		}
		return elem.key, elem.value, true
	}

	var insertLocation *element
	if len(self.storage) >= cap(self.storage) {
		insertLocation = self.evict()
		if insertLocation == nil {
			return key, value, false
		}
		self.elementsLock.Lock()
		defer self.elementsLock.Unlock()
		delete(self.elements, insertLocation.key)
		*insertLocation = element{key: key, value: value}
	} else {
		self.elementsLock.Lock()
		defer self.elementsLock.Unlock()
		self.storage = append(self.storage, element{key: key, value: value})
		insertLocation = &self.storage[len(self.storage)-1]
	}

	self.elements[key] = insertLocation
	return key, value, true
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
			old := atomic.SwapUint32(&elem.used, 0)
			if old == 0 {
				insertPtr = elem
			}

			if insertPtr != nil {
				self.next = next + 1
				return
			}
		}
		for next := range preStart {
			elem := &preStart[next]
			old := atomic.SwapUint32(&elem.used, 0)
			if old == 0 {
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
	self.elementsLock.RLock()
	defer self.elementsLock.RUnlock()
	return self.get(key)
}

func (self *Cache) get(key interface{}) (interface{}, bool) {

	elem, present := self.elements[key]
	if !present {
		return 0, false
	}

	// While logically this is a CompareAndSwap, this code has an important
	// advantage: in the common case of the element already being marked as used,
	// this is a read-only operation, and doesn't trash the cache line that used
	// is stored on. The lack of atomicity of the update doesn't matter for our
	// use case.
	if atomic.LoadUint32(&elem.used) == 0 {
		atomic.StoreUint32(&elem.used, 1)
	}

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
	if atomic.LoadUint32(&elem.used) != 0 {
		atomic.StoreUint32(&elem.used, 0)
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
			key:   elem.key,
			value: elem.value,
			used:  atomic.LoadUint32(&elem.used),
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
}

func (self *Cache) Len() int {
	self.elementsLock.RLock()
	defer self.elementsLock.RUnlock()
	return len(self.storage)
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
