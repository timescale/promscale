# clockcache #

This package contains a CLOCK based approximate-LRU cache optimized for
concurrent usage. It minimizes both the number of locks held, and the number of
writes to shared cache lines. In the worst case, this implementation requires
_at most_ 1 shared-cache write on a hit, not counting lock acquisition.

## Issues With Concurrent LRU ##

Typically, LRU is implemented with a map and a doubly-linked list, all elements
are stored in both the map, and the list. Whenever there's a hit on a
cache-item, it's moved to the head of the list; evictions are done from the tail
of the list. This ensures that it's always the least-recently-hit element that's
evicted.

While conceptually simple, the standard implementation of LRU as significant
issues in a multi-processor setting, since _every_ read to the cache writes to
the underlying datastructures. Updating the LRU list requires at least 4 writes
on shared cache lines; 2 to unlink the element from it's old place in the list,
and 2 more to add it to the head. When the LRU is being accessed from multiple
cores, these cache lines will bounce among them, causing a significant slowdown.

## CLOCK-based LRU ##

The CLOCK algorithm approximates LRU. Conceptually, it augments a map with a
bitset, with each element owning one bit in the bitset. Whenever an element is
hit, it's bit is set to `1`. The evictor maintains a cursor into the bitset, and
reads it round-robin.

```
                    eviction cursor
                            v
bitset: 0 1 0 0 1 1 0 1 0 0 1 1 1 1 0
```

Whenever the evictor sees a `1` it sets the bit to `0`.
Whenever the evictor see a `0`, it evicts the element.
This means that eviction always evicts an element that hasn't been read since
the last time the evictor ran, and, since it reads the element in order, the
first such element is the element that hasn't been touched the longest since the
last run.

### Concurrent CLOCK ###

CLOCK two important benefits over regular LRU in a concurrent setting; the state
that changes on a hit is per-element instead of shared, and the changes made by
a hit are idempotent.

Regular LRU relies on a shared list, which is manipulated every time an element
is hit. CLOCK only requires a per-element bit to be set on a hit. This means
that on a hit, only a single value, and with the proper layout, a single cache
line, needs to be written to. Already, this reduces the worst-case number of
writes to 1 from 4. We can further reduce the number of writes by observing that
for an LRU to be useful, hits must be more common than evictions, and in CLOCK
any successive hits before an eviction need not change any state; the bit is
already set to `1`. This means that we can check the bit before writing it, and
do nothing if it's already set, further reducing the number of writes.

## Other Optimizations ##

The map consists of a hashmap and slice. The map is used to perform key
lookups, and the slice is used to ensure a consistent order for eviction checks.
The actual elements and staleness-bits are stored in the slice, there is no
separate bitset. Slice-elements nodes are padded to take a cache line
each to prevent false-sharing. Staleness-bits are set using atomic operations,
and are only written after they have been read to be `0`. The slice and map are
preallocated to the maximum capacity of the cache, this prevents most
allocations within the critical section (the map will occasionally allocate
even with preallocation). We have separate locks for the slice and the cache;
during eviction we grab a lock on only the slice, and allow gets to proceed
until we find an element to evict. At that point we lock the map and replace the
element.
