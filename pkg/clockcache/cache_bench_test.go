package clockcache

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// microbenchmark. measure the length of the Insert critical section
func BenchmarkIntCache(b *testing.B) {
	keys := make([]interface{}, b.N)
	for i := range keys {
		keys[i] = i
	}
	cache := WithMax(uint64(b.N))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Insert(keys[i], keys[i], 32)
	}
}

// microbenchmark. measure the length of the Insert critical section
func BenchmarkStringCache(b *testing.B) {
	keys := make([]interface{}, b.N)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	cache := WithMax(uint64(b.N))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Insert(keys[i], keys[i], 32)
	}
}

// microbenchmark. measure the time it takes to evict on a fully-used cache.
// attempts to measure worst-cache eviction time, but under-measures contention.
func BenchmarkEviction(b *testing.B) {
	for _, n := range []int{500, 5000, 50000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			keys := make([]interface{}, n)
			vals := make([]interface{}, n)
			insertKeys := make([]interface{}, b.N)

			for i := 0; i < n; i++ {
				keys[i], vals[i] = n, n
			}
			rand.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
			})

			for i := 0; i < b.N; i++ {
				insertKeys[i] = b.N + i
			}
			rand.Shuffle(len(insertKeys), func(i, j int) {
				insertKeys[i], insertKeys[j] = insertKeys[j], insertKeys[i]
			})

			cache := WithMax(uint64(n / 4))
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				bval, _ = cache.Insert(insertKeys[i%n], insertKeys[i%n], 16)
				cache.markAll()
			}
		})
	}
}

func (c *Cache) markAll() {
	now := int32(time.Now().Unix())
	for i := range c.storage {
		c.storage[i].usedWithTs = packUsedAndTimestamp(true, now)
	}
}

// microbenchmark. attempts to measure a cache hit
func BenchmarkMembership(b *testing.B) {
	for _, n := range []int{500, 5000, 50000, 500000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			rng := rand.New(rand.NewSource(299792458))

			keys := make([]interface{}, n)
			vals := make([]interface{}, n)
			sizes := make([]uint64, n)

			for i := 0; i < n; i++ {
				keys[i], vals[i], sizes[i] = i, i, 16
			}

			rng.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
			})

			cache := WithMax(uint64(n))
			cache.InsertBatch(keys, vals, sizes)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				bval, _ = cache.Get(keys[i%n])
			}
		})
	}
}

// microbenchmark. attempts to measure a cache miss
func BenchmarkNotFound(b *testing.B) {
	for _, n := range []int{500, 5000, 50000, 500000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			rng := rand.New(rand.NewSource(299792458))

			keys := make([]interface{}, n)
			vals := make([]interface{}, n)
			sizes := make([]uint64, n)
			gets := make([]interface{}, n)

			for i := 0; i < n; i++ {
				keys[i], vals[i], sizes[i] = i, i, 16
				gets[i] = n + i
			}

			rng.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
			})

			cache := WithMax(uint64(n))
			cache.InsertBatch(keys, vals, sizes)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				bval, _ = cache.Get(gets[i%n])
			}
		})
	}
}

// more macro benchmarks

var bval interface{}

func BenchmarkInsertUnderCapacity(b *testing.B) {
	for _, n := range []int{500, 5000, 50000, 500000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			rng := rand.New(rand.NewSource(299792458))
			zipf := rand.NewZipf(rng, 1.07, 1, 1e9)

			keys := make([]interface{}, n)
			vals := make([]interface{}, n)

			for i := 0; i < n; i++ {
				keys[i], vals[i] = zipf.Uint64(), rng.Intn(1e9)
			}

			cache := WithMax(uint64(n))
			b.ReportAllocs()
			b.ResetTimer()

			inserts := 0
			var inserted bool
			for i := 0; i < b.N; i++ {
				bval, inserted = cache.Insert(keys[i%n], vals[i%n], 16)
				if inserted {
					inserts++
				}
				b.ReportMetric(float64(inserts), "inserts")
				b.ReportMetric(float64(inserts)/float64(b.N), "inserts/ops")
			}
		})
	}
}

func BenchmarkInsertOverCapacity(b *testing.B) {
	for _, n := range []int{500, 5000, 50000, 500000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			rng := rand.New(rand.NewSource(299792458))
			zipf := rand.NewZipf(rng, 1.07, 1, 1e9)

			keys := make([]interface{}, n)
			vals := make([]interface{}, n)

			for i := 0; i < n; i++ {
				keys[i], vals[i] = zipf.Uint64(), rng.Intn(1e9)
			}

			cache := WithMax(uint64(n / 4))
			b.ReportAllocs()
			b.ResetTimer()

			inserts := 0
			var inserted bool
			for i := 0; i < b.N; i++ {
				bval, inserted = cache.Insert(keys[i%n], vals[i%n], 16)
				if inserted {
					cache.Get(keys[i%n])
					inserts++
				}
			}
			b.ReportMetric(float64(inserts), "inserts")
			b.ReportMetric(float64(inserts)/float64(b.N), "inserts/ops")
		})
	}
}

func BenchmarkInsertConcurrent(b *testing.B) {
	for _, n := range []int{500, 5000, 50000, 500000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			rng := rand.New(rand.NewSource(299792458))
			zipf := rand.NewZipf(rng, 1.07, 1, 1e9)

			keys := make([]interface{}, n)
			vals := make([]interface{}, n)

			for i := 0; i < n; i++ {
				keys[i], vals[i] = zipf.Uint64(), rng.Intn(1e9)
			}

			cache := WithMax(uint64(n / 4))
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var val interface{}
				i := 0
				for pb.Next() {
					val, _ = cache.Insert(keys[i%n], vals[i%n], 16)
					cache.Get(keys[i%n])
					i++
				}
				bval = val
			})
		})
	}
}

// BenchmarkCacheFalseSharing is a benchmark to measure the effect of the false
// sharing of CPU cache lines. In the clockcache.element struct, we introduce
// padding to ensure that only one clockcache.element fits in a CPU cache line,
// avoiding false sharing.
//
// The principle behind this benchmark is simple: construct a cache with two
// entries, and start two goroutines which each clobber one of the cache values
// over and over again. If there is false sharing, it should be measurable by
// toggling the padding on and off, and measuring the difference in output of
// this benchmark.
//
// At the time of writing, this code was tested on an M1 MacBook Pro, where the
// advantage obtained by introducing padding is approximately 16%:
//     go test -bench=BenchmarkCacheFalseSharing -cpu=2 -count=10 > no-padding.txt
//     go test -bench=BenchmarkCacheFalseSharing -cpu=2 -count=10 > padding.txt
//     benchstat no-padding.txt padding.txt
//       name                 old time/op    new time/op    delta
//       CacheFalseSharing-2     230ns ± 6%     193ns ±19%  -16.09%  (p=0.001 n=10+9)
//
// Note: This benchmark _must_ be run with the `-cpu=2` argument, to ensure
// that each goroutine ends up on a different CPU, possibly causing contention
// for the same cache line.
func BenchmarkCacheFalseSharing(b *testing.B) {
	cache := WithMax(2)
	b.ReportAllocs()

	// define waitgroup so that we can coordinate the start of the stressors
	startWg := &sync.WaitGroup{}
	startWg.Add(2)

	// define waitgroup, so we can wait until concurrent stressors are finished
	endWg := &sync.WaitGroup{}
	endWg.Add(2)

	key1 := 0
	key2 := 1
	times := b.N

	// stressor is a function to be run in a goroutine which continually writes
	// and reads to/from a specific key in the cache
	stressor := func(key, count int) {
		var val interface{}

		// Coordinate the start of the two stressors
		startWg.Done()
		startWg.Wait()

		// Reset the timer immediately before doing the real work
		b.ResetTimer()
		for i := 0; i < count; i++ {
			cache.Insert(key, i, 16)
			val, _ = cache.Get(key)
		}

		bval = val
		endWg.Done()
	}

	// run two contending goroutines
	go stressor(key1, times)
	go stressor(key2, times)

	// wait for tasks to complete
	endWg.Wait()
}
func BenchmarkMemoryEmptyCache(b *testing.B) {
	b.ReportAllocs()
	WithMax(uint64(b.N))
}
