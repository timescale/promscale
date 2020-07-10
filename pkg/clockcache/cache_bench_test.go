package clockcache

import (
	"fmt"
	"math/rand"
	"testing"
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
		cache.Insert(keys[i], keys[i])
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
		cache.Insert(keys[i], keys[i])
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
				bval, _ = cache.Insert(insertKeys[i%n], insertKeys[i%n])
				cache.markAll()
			}
		})
	}
}

func (c *Cache) markAll() {
	for i := range c.storage {
		c.storage[i].used = 2
	}
}

// microbenchmark. attempts to measure a cache hit
func BenchmarkMembership(b *testing.B) {
	for _, n := range []int{500, 5000, 50000, 500000} {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			rng := rand.New(rand.NewSource(299792458))

			keys := make([]interface{}, n)
			vals := make([]interface{}, n)

			for i := 0; i < n; i++ {
				keys[i], vals[i] = i, i
			}

			rng.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
			})

			cache := WithMax(uint64(n))
			cache.InsertBatch(keys, vals)
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
			gets := make([]interface{}, n)

			for i := 0; i < n; i++ {
				keys[i], vals[i] = i, i
				gets[i] = n + i
			}

			rng.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
			})

			cache := WithMax(uint64(n))
			cache.InsertBatch(keys, vals)
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
				bval, inserted = cache.Insert(keys[i%n], vals[i%n])
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
				bval, inserted = cache.Insert(keys[i%n], vals[i%n])
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
					val, _ = cache.Insert(keys[i%n], vals[i%n])
					cache.Get(keys[i%n])
					i++
				}
				bval = val
			})
		})
	}
}
