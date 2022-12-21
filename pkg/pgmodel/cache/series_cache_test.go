// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package cache

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/prompb"
)

func TestGenerateKey(t *testing.T) {
	require.NoError(t, os.Setenv("IS_TEST", "true"))
	cache := NewSeriesCache(DefaultConfig, nil)

	var labels = []prompb.Label{
		{Name: "__name__", Value: "test"},
		{Name: "hell", Value: "oworld"},
		{Name: "hello", Value: "world"},
	}

	key := cache.getSeriesKey(labels)
	require.True(t, len(key) > 0)
	require.Equal(t, "AQEBAgED", key)
}

func BenchmarkGetSeriesKey(b *testing.B) {
	require.NoError(b, os.Setenv("IS_TEST", "true"))
	cache := NewSeriesCache(DefaultConfig, nil)

	var labels = []prompb.Label{
		{Name: "__name__", Value: "test"},
		{Name: "hell", Value: "oworld"},
		{Name: "hello", Value: "world"},
	}

	for i := 0; i < b.N; i++ {
		cache.getSeriesKey(labels)
	}

}

func TestGrowSeriesCache(t *testing.T) {
	testCases := []struct {
		name         string
		sleep        time.Duration
		cacheGrowCnt int
	}{
		{
			name:         "Grow criteria satisfied - we shoulnd't be evicting elements",
			sleep:        time.Millisecond,
			cacheGrowCnt: 1,
		},
		{
			name:         "Growth criteria not satisfied - we can keep evicting old elements",
			sleep:        time.Second * 2,
			cacheGrowCnt: 0,
		},
	}

	t.Setenv("IS_TEST", "true")
	evictionMaxAge = time.Second // tweaking it to not wait too long
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache := NewSeriesCache(Config{SeriesCacheInitialSize: 100, SeriesCacheMemoryMaxBytes: DefaultConfig.SeriesCacheMemoryMaxBytes}, nil)
			cacheGrowCounter := 0
			for i := 0; i < 200; i++ {
				cache.Insert(i, i, 1)
				if i%100 == 0 {
					time.Sleep(tc.sleep)
				}
				if cache.shouldGrow() {
					cache.grow()
					cacheGrowCounter++
				}
			}
			require.Equal(t, tc.cacheGrowCnt, cacheGrowCounter, "series cache unexpectedly grow")
		})
	}

}
