package clockcache

import (
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"testing"
)

func TestBuildIndex(t *testing.T) {
	cache := IndexedCacheWithMax(1)
	seriesId := model.SeriesID(1)
	value := model.NewStoredSeries(seriesId, 1)
	cache.Insert(1, value, 20)

	_, present := cache.index[seriesId]
	require.True(t, present)
}

func TestIndexEvictionRemovesElement(t *testing.T) {
	cache := IndexedCacheWithMax(1)
	for i := 1; i < 10; i++ {
		key := i
		seriesId := model.SeriesID(i)
		value := model.NewStoredSeries(seriesId, 1)
		cache.Insert(key, value, 20)

		_, present := cache.index[seriesId]
		require.True(t, present)

		cache.EvictWithIndex(seriesId)
		cache.EvictWithIndex(seriesId)

		_, present = cache.index[seriesId]
		require.False(t, present)
		require.Equal(t, cache.evictions, uint64(2*i-1))
		require.Equal(t, cache.dataSize, uint64(0))
		require.Len(t, cache.elements, 0)
	}
}

func TestCapacityEvictionHandlesIndex(t *testing.T) {
	cache := IndexedCacheWithMax(1)
	for i := 1; i < 10; i++ {
		key := i
		seriesId := model.SeriesID(i)
		value := model.NewStoredSeries(seriesId, 1)
		cache.Insert(key, value, 20)
	}
	for i := 1; i < 9; i++ {
		_, present := cache.index[model.SeriesID(1)]
		require.False(t, present)
	}
}
