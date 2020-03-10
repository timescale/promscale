package pgmodel

import (
	"testing"
	"time"

	"github.com/allegro/bigcache"
)

func TestInt32Functions(t *testing.T) {
	values := []int32{0, 1, 10, 123, 12355, 6000000, 1<<31 - 1}
	for _, value := range values {
		if bytesInt32(int32Bytes(value)) != value {
			t.Errorf("invalid value: got %d, want %d", bytesInt32(int32Bytes(value)), value)
		}
	}

}

func TestBigCache(t *testing.T) {
	ss := []uint64{1, 5, 7, 9, 10}

	config := bigcache.DefaultConfig(10 * time.Minute)
	series, err := bigcache.NewBigCache(config)
	if err != nil {
		t.Fatal("unable to run test, unable to create labels cache")
	}
	cache := bCache{
		series: series,
	}

	if _, err := cache.GetSeries(5); err == nil {
		t.Errorf("found cache for a series that was not stored")
	}

	for _, series := range ss {
		if err := cache.SetSeries(series, SeriesID(series)); err != nil {
			t.Errorf("got unexpected error while storing series: %d", series)

		}
	}

	for _, series := range ss {
		if _, err := cache.GetSeries(series); err != nil {
			t.Errorf("got unexpected error while getting series: %d", series)
		}
	}

}
