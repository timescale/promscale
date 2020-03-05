package pgmodel

import (
	"testing"
	"time"

	"github.com/allegro/bigcache"
	"github.com/prometheus/prometheus/prompb"
)

func TestUint64Functions(t *testing.T) {
	values := []uint64{0, 1, 10, 123, 12355, 6000000, 18446744073709551615}
	for _, value := range values {
		if bytesUint64(uint64Bytes(value)) != value {
			t.Errorf("invalid value: got %d, want %d", bytesUint64(uint64Bytes(value)), value)
		}

		if uint64String(value) != string(uint64Bytes(value)) {
			t.Errorf("invalid string value: got %s, want %s", uint64String(value), string(uint64Bytes(value)))
		}
	}
}

func TestInt32Functions(t *testing.T) {
	values := []int32{0, 1, 10, 123, 12355, 6000000, 1<<31 - 1}
	for _, value := range values {
		if bytesInt32(int32Bytes(value)) != value {
			t.Errorf("invalid value: got %d, want %d", bytesInt32(int32Bytes(value)), value)
		}
	}

}

func TestBigCache(t *testing.T) {
	ll := []*prompb.Label{
		&prompb.Label{
			Name:  "foo",
			Value: "bar",
		},
		&prompb.Label{
			Name:  "test",
			Value: "val",
		},
	}
	ss := []uint64{1, 5, 7, 9, 10}

	config := bigcache.DefaultConfig(10 * time.Minute)
	labels, err := bigcache.NewBigCache(config)
	if err != nil {
		t.Fatal("unable to run test, unable to create labels cache")
	}
	series, err := bigcache.NewBigCache(config)
	if err != nil {
		t.Fatal("unable to run test, unable to create labels cache")
	}
	cache := bCache{
		labels: labels,
		series: series,
	}

	if _, err := cache.GetLabel(&prompb.Label{}); err == nil {
		t.Errorf("found cache for a label that was not stored")
	}

	for _, label := range ll {
		if err := cache.SetLabel(label, 1); err != nil {
			t.Errorf("got unexpected error while storing label: %s", label.String())

		}
	}

	for _, label := range ll {
		if _, err := cache.GetLabel(label); err != nil {
			t.Errorf("got unexpected error while getting label: %s", label.String())
		}
	}

	if _, err := cache.GetSeries(5); err == nil {
		t.Errorf("found cache for a series that was not stored")
	}

	for _, series := range ss {
		if err := cache.SetSeries(series, series); err != nil {
			t.Errorf("got unexpected error while storing series: %d", series)

		}
	}

	for _, series := range ss {
		if _, err := cache.GetSeries(series); err != nil {
			t.Errorf("got unexpected error while getting series: %d", series)
		}
	}

}
