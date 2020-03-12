package pgmodel

import (
	"testing"
	"time"

	"github.com/allegro/bigcache"
	"github.com/prometheus/prometheus/pkg/labels"
)

func TestBigCache(t *testing.T) {
	config := bigcache.DefaultConfig(10 * time.Minute)
	series, err := bigcache.NewBigCache(config)
	if err != nil {
		t.Fatal("unable to run test, unable to create labels cache")
	}
	cache := bCache{
		series: series,
	}

	label := labels.Labels{
		labels.Label{
			Name:  "name1",
			Value: "val1",
		},
	}

	if _, err := cache.GetSeries(label); err == nil {
		t.Errorf("found cache for a series that was not stored")
	}

	testLabels := []labels.Labels{
		labels.Labels{
			labels.Label{
				Name:  "name1",
				Value: "val1",
			},
		},
		labels.Labels{
			labels.Label{
				Name:  "name1",
				Value: "val2",
			},
		},
		labels.Labels{
			labels.Label{
				Name:  "name2",
				Value: "val2",
			},
		},
		labels.Labels{
			labels.Label{
				Name:  "name1",
				Value: "val1",
			},
			labels.Label{
				Name:  "name1",
				Value: "val1",
			},
		},
	}

	for i, series := range testLabels {
		if err := cache.SetSeries(series, SeriesID(i)); err != nil {
			t.Errorf("got unexpected error while storing series: %d", i)

		}
	}

	for i, series := range testLabels {
		var res SeriesID
		if res, err = cache.GetSeries(series); err != nil {
			t.Errorf("got unexpected error while getting series: %v", series)
		}
		if res != SeriesID(i) {
			t.Errorf("wrong id returned: got %v expected %v", res, i)
		}
	}

}

func TestBigCachePanicUnsorted(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Didn't throw panic as expected")
		}
	}()

	unsorted := labels.Labels{
		labels.Label{
			Name:  "name2",
			Value: "val1",
		},
		labels.Label{
			Name:  "name1",
			Value: "val1",
		},
	}

	config := bigcache.DefaultConfig(10 * time.Minute)
	series, err := bigcache.NewBigCache(config)
	if err != nil {
		t.Fatal("unable to run test, unable to create labels cache")
	}
	cache := bCache{
		series: series,
	}
	cache.SetSeries(unsorted, SeriesID(0))
}
