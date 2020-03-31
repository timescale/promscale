package pgmodel

import (
	"math"
	"strings"
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

	l, err := LabelsFromSlice(label)
	if err != nil {
		t.Errorf("invalid labels %+v: %v", l, err)
	}
	if _, err := cache.GetSeries(l); err == nil {
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
				Name:  "name2",
				Value: "val2",
			},
		},
	}

	for i, series := range testLabels {
		ls, err := LabelsFromSlice(series)
		if err != nil {
			t.Errorf("invalid series %+v, %v", ls, err)
		}
		if err := cache.SetSeries(ls, SeriesID(i)); err != nil {
			t.Errorf("got unexpected error while storing series: %d", i)

		}
	}

	for i, series := range testLabels {
		var res SeriesID
		ls, err := LabelsFromSlice(series)
		if err != nil {
			t.Errorf("invalid series %+v, %v", ls, err)
		}
		if res, err = cache.GetSeries(ls); err != nil {
			t.Errorf("got unexpected error while getting series: %v", series)
		}
		if res != SeriesID(i) {
			t.Errorf("wrong id returned: got %v expected %v", res, i)
		}
	}

}

func TestBigLables(t *testing.T) {
	builder := strings.Builder{}
	builder.Grow(int(^uint16(0)) + 1) // one greater than uint16 max

	builder.WriteByte('a')
	for len(builder.String()) < math.MaxUint16 {
		builder.WriteString(builder.String())
	}

	labels := labels.Labels{
		labels.Label{
			Name:  builder.String(),
			Value: "",
		},
	}

	_, err := LabelsFromSlice(labels)
	if err == nil {
		t.Errorf("expected error")
	}
}

func TestMetricTableNameCache(t *testing.T) {
	testCases := []struct {
		name      string
		metric    string
		tableName string
	}{
		{
			name:      "empty",
			metric:    "",
			tableName: "",
		},
		{
			name:      "simple metric",
			metric:    "metric",
			tableName: "metricTableName",
		},
		{
			name:      "metric as table name",
			metric:    "metric",
			tableName: "metric",
		},
		{
			name:      "empty table name",
			metric:    "metric",
			tableName: "",
		},
	}
	config := bigcache.DefaultConfig(10 * time.Minute)

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			metrics, err := bigcache.NewBigCache(config)

			if err != nil {
				t.Fatal("unable to run test, unable to create metrics table name cache")
			}
			cache := MetricNameCache{
				Metrics: metrics,
			}

			missing, err := cache.Get(c.metric)

			if missing != "" {
				t.Fatal("found cache that should be missing, not stored yet")
			}

			if err != ErrEntryNotFound {
				t.Fatalf("got unexpected error:\ngot\n%s\nwanted\n%s\n", err, ErrEntryNotFound)
			}

			err = cache.Set(c.metric, c.tableName)

			if err != nil {
				t.Fatalf("got unexpected error:\ngot\n%s\nwanted\nnil\n", err)
			}

			found, err := cache.Get(c.metric)

			if found != c.tableName {
				t.Fatalf("found wrong cache value: got %s wanted %s", found, c.tableName)
			}

			if err != nil {
				t.Fatalf("got unexpected error:\ngot\n%s\nwanted\nnil\n", err)
			}
		})
	}
}
