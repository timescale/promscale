// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package pgmodel

import (
	"math"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/timescale/promscale/pkg/clockcache"
)

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

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			cache := MetricNameCache{
				Metrics: clockcache.WithMax(100),
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
