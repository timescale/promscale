// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package cache

import (
	"testing"

	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
)

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

			if err != errors.ErrEntryNotFound {
				t.Fatalf("got unexpected error:\ngot\n%s\nwanted\n%s\n", err, errors.ErrEntryNotFound)
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
