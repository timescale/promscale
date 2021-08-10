// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package cache

import (
	"reflect"
	"testing"

	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/model"
)

func TestMetricTableNameCache(t *testing.T) {
	testCases := []struct {
		name        string
		schema      string
		metric      string
		tableSchema string
		tableName   string
		seriesTable string
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
		{
			name:        "empty schema",
			metric:      "metric",
			tableName:   "tableName",
			seriesTable: "tableName",
		},
		{
			name:        "default schema",
			schema:      "",
			metric:      "metric",
			tableSchema: "schema",
			tableName:   "tableName",
			seriesTable: "tableName",
		},
		{
			name:        "with schema",
			schema:      "schema",
			metric:      "metric",
			tableSchema: "schema",
			tableName:   "tableName",
			seriesTable: "tableName",
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			cache := MetricNameCache{
				Metrics: clockcache.WithMax(100),
			}

			mInfo, err := cache.Get(c.schema, c.metric, false)

			if mInfo.TableName != "" {
				t.Fatal("found cache that should be missing, not stored yet")
			}

			if err != errors.ErrEntryNotFound {
				t.Fatalf("got unexpected error:\ngot\n%s\nwanted\n%s\n", err, errors.ErrEntryNotFound)
			}

			err = cache.Set(
				c.schema,
				c.metric,
				model.MetricInfo{
					TableSchema: c.tableSchema,
					TableName:   c.tableName,
					SeriesTable: c.seriesTable,
				},
				false,
			)

			if err != nil {
				t.Fatalf("got unexpected error:\ngot\n%s\nwanted\nnil\n", err)
			}

			mInfo, err = cache.Get(c.schema, c.metric, false)

			if mInfo.TableSchema != c.tableSchema {
				t.Fatalf("found wrong cache schema value: got %s wanted %s", mInfo.TableSchema, c.schema)
			}
			if mInfo.TableName != c.tableName {
				t.Fatalf("found wrong cache table name value: got %s wanted %s", mInfo.TableName, c.tableName)
			}
			if mInfo.SeriesTable != c.seriesTable {
				t.Fatalf("found wrong cache series table name value: got %s wanted %s", mInfo.SeriesTable, c.seriesTable)
			}

			if err != nil {
				t.Fatalf("got unexpected error:\ngot\n%s\nwanted\nnil\n", err)
			}

			// Check if specific schema key is set with correct value
			if c.schema == "" && c.tableSchema != "" {
				mInfo, err = cache.Get(c.tableSchema, c.metric, false)

				if mInfo.TableSchema != c.tableSchema {
					t.Fatalf("found wrong cache schema value: got %s wanted %s", mInfo.TableSchema, c.tableSchema)
				}
				if mInfo.TableName != c.tableName {
					t.Fatalf("found wrong cache table name value: got %s wanted %s", mInfo.TableName, c.tableName)
				}
				if mInfo.SeriesTable != c.seriesTable {
					t.Fatalf("found wrong cache series table name value: got %s wanted %s", mInfo.SeriesTable, c.seriesTable)
				}

				if err != nil {
					t.Fatalf("got unexpected error:\ngot\n%s\nwanted\nnil\n", err)
				}
			}
		})
	}
}

func TestMetricNameCacheExemplarEntry(t *testing.T) {
	metric, table := "test_metric", "test_table"
	mInfo := model.MetricInfo{TableName: table}
	cache := NewMetricCache(Config{MetricsCacheSize: 2})
	_, foundErr := cache.Get("", metric, false)
	if foundErr != errors.ErrEntryNotFound {
		t.Fatal("entry found for non inserted data")
	}
	err := cache.Set("", metric, mInfo, false)
	if err != nil {
		t.Fatal(err)
	}
	val, err := cache.Get("", metric, false)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(val, mInfo) {
		t.Fatalf("metric entry does not match table entry")
	}
	_, err = cache.Get("", metric, true)
	if err != errors.ErrEntryNotFound {
		t.Fatalf("exemplar metric not set, but still exists")
	}
	err = cache.Set("", metric, mInfo, true)
	if err != nil {
		t.Fatalf(err.Error())
	}
	// Should have entry both for exemplar metric and sample metric.
	val, err = cache.Get("", metric, true)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if !reflect.DeepEqual(val, mInfo) {
		t.Fatalf("does not match")
	}
	val, err = cache.Get("", metric, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if !reflect.DeepEqual(val, mInfo) {
		t.Fatalf("does not match")
	}
}
