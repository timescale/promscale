// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package end_to_end_tests

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

func TestSQLJsonLabelArray(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	testCases := []struct {
		name        string
		metrics     []prompb.TimeSeries
		arrayLength map[string]int
	}{
		{
			name: "One metric",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "metric1"},
						{Name: "test", Value: "test"},
					},
				},
			},
			arrayLength: map[string]int{"metric1": 2},
		},
		{
			name: "Long keys and values",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: strings.Repeat("val", 60)},
						{Name: strings.Repeat("key", 60), Value: strings.Repeat("val2", 60)},
					},
				},
			},
		},
		{
			name: "New keys and values",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "metric1"},
						{Name: "test", Value: "test"},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "metric1"},
						{Name: "test1", Value: "test"},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "metric1"},
						{Name: "test", Value: "test"},
						{Name: "test1", Value: "test"},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "metric1"},
						{Name: "test", Value: "val1"},
						{Name: "test1", Value: "val2"},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "metric1"},
						{Name: "test", Value: "test"},
						{Name: "test1", Value: "val2"},
					},
				},
			},
		},
		{
			name: "Multiple metrics",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "m1"},
						{Name: "test1", Value: "val1"},
						{Name: "test2", Value: "val1"},
						{Name: "test3", Value: "val1"},
						{Name: "test4", Value: "val1"},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "m2"},
						{Name: "test", Value: "test"},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "m1"},
						{Name: "test1", Value: "val2"},
						{Name: "test2", Value: "val2"},
						{Name: "test3", Value: "val2"},
						{Name: "test4", Value: "val2"},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "m2"},
						{Name: "test", Value: "test2"},
					},
				},
			},
			//make sure each metric's array is compact
			arrayLength: map[string]int{"m1": 5, "m2": 2},
		},
	}

	for tcIndex, c := range testCases {
		databaseName := fmt.Sprintf("%s_%d", *testDatabase, tcIndex)
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			withDB(t, databaseName, func(db *pgxpool.Pool, t testing.TB) {
				for _, ts := range c.metrics {
					labelSet := make(model.LabelSet, len(ts.Labels))
					metricName := ""
					kvMap := make(map[string]string)
					keys := make([]string, 0)
					values := make([]string, 0)
					for _, l := range ts.Labels {
						if l.Name == "__name__" {
							metricName = l.Value
						}
						labelSet[model.LabelName(l.Name)] = model.LabelValue(l.Value)
						keys = append(keys, l.Name)
						values = append(values, l.Value)
						kvMap[l.Name] = l.Value
					}

					jsonOrig, err := json.Marshal(labelSet)
					if err != nil {
						t.Fatal(err)
					}
					var labelArray []int
					err = db.QueryRow(context.Background(), "SELECT * FROM prom_api.label_array($1)", jsonOrig).Scan(&labelArray)
					if err != nil {
						t.Fatal(err)
					}
					if c.arrayLength != nil {
						expected, ok := c.arrayLength[metricName]
						if ok && expected != len(labelArray) {
							t.Fatalf("Unexpected label array length: got\n%v\nexpected\n%v", len(labelArray), expected)
						}
					}

					var labelArrayKV []int
					err = db.QueryRow(context.Background(), "SELECT * FROM prom_api.label_array($1, $2, $3)", metricName, keys, values).Scan(&labelArrayKV)
					if err != nil {
						t.Fatal(err)
					}
					if c.arrayLength != nil {
						expected, ok := c.arrayLength[metricName]
						if ok && expected != len(labelArrayKV) {
							t.Fatalf("Unexpected label array length: got\n%v\nexpected\n%v", len(labelArrayKV), expected)
						}
					}

					if !reflect.DeepEqual(labelArray, labelArrayKV) {
						t.Fatalf("Expected label arrays to be equal: %v != %v", labelArray, labelArrayKV)
					}

					var jsonRes []byte
					err = db.QueryRow(context.Background(), "SELECT * FROM jsonb(($1::int[]))", labelArray).Scan(&jsonRes)
					if err != nil {
						t.Fatal(err)
					}
					fingerprintRes := getFingerprintFromJSON(t, jsonRes)
					if labelSet.Fingerprint() != fingerprintRes {
						t.Fatalf("Json not equal: got\n%v\nexpected\n%v", string(fingerprintRes), string(jsonOrig))

					}

					var (
						retKeys []string
						retVals []string
					)
					err = db.QueryRow(context.Background(), "SELECT * FROM prom_api.key_value_array($1::int[])", labelArray).Scan(&retKeys, &retVals)
					if err != nil {
						t.Fatal(err)
					}
					if len(retKeys) != len(retVals) {
						t.Errorf("invalid kvs, # keys %d, # vals %d", len(retKeys), len(retVals))
					}
					if len(retKeys) != len(kvMap) {
						t.Errorf("invalid kvs, # keys %d, should be %d", len(retKeys), len(kvMap))
					}
					for i, k := range retKeys {
						if kvMap[k] != retVals[i] {
							t.Errorf("invalid value for %s\n\tgot\n\t%s\n\twanted\n\t%s", k, retVals[i], kvMap[k])
						}
					}

					// Check the series_id logic
					var seriesID int
					err = db.QueryRow(context.Background(), "SELECT prom_api.series_id($1)", jsonOrig).Scan(&seriesID)
					if err != nil {
						t.Fatal(err)
					}

					var seriesIDKeyVal int
					err = db.QueryRow(context.Background(), "SELECT get_series_id_for_key_value_array($1, $2, $3)", metricName, keys, values).Scan(&seriesIDKeyVal)
					if err != nil {
						t.Fatal(err)
					}
					if seriesID != seriesIDKeyVal {
						t.Fatalf("Expected the series ids to be equal: %v != %v", seriesID, seriesIDKeyVal)
					}

					err = db.QueryRow(context.Background(), "SELECT jsonb(labels) FROM _prom_catalog.series WHERE id=$1",
						seriesID).Scan(&jsonRes)
					if err != nil {
						t.Fatal(err)
					}
					fingerprintRes = getFingerprintFromJSON(t, jsonRes)

					if labelSet.Fingerprint() != fingerprintRes {
						t.Fatalf("Json not equal: got\n%v\nexpected\n%v", string(jsonRes), string(jsonOrig))

					}

					err = db.QueryRow(context.Background(), "SELECT (key_value_array(labels)).* FROM _prom_catalog.series WHERE id=$1",
						seriesID).Scan(&retKeys, &retVals)
					if err != nil {
						t.Fatal(err)
					}
					if len(retKeys) != len(retVals) {
						t.Errorf("invalid kvs, # keys %d, # vals %d", len(retKeys), len(retVals))
					}
					if len(retKeys) != len(kvMap) {
						t.Errorf("invalid kvs, # keys %d, should be %d", len(retKeys), len(kvMap))
					}
					for i, k := range retKeys {
						if kvMap[k] != retVals[i] {
							t.Errorf("invalid value for %s\n\tgot\n\t%s\n\twanted\n\t%s", k, retVals[i], kvMap[k])
						}
					}
				}
			})
		})
	}
}

func getFingerprintFromJSON(t testing.TB, jsonRes []byte) model.Fingerprint {
	labelSetRes := make(model.LabelSet)
	err := json.Unmarshal(jsonRes, &labelSetRes)
	if err != nil {
		t.Fatal(err)
	}
	return labelSetRes.Fingerprint()
}

func TestExtensionFunctions(t *testing.T) {
	if !*useExtension || testing.Short() {
		t.Skip("skipping extension test; testing without extension")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {

		searchPath := ""
		// right now the schemas in this test are hardcoded, if we ever allow
		// user-defined schemas we will need to test those as well
		expected := `"$user", public, _prom_ext, prom_api, prom_metric, _prom_catalog`
		err := db.QueryRow(context.Background(), "SHOW search_path;").Scan(&searchPath)
		if err != nil {
			t.Fatal(err)
		}
		if searchPath != expected {
			t.Errorf("incorrect search path\nexpected\n\t%s\nfound\n\t%s", expected, searchPath)
		}

		functions := []string{
			"label_jsonb_each_text",
			"label_unnest",
			"label_find_key_equal",
			"label_find_key_not_equal",
			"label_find_key_regex",
			"label_find_key_not_regex",
		}
		extSchema := "_prom_ext"
		for _, fn := range functions {
			const query = "SELECT nspname FROM pg_proc LEFT JOIN pg_namespace ON pronamespace = pg_namespace.oid WHERE pg_proc.oid = $1::regproc;"
			schema := ""
			err := db.QueryRow(context.Background(), query, fn).Scan(&schema)
			if err != nil {
				t.Fatal(err)
			}
			if schema != extSchema {
				t.Errorf("function %s in wrong schema\nexpected\n\t%s\nfound\n\t%s", fn, extSchema, schema)
			}
		}

		operators := []string{
			"==(prom_api.label_key,prom_api.pattern)",
			"!==(prom_api.label_key,prom_api.pattern)",
			"==~(prom_api.label_key,prom_api.pattern)",
			"!=~(prom_api.label_key,prom_api.pattern)",
		}
		for _, opr := range operators {
			const query = "SELECT nspname FROM pg_operator LEFT JOIN pg_namespace ON oprnamespace = pg_namespace.oid WHERE pg_operator.oid = $1::regoperator;"
			schema := ""
			err := db.QueryRow(context.Background(), query, opr).Scan(&schema)
			if err != nil {
				t.Fatal(err)
			}
			if schema != extSchema {
				t.Errorf("function %s in wrong schema\nexpected\n\t%s\nfound\n\t%s", opr, extSchema, schema)
			}
		}
	})
}
