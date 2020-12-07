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
	"time"

	"github.com/jackc/pgx/v4/pgxpool"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/prometheus/common/model"
	"github.com/timescale/promscale/pkg/prompb"
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

	for tcIndexIter, cIter := range testCases {
		tcIndex := tcIndexIter
		c := cIter
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
					err = db.QueryRow(context.Background(), "SELECT * FROM _prom_catalog.get_or_create_label_array($1)", jsonOrig).Scan(&labelArray)
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
					err = db.QueryRow(context.Background(), "SELECT * FROM _prom_catalog.get_or_create_label_array($1, $2, $3)", metricName, keys, values).Scan(&labelArrayKV)
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
						t.Fatalf("Json not equal: got\n%v\nexpected\n%v", fmt.Sprint(fingerprintRes), string(jsonOrig))

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
					err = db.QueryRow(context.Background(), "SELECT _prom_catalog.get_or_create_series_id($1)", jsonOrig).Scan(&seriesID)
					if err != nil {
						t.Fatal(err)
					}

					var seriesIDKeyVal int
					err = db.QueryRow(context.Background(), "SELECT series_id FROM get_or_create_series_id_for_kv_array($1, $2, $3)", metricName, keys, values).Scan(&seriesIDKeyVal)
					if err != nil {
						t.Fatal(err)
					}
					if seriesID != seriesIDKeyVal {
						t.Fatalf("Expected the series ids to be equal: %v != %v", seriesID, seriesIDKeyVal)
					}
					_, err = db.Exec(context.Background(), "CALL _prom_catalog.finalize_metric_creation()")
					if err != nil {
						t.Fatal(err)
					}

					err = db.QueryRow(context.Background(), "SELECT jsonb(labels) FROM _prom_catalog.series WHERE id=$1",
						seriesID).Scan(&jsonRes)
					if err != nil {
						t.Fatal(err)
					}
					fingerprintRes = getFingerprintFromJSON(t, jsonRes)

					if labelSet.Fingerprint() != fingerprintRes {
						t.Fatalf("Json not equal: id %v\n got\n%v\nexpected\n%v", seriesID, string(jsonRes), string(jsonOrig))
					}

					err = db.QueryRow(context.Background(), "SELECT jsonb(labels($1))", seriesID).Scan(&jsonRes)
					if err != nil {
						t.Fatal(err)
					}
					fingerprintRes = getFingerprintFromJSON(t, jsonRes)

					if labelSet.Fingerprint() != fingerprintRes {
						t.Fatalf("Json not equal: id %v\n got\n%v\nexpected\n%v", seriesID, string(jsonRes), string(jsonOrig))
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

func TestExtensionGapfillDelta(t *testing.T) {
	if !*useExtension || testing.Short() {
		t.Skip("skipping extension test; testing without extension")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		_, err := db.Exec(context.Background(), "CREATE TABLE gfd_test_table(t TIMESTAMPTZ, v DOUBLE PRECISION);")
		if err != nil {
			t.Fatal(err)
		}

		if err != nil {
			t.Fatal(err)
		}
		times := []string{
			"2000-01-02 15:00:00 UTC",
			"2000-01-02 15:05:00 UTC",
			"2000-01-02 15:10:00 UTC",
			"2000-01-02 15:15:00 UTC",
			"2000-01-02 15:20:00 UTC",
			"2000-01-02 15:25:00 UTC",
			"2000-01-02 15:30:00 UTC",
			"2000-01-02 15:35:00 UTC",
			"2000-01-02 15:40:00 UTC",
			"2000-01-02 15:45:00 UTC",
		}
		vals := []float64{0, 50, 100, 150, 200, 200, 150, 100, 50, 0}
		_, err = db.Exec(context.Background(),
			"INSERT INTO gfd_test_table SELECT unnest($1::TEXT[])::TIMESTAMPTZ t, unnest($2::DOUBLE PRECISION[]) v;",
			times, vals)
		if err != nil {
			t.Fatal(err)
		}
		var res string
		err = db.QueryRow(context.Background(),
			"SELECT prom_delta('2000-01-02 15:00:00 UTC'::TIMESTAMPTZ, '2000-01-02 15:45:00 UTC'::TIMESTAMPTZ, 20 * 60 * 1000, 20 * 60 * 1000, NULL, v order by t)::TEXT FROM gfd_test_table;").Scan(&res)
		if err.Error() != "ERROR: NULL value for non-nullable argument \"time\" (SQLSTATE XX000)" {
			t.Error(err)
		}
		err = db.QueryRow(context.Background(),
			"SELECT prom_delta('2000-01-02 15:00:00 UTC'::TIMESTAMPTZ, '2000-01-02 15:45:00 UTC'::TIMESTAMPTZ, 20 * 60 * 1000, 20 * 60 * 1000, '2020-01-02 15:00:00 UTC'::TIMESTAMPTZ, v order by t)::TEXT FROM gfd_test_table;").Scan(&res)
		if err.Error() != "ERROR: input time less than lowest time (SQLSTATE XX000)" {
			t.Error(err)
		}
		err = db.QueryRow(context.Background(),
			"SELECT prom_delta('2000-01-02 15:00:00 UTC'::TIMESTAMPTZ, '2000-01-02 15:45:00 UTC'::TIMESTAMPTZ, 20 * 60 * 1000, 20 * 60 * 1000, t, v order by t)::TEXT FROM gfd_test_table;").Scan(&res)
		if err != nil {
			t.Fatal(err)
		}

		if res != "{200,-150}" {
			t.Errorf("wrong result. Expected\n\t{200,-150}\nfound\n\t%s\n", res)
		}

		err = db.QueryRow(context.Background(),
			"SELECT prom_delta('2000-01-02 14:15:00 UTC'::TIMESTAMPTZ, '2000-01-02 15:45:00 UTC'::TIMESTAMPTZ, 20 * 60 * 1000, 20 * 60 * 1000, t, v order by t)::TEXT FROM gfd_test_table;").Scan(&res)
		if err != nil {
			t.Fatal(err)
		}

		if res != "{NULL,NULL,200,-50}" {
			t.Errorf("wrong result. Expected\n\t{NULL,NULL,200,-50}\nfound\n\t%s\n", res)
		}
	})
}

func TestExtensionGapfillIncrease(t *testing.T) {
	if !*useExtension || testing.Short() {
		t.Skip("skipping extension test; testing without extension")
	}
	startTime, err := time.Parse(time.RFC3339, "2000-01-02T15:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	testCases := []struct {
		name   string
		times  []time.Time
		vals   []float64
		window int64
		result string
	}{
		{
			name: "basic foo 50m",
			times: func() []time.Time {
				times := make([]time.Time, 11)
				nextTime := startTime
				for i := range times {
					times[i] = nextTime
					nextTime = nextTime.Add(5 * time.Minute)
				}
				return times
			}(),
			vals: func() []float64 {
				vals := make([]float64, 11)
				nextVal := 0.0
				for i := range vals {
					vals[i] = nextVal
					nextVal += 10
				}
				return vals
			}(),
			window: 50 * 60 * 1000,
			result: "{100}",
		},
		{
			name: "basic bar 50m",
			times: func() []time.Time {
				times := make([]time.Time, 11)
				nextTime := startTime
				if err != nil {
					t.Fatal(err)
				}
				for i := range times {
					times[i] = nextTime
					nextTime = nextTime.Add(5 * time.Minute)
				}
				return times
			}(),
			vals: func() []float64 {
				vals := make([]float64, 11)
				nextVal := 0.0
				for i := 0; i <= 5; i++ {
					vals[i] = nextVal
					nextVal += 10
				}
				nextVal = 0.0
				for i := 6; i < 11; i++ {
					vals[i] = nextVal
					nextVal += 10
				}
				return vals
			}(),
			window: 50 * 60 * 1000,
			result: "{90}",
		},
		{
			name: "counter reset",
			times: func() []time.Time {
				times := make([]time.Time, 7)
				nextTime := startTime
				if err != nil {
					t.Fatal(err)
				}
				for i := range times {
					times[i] = nextTime
					nextTime = nextTime.Add(5 * time.Minute)
				}
				return times
			}(),
			vals:   []float64{0, 1, 2, 3, 2, 3, 4},
			window: 30 * 60 * 1000,
			result: "{7}",
		},
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, tb testing.TB) {
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {

				_, err := db.Exec(context.Background(), "CREATE TABLE gfi_test_table(t TIMESTAMPTZ, v DOUBLE PRECISION);")
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					_, err := db.Exec(context.Background(), "DROP TABLE gfi_test_table;")
					if err != nil {
						t.Fatal(err)
					}
				}()

				_, err = db.Exec(context.Background(),
					"INSERT INTO gfi_test_table SELECT unnest($1::TIMESTAMPTZ[]) t, unnest($2::DOUBLE PRECISION[]) v;",
					testCase.times, testCase.vals)
				if err != nil {
					t.Fatal(err)
				}
				var res string
				err = db.QueryRow(context.Background(),
					"SELECT prom_increase($1::TIMESTAMPTZ, $2::TIMESTAMPTZ, $3, $3, t, v order by t)::TEXT FROM gfi_test_table;",
					startTime, startTime.Add(time.Duration(testCase.window)*time.Millisecond), testCase.window,
				).Scan(&res)
				if err != nil {
					t.Fatal(err)
				}

				if res != testCase.result {
					t.Errorf("wrong result. Expected\n\t%s\nfound\n\t%s\n", testCase.result, res)
				}
			})
		}
	})
}
