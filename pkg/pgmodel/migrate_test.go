// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package pgmodel

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	database           = flag.String("database", "tmp_db_timescale_migrate_test", "database to run integration tests on")
	useDocker          = flag.Bool("use-docker", true, "start database using a docker container")
	pgHost             = "localhost"
	pgPort    nat.Port = "5432/tcp"
)

const (
	expectedVersion = 2
	defaultDB       = "postgres"
)

func TestMigrate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *database, func(db *pgxpool.Pool, t *testing.T) {
		var version int64
		var dirty bool
		err := db.QueryRow(context.Background(), "SELECT version, dirty FROM prom_schema_migrations").Scan(&version, &dirty)
		if err != nil {
			t.Fatal(err)
		}
		if version != expectedVersion {
			t.Errorf("Version unexpected:\ngot\n%d\nwanted\n%d", version, expectedVersion)
		}
		if dirty {
			t.Error("Dirty is true")
		}

	})
}

func testConcurrentMetricTable(t *testing.T, db *pgxpool.Pool, metricName string) int64 {
	var id *int64 = nil
	var name *string = nil
	err := db.QueryRow(context.Background(), "SELECT id, table_name FROM _prom_catalog.create_metric_table($1)", metricName).Scan(&id, &name)
	if err != nil {
		t.Fatal(err)
	}
	if id == nil || name == nil {
		t.Fatalf("NULL found")
	}
	return *id
}

func testConcurrentNewLabel(t *testing.T, db *pgxpool.Pool, labelName string) int64 {
	var id *int64 = nil
	err := db.QueryRow(context.Background(), "SELECT _prom_catalog.get_new_label_id($1, $1)", labelName).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	if id == nil {
		t.Fatalf("NULL found")
	}
	return *id
}

func testConcurrentCreateSeries(t *testing.T, db *pgxpool.Pool, index int) int64 {
	var id *int64 = nil
	err := db.QueryRow(context.Background(), "SELECT _prom_catalog.create_series($1, array[$1::int])", index).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	if id == nil {
		t.Fatalf("NULL found")
	}
	return *id
}

func TestConcurrentSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *database, func(db *pgxpool.Pool, t *testing.T) {
		for i := 0; i < 10; i++ {
			name := fmt.Sprintf("metric_%d", i)
			var id1, id2 int64
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				id1 = testConcurrentMetricTable(t, db, name)
			}()
			go func() {
				defer wg.Done()
				id2 = testConcurrentMetricTable(t, db, name)
			}()
			wg.Wait()

			if id1 != id2 {
				t.Fatalf("ids aren't equal: %d != %d", id1, id2)
			}

			wg.Add(2)
			go func() {
				defer wg.Done()
				id1 = testConcurrentNewLabel(t, db, name)
			}()
			go func() {
				defer wg.Done()
				id2 = testConcurrentNewLabel(t, db, name)
			}()
			wg.Wait()

			if id1 != id2 {
				t.Fatalf("ids aren't equal: %d != %d", id1, id2)
			}

			wg.Add(2)
			go func() {
				defer wg.Done()
				id1 = testConcurrentCreateSeries(t, db, i)
			}()
			go func() {
				defer wg.Done()
				id2 = testConcurrentCreateSeries(t, db, i)
			}()
			wg.Wait()

			if id1 != id2 {
				t.Fatalf("ids aren't equal: %d != %d", id1, id2)
			}
		}
	})
}

func TestPGConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	db, err := pgx.Connect(context.Background(), PGConnectURL(t, defaultDB))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(context.Background())
	var res int
	err = db.QueryRow(context.Background(), "SELECT 1").Scan(&res)
	if err != nil {
		t.Fatal(err)
	}
	if res != 1 {
		t.Errorf("Res is not 1 but %d", res)
	}
}

func TestSQLGetOrCreateMetricTableName(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *database, func(db *pgxpool.Pool, t *testing.T) {
		metricName := "test_metric_1"
		var metricID int
		var tableName string
		err := db.QueryRow(context.Background(), "SELECT * FROM prom.get_or_create_metric_table_name(metric_name => $1)", metricName).Scan(&metricID, &tableName)
		if err != nil {
			t.Fatal(err)
		}
		if metricName != tableName {
			t.Errorf("expected metric and table name to be the same: got %v wanted %v", metricName, tableName)
		}
		if metricID <= 0 {
			t.Errorf("metric_id should be >= 0:\ngot:%v", metricID)
		}
		savedMetricID := metricID

		//query for same name should give same result
		err = db.QueryRow(context.Background(), "SELECT * FROM prom.get_or_create_metric_table_name($1)", metricName).Scan(&metricID, &tableName)
		if err != nil {
			t.Fatal(err)
		}
		if metricName != tableName {
			t.Errorf("expected metric and table name to be the same unexpected:\ngot\n%v\nwanted\n%v", metricName, tableName)
		}
		if metricID != savedMetricID {
			t.Errorf("metric_id should be same:\nexpected:%v\ngot:%v", savedMetricID, metricID)
		}

		//different metric id should give new result
		metricName = "test_metric_2"
		err = db.QueryRow(context.Background(), "SELECT * FROM prom.get_or_create_metric_table_name($1)", metricName).Scan(&metricID, &tableName)
		if err != nil {
			t.Fatal(err)
		}
		if metricName != tableName {
			t.Errorf("expected metric and table name to be the same unexpected:\ngot\n%v\nwanted\n%v", metricName, tableName)
		}
		if metricID == savedMetricID {
			t.Errorf("metric_id should not be same:\nexpected: != %v\ngot:%v", savedMetricID, metricID)
		}
		savedMetricID = metricID

		//test long names that don't fit as table names
		metricName = "test_metric_very_very_long_name_have_to_truncate_it_longer_than_64_chars_1"
		err = db.QueryRow(context.Background(), "SELECT * FROM prom.get_or_create_metric_table_name($1)", metricName).Scan(&metricID, &tableName)
		if err != nil {
			t.Fatal(err)
		}
		if metricName == tableName {
			t.Errorf("expected metric and table name to not be the same unexpected:\ngot\n%v", tableName)
		}
		if metricID == savedMetricID {
			t.Errorf("metric_id should not be same:\nexpected: != %v\ngot:%v", savedMetricID, metricID)
		}
		savedTableName := tableName
		savedMetricID = metricID

		//another call return same info
		err = db.QueryRow(context.Background(), "SELECT * FROM prom.get_or_create_metric_table_name($1)", metricName).Scan(&metricID, &tableName)
		if err != nil {
			t.Fatal(err)
		}
		if savedTableName != tableName {
			t.Errorf("expected table name to be the same:\ngot\n%v\nexpected\n%v", tableName, savedTableName)
		}
		if metricID != savedMetricID {
			t.Errorf("metric_id should be same:\nexpected:%v\ngot:%v", savedMetricID, metricID)
		}

		//changing just ending returns new table
		metricName = "test_metric_very_very_long_name_have_to_truncate_it_longer_than_64_chars_2"
		err = db.QueryRow(context.Background(), "SELECT * FROM prom.get_or_create_metric_table_name($1)", metricName).Scan(&metricID, &tableName)
		if err != nil {
			t.Fatal(err)
		}
		if savedTableName == tableName {
			t.Errorf("expected table name to not be the same:\ngot\n%v\nnot =\n%v", tableName, savedTableName)
		}
		if metricID == savedMetricID {
			t.Errorf("metric_id should not be same:\nexpected:%v\ngot:%v", savedMetricID, metricID)
		}
	})
}

func verifyChunkInterval(t *testing.T, db *pgxpool.Pool, tableName string, expectedDuration time.Duration) {
	var intervalLength int64

	err := db.QueryRow(context.Background(),
		`SELECT d.interval_length
	 FROM _timescaledb_catalog.hypertable h
	 INNER JOIN LATERAL
	 (SELECT dim.interval_length FROM _timescaledb_catalog.dimension dim WHERE dim.hypertable_id = h.id ORDER BY dim.id LIMIT 1) d
	    ON (true)
	 WHERE table_name = $1`,
		tableName).Scan(&intervalLength)
	if err != nil {
		t.Error(err)
	}

	dur := time.Duration(time.Duration(intervalLength) * time.Microsecond)
	if dur.Round(time.Hour) != expectedDuration {
		t.Errorf("Unexpected chunk interval for table %v: got %v want %v", tableName, dur, expectedDuration)
	}
}

func TestSQLChunkInterval(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *database, func(db *pgxpool.Pool, t *testing.T) {
		ts := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: metricNameLabelName, Value: "test"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.1},
					{Timestamp: 2, Value: 0.2},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: metricNameLabelName, Value: "test2"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.1},
					{Timestamp: 2, Value: 0.2},
				},
			},
		}
		ingestor := NewPgxIngestor(db)
		defer ingestor.Close()
		_, err := ingestor.Ingest(ts)
		if err != nil {
			t.Fatal(err)
		}
		verifyChunkInterval(t, db, "test", time.Duration(8*time.Hour))
		_, err = db.Exec(context.Background(), "SELECT prom.set_metric_chunk_interval('test2', INTERVAL '7 hours')")
		if err != nil {
			t.Error(err)
		}
		verifyChunkInterval(t, db, "test2", time.Duration(7*time.Hour))
		_, err = db.Exec(context.Background(), "SELECT prom.set_default_chunk_interval(INTERVAL '6 hours')")
		if err != nil {
			t.Error(err)
		}
		verifyChunkInterval(t, db, "test", time.Duration(6*time.Hour))
		verifyChunkInterval(t, db, "test2", time.Duration(7*time.Hour))
		_, err = db.Exec(context.Background(), "SELECT prom.reset_metric_chunk_interval('test2')")
		if err != nil {
			t.Error(err)
		}
		verifyChunkInterval(t, db, "test2", time.Duration(6*time.Hour))

		//set on a metric that doesn't exist should create the metric and set the parameter
		_, err = db.Exec(context.Background(), "SELECT prom.set_metric_chunk_interval('test_new_metric1', INTERVAL '7 hours')")
		if err != nil {
			t.Error(err)
		}
		verifyChunkInterval(t, db, "test_new_metric1", time.Duration(7*time.Hour))

		_, err = db.Exec(context.Background(), "SELECT prom.set_default_chunk_interval(INTERVAL '2 hours')")
		if err != nil {
			t.Error(err)
		}

		verifyChunkInterval(t, db, "test_new_metric1", time.Duration(7*time.Hour))

	})
}

func verifyRetentionPeriod(t *testing.T, db *pgxpool.Pool, metricName string, expectedDuration time.Duration) {
	var dur time.Duration

	err := db.QueryRow(context.Background(),
		`SELECT prom.get_metric_retention_period($1)`,
		metricName).Scan(&dur)
	if err != nil {
		t.Error(err)
	}

	if dur != expectedDuration {
		t.Fatalf("Unexpected retention period for table %v: got %v want %v", metricName, dur, expectedDuration)
	}
}

func TestSQLRetentionPeriod(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *database, func(db *pgxpool.Pool, t *testing.T) {
		ts := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: metricNameLabelName, Value: "test"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.1},
					{Timestamp: 2, Value: 0.2},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: metricNameLabelName, Value: "test2"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.1},
					{Timestamp: 2, Value: 0.2},
				},
			},
		}
		ingestor := NewPgxIngestor(db)
		defer ingestor.Close()
		_, err := ingestor.Ingest(ts)
		if err != nil {
			t.Fatal(err)
		}
		verifyRetentionPeriod(t, db, "test", time.Duration(90*24*time.Hour))
		_, err = db.Exec(context.Background(), "SELECT prom.set_metric_retention_period('test2', INTERVAL '7 hours')")
		if err != nil {
			t.Error(err)
		}

		verifyRetentionPeriod(t, db, "test", time.Duration(90*24*time.Hour))
		verifyRetentionPeriod(t, db, "test2", time.Duration(7*time.Hour))
		_, err = db.Exec(context.Background(), "SELECT prom.set_default_retention_period(INTERVAL '6 hours')")
		if err != nil {
			t.Error(err)
		}
		verifyRetentionPeriod(t, db, "test", time.Duration(6*time.Hour))
		verifyRetentionPeriod(t, db, "test2", time.Duration(7*time.Hour))
		_, err = db.Exec(context.Background(), "SELECT prom.reset_metric_retention_period('test2')")
		if err != nil {
			t.Error(err)
		}
		verifyRetentionPeriod(t, db, "test2", time.Duration(6*time.Hour))

		//set on a metric that doesn't exist should create the metric and set the parameter
		_, err = db.Exec(context.Background(), "SELECT prom.set_metric_retention_period('test_new_metric1', INTERVAL '7 hours')")
		if err != nil {
			t.Error(err)
		}
		verifyRetentionPeriod(t, db, "test_new_metric1", time.Duration(7*time.Hour))

		_, err = db.Exec(context.Background(), "SELECT prom.set_default_retention_period(INTERVAL '2 hours')")
		if err != nil {
			t.Error(err)
		}

		verifyRetentionPeriod(t, db, "test_new_metric1", time.Duration(7*time.Hour))

		//get on non-existing metric returns default
		verifyRetentionPeriod(t, db, "test_new_metric2", time.Duration(2*time.Hour))
	})
}

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
		databaseName := fmt.Sprintf("%s_%d", *database, tcIndex)
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			withDB(t, databaseName, func(db *pgxpool.Pool, t *testing.T) {
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
					err = db.QueryRow(context.Background(), "SELECT * FROM prom.jsonb_to_label_array($1)", jsonOrig).Scan(&labelArray)
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
					err = db.QueryRow(context.Background(), "SELECT * FROM prom.key_value_array_to_label_array($1, $2, $3)", metricName, keys, values).Scan(&labelArrayKV)
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

					var jsonres []byte
					err = db.QueryRow(context.Background(), "SELECT * FROM prom.label_array_to_jsonb($1)", labelArray).Scan(&jsonres)
					labelSetRes := make(model.LabelSet, len(ts.Labels))
					err = json.Unmarshal(jsonres, &labelSetRes)
					if err != nil {
						t.Fatal(err)
					}
					if labelSet.Fingerprint() != labelSetRes.Fingerprint() {
						t.Fatalf("Json not equal: got\n%v\nexpected\n%v", string(jsonres), string(jsonOrig))

					}

					var (
						retKeys []string
						retVals []string
					)
					err = db.QueryRow(context.Background(), "SELECT * FROM prom.label_array_to_key_value_array($1)", labelArray).Scan(&retKeys, &retVals)
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
					err = db.QueryRow(context.Background(), "SELECT prom.get_series_id_for_label($1)", jsonOrig).Scan(&seriesID)
					if err != nil {
						t.Fatal(err)
					}

					var seriesIDKeyVal int
					err = db.QueryRow(context.Background(), "SELECT prom.get_series_id_for_key_value_array($1, $2, $3)", metricName, keys, values).Scan(&seriesIDKeyVal)
					if err != nil {
						t.Fatal(err)
					}
					if seriesID != seriesIDKeyVal {
						t.Fatalf("Expected the series ids to be equal: %v != %v", seriesID, seriesIDKeyVal)
					}

					err = db.QueryRow(context.Background(), "SELECT prom.label_array_to_jsonb(labels) FROM _prom_catalog.series WHERE id=$1",
						seriesID).Scan(&jsonres)
					labelSetRes = make(model.LabelSet, len(ts.Labels))
					err = json.Unmarshal(jsonres, &labelSetRes)
					if err != nil {
						t.Fatal(err)
					}

					if labelSet.Fingerprint() != labelSetRes.Fingerprint() {
						t.Fatalf("Json not equal: got\n%v\nexpected\n%v", string(jsonres), string(jsonOrig))

					}

					err = db.QueryRow(context.Background(), "SELECT (prom.label_array_to_key_value_array(labels)).* FROM _prom_catalog.series WHERE id=$1",
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

func TestSQLIngest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	testCases := []struct {
		name        string
		metrics     []prompb.TimeSeries
		count       uint64
		countSeries int
		expectErr   error
	}{
		{
			name:    "Zero metrics",
			metrics: []prompb.TimeSeries{},
		},
		{
			name: "One metric",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			count:       1,
			countSeries: 1,
		},
		{
			name: "One metric, no sample",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
				},
			},
		},
		{
			name: "Two timeseries",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "foo", Value: "bar"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			count:       2,
			countSeries: 2,
		},
		{
			name: "Two samples",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
						{Timestamp: 2, Value: 0.2},
					},
				},
			},
			count:       2,
			countSeries: 1,
		},
		{
			name: "Two samples that are complete duplicates",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			count:       2,
			countSeries: 1,
		},
		{
			name: "Two timeseries, one series",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 2, Value: 0.2},
					},
				},
			},
			count:       2,
			countSeries: 1,
		},
		{
			name: "Two metric names , one series",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test1"},
						{Name: "commonkey", Value: "test"},
						{Name: "key1", Value: "test"},
						{Name: "key2", Value: "val1"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test2"},
						{Name: "commonkey", Value: "test"},
						{Name: "key1", Value: "val2"},
						{Name: "key3", Value: "val3"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 2, Value: 0.2},
					},
				},
			},
			count:       2,
			countSeries: 2,
		},
		{
			name: "Missing metric name",
			metrics: []prompb.TimeSeries{
				{
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			count:       0,
			countSeries: 0,
			expectErr:   errNoMetricName,
		},
	}
	for tcIndex, c := range testCases {
		databaseName := fmt.Sprintf("%s_%d", *database, tcIndex)
		tcase := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			withDB(t, databaseName, func(db *pgxpool.Pool, t *testing.T) {
				ingestor := NewPgxIngestor(db)
				defer ingestor.Close()
				cnt, err := ingestor.Ingest(tcase.metrics)
				if cnt != tcase.count {
					t.Fatalf("counts not equal: got %v expected %v\n", cnt, tcase.count)
				}

				if err != nil && err != tcase.expectErr {
					t.Fatalf("got an unexpected error %v", err)
				}

				if err != nil {
					return
				}

				tables := make(map[string]bool)
				for _, ts := range tcase.metrics {
					for _, l := range ts.Labels {
						if len(ts.Samples) > 0 && l.Name == metricNameLabelName {
							tables[l.Value] = true
						}
					}
				}
				totalRows := 0
				for table := range tables {
					var rowsInTable int
					err := db.QueryRow(context.Background(), fmt.Sprintf("SELECT count(*) FROM prom.%s", table)).Scan(&rowsInTable)
					if err != nil {
						t.Fatal(err)
					}
					totalRows += rowsInTable
				}

				if totalRows != int(cnt) {
					t.Fatalf("counts not equal: got %v expected %v\n", totalRows, cnt)
				}

				var numberSeries int
				err = db.QueryRow(context.Background(), "SELECT count(*) FROM _prom_catalog.series").Scan(&numberSeries)
				if err != nil {
					t.Fatal(err)
				}
				if numberSeries != tcase.countSeries {
					t.Fatalf("unexpected number of series: got %v expected %v\n", numberSeries, tcase.countSeries)
				}
			})
		})
	}
}

func TestSQLQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	testCases := []struct {
		name           string
		readRequest    prompb.ReadRequest
		expectResponse prompb.ReadResponse
		expectErr      error
	}{
		{
			name:        "empty request",
			readRequest: prompb.ReadRequest{},
			expectResponse: prompb.ReadResponse{
				Results: make([]*prompb.QueryResult, 0),
			},
		},
		{
			name: "empty response",
			readRequest: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  metricNameLabelName,
								Value: "nonExistantMetric",
							},
						},
						StartTimestampMs: 1,
						EndTimestampMs:   3,
					},
				},
			},
			expectResponse: prompb.ReadResponse{
				Results: createQueryResult([]*prompb.TimeSeries{}),
			},
		},
		{
			name: "one matcher, exact metric",
			readRequest: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  metricNameLabelName,
								Value: "firstMetric",
							},
						},
						StartTimestampMs: 1,
						EndTimestampMs:   3,
					},
				},
			},
			expectResponse: prompb.ReadResponse{
				Results: createQueryResult([]*prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: metricNameLabelName, Value: "firstMetric"},
							{Name: "common", Value: "tag"},
							{Name: "empty", Value: ""},
							{Name: "foo", Value: "bar"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 1, Value: 0.1},
							{Timestamp: 2, Value: 0.2},
							{Timestamp: 3, Value: 0.3},
						},
					},
				}),
			},
		},
		{
			name: "one matcher, regex metric",
			readRequest: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_RE,
								Name:  metricNameLabelName,
								Value: "first.*",
							},
						},
						StartTimestampMs: 1,
						EndTimestampMs:   3,
					},
				},
			},
			expectResponse: prompb.ReadResponse{
				Results: createQueryResult([]*prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: metricNameLabelName, Value: "firstMetric"},
							{Name: "common", Value: "tag"},
							{Name: "empty", Value: ""},
							{Name: "foo", Value: "bar"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 1, Value: 0.1},
							{Timestamp: 2, Value: 0.2},
							{Timestamp: 3, Value: 0.3},
						},
					},
				}),
			},
		},
		{
			name: "one matcher, no exact metric",
			readRequest: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_NEQ,
								Name:  metricNameLabelName,
								Value: "firstMetric",
							},
						},
						StartTimestampMs: 1,
						EndTimestampMs:   1,
					},
				},
			},
			expectResponse: prompb.ReadResponse{
				Results: createQueryResult([]*prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: metricNameLabelName, Value: "secondMetric"},
							{Name: "common", Value: "tag"},
							{Name: "foo", Value: "baz"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 1, Value: 1.1},
						},
					},
				}),
			},
		},
		{
			name: "one matcher, not regex metric",
			readRequest: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_NRE,
								Name:  metricNameLabelName,
								Value: "first.*",
							},
						},
						StartTimestampMs: 1,
						EndTimestampMs:   2,
					},
				},
			},
			expectResponse: prompb.ReadResponse{
				Results: createQueryResult([]*prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: metricNameLabelName, Value: "secondMetric"},
							{Name: "common", Value: "tag"},
							{Name: "foo", Value: "baz"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 1, Value: 1.1},
							{Timestamp: 2, Value: 1.2},
						},
					},
				}),
			},
		},
		{
			name: "one matcher, both metrics",
			readRequest: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  "common",
								Value: "tag",
							},
						},
						StartTimestampMs: 2,
						EndTimestampMs:   3,
					},
				},
			},
			expectResponse: prompb.ReadResponse{
				Results: createQueryResult([]*prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: metricNameLabelName, Value: "firstMetric"},
							{Name: "common", Value: "tag"},
							{Name: "empty", Value: ""},
							{Name: "foo", Value: "bar"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 2, Value: 0.2},
							{Timestamp: 3, Value: 0.3},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: metricNameLabelName, Value: "secondMetric"},
							{Name: "common", Value: "tag"},
							{Name: "foo", Value: "baz"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 2, Value: 1.2},
							{Timestamp: 3, Value: 1.3},
						},
					},
				}),
			},
		},
		{
			name: "two matchers",
			readRequest: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  "common",
								Value: "tag",
							},
							{
								Type:  prompb.LabelMatcher_RE,
								Name:  metricNameLabelName,
								Value: ".*Metric",
							},
						},
						StartTimestampMs: 2,
						EndTimestampMs:   3,
					},
				},
			},
			expectResponse: prompb.ReadResponse{
				Results: createQueryResult([]*prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: metricNameLabelName, Value: "firstMetric"},
							{Name: "common", Value: "tag"},
							{Name: "empty", Value: ""},
							{Name: "foo", Value: "bar"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 2, Value: 0.2},
							{Timestamp: 3, Value: 0.3},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: metricNameLabelName, Value: "secondMetric"},
							{Name: "common", Value: "tag"},
							{Name: "foo", Value: "baz"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 2, Value: 1.2},
							{Timestamp: 3, Value: 1.3},
						},
					},
				}),
			},
		},
		{
			name: "three matchers",
			readRequest: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  "common",
								Value: "tag",
							},
							{
								Type:  prompb.LabelMatcher_RE,
								Name:  "foo",
								Value: "bar|baz",
							},
							{
								Type:  prompb.LabelMatcher_NRE,
								Name:  metricNameLabelName,
								Value: "non-existant",
							},
						},
						StartTimestampMs: 2,
						EndTimestampMs:   3,
					},
				},
			},
			expectResponse: prompb.ReadResponse{
				Results: createQueryResult([]*prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: metricNameLabelName, Value: "firstMetric"},
							{Name: "common", Value: "tag"},
							{Name: "empty", Value: ""},
							{Name: "foo", Value: "bar"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 2, Value: 0.2},
							{Timestamp: 3, Value: 0.3},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: metricNameLabelName, Value: "secondMetric"},
							{Name: "common", Value: "tag"},
							{Name: "foo", Value: "baz"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 2, Value: 1.2},
							{Timestamp: 3, Value: 1.3},
						},
					},
				}),
			},
		},
		{
			name: "one matcher, match empty value",
			readRequest: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  "empty",
								Value: "",
							},
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  "common",
								Value: "tag",
							},
						},
						StartTimestampMs: 1,
						EndTimestampMs:   3,
					},
				},
			},
			expectResponse: prompb.ReadResponse{
				Results: createQueryResult([]*prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: metricNameLabelName, Value: "firstMetric"},
							{Name: "common", Value: "tag"},
							{Name: "empty", Value: ""},
							{Name: "foo", Value: "bar"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 1, Value: 0.1},
							{Timestamp: 2, Value: 0.2},
							{Timestamp: 3, Value: 0.3},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: metricNameLabelName, Value: "secondMetric"},
							{Name: "common", Value: "tag"},
							{Name: "foo", Value: "baz"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 1, Value: 1.1},
							{Timestamp: 2, Value: 1.2},
							{Timestamp: 3, Value: 1.3},
						},
					},
				}),
			},
		},
		{
			name: "one matcher, regex match empty value",
			readRequest: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_RE,
								Name:  "empty",
								Value: ".*",
							},
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  "common",
								Value: "tag",
							},
						},
						StartTimestampMs: 1,
						EndTimestampMs:   3,
					},
				},
			},
			expectResponse: prompb.ReadResponse{
				Results: createQueryResult([]*prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: metricNameLabelName, Value: "firstMetric"},
							{Name: "common", Value: "tag"},
							{Name: "empty", Value: ""},
							{Name: "foo", Value: "bar"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 1, Value: 0.1},
							{Timestamp: 2, Value: 0.2},
							{Timestamp: 3, Value: 0.3},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: metricNameLabelName, Value: "secondMetric"},
							{Name: "common", Value: "tag"},
							{Name: "foo", Value: "baz"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 1, Value: 1.1},
							{Timestamp: 2, Value: 1.2},
							{Timestamp: 3, Value: 1.3},
						},
					},
				}),
			},
		},
	}

	withDB(t, *database, func(db *pgxpool.Pool, t *testing.T) {
		// Ingest test dataset.
		ingestQueryTestDataset(db, t)

		r := NewPgxReader(db)
		for _, c := range testCases {
			t.Run(c.name, func(t *testing.T) {
				resp, err := r.Read(&c.readRequest)

				if err != nil && err != c.expectErr {
					t.Fatalf("unexpected error returned:\ngot\n%s\nwanted\n%s", err, c.expectErr)
				}

				if !reflect.DeepEqual(resp, &c.expectResponse) {
					t.Fatalf("unexpected response:\ngot\n%+v\nwanted\n%+v", resp, &c.expectResponse)
				}

			})
		}
	})
}

func createQueryResult(ts []*prompb.TimeSeries) []*prompb.QueryResult {
	return []*prompb.QueryResult{
		{
			Timeseries: ts,
		},
	}
}

func ingestQueryTestDataset(db *pgxpool.Pool, t *testing.T) {
	metrics := []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: metricNameLabelName, Value: "firstMetric"},
				{Name: "foo", Value: "bar"},
				{Name: "common", Value: "tag"},
				{Name: "empty", Value: ""},
			},
			Samples: []prompb.Sample{
				{Timestamp: 1, Value: 0.1},
				{Timestamp: 2, Value: 0.2},
				{Timestamp: 3, Value: 0.3},
				{Timestamp: 4, Value: 0.4},
				{Timestamp: 5, Value: 0.5},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: metricNameLabelName, Value: "secondMetric"},
				{Name: "foo", Value: "baz"},
				{Name: "common", Value: "tag"},
			},
			Samples: []prompb.Sample{
				{Timestamp: 1, Value: 1.1},
				{Timestamp: 2, Value: 1.2},
				{Timestamp: 3, Value: 1.3},
				{Timestamp: 4, Value: 1.4},
				{Timestamp: 5, Value: 1.5},
			},
		},
	}
	ingestor := NewPgxIngestor(db)
	defer ingestor.Close()
	cnt, err := ingestor.Ingest(metrics)

	if err != nil {
		t.Fatalf("unexpected error while ingesting test dataset: %s", err)
	}

	expectedCount := 0

	for _, ts := range metrics {
		expectedCount = expectedCount + len(ts.Samples)
	}

	if cnt != uint64(expectedCount) {
		t.Fatalf("wrong amount of metrics ingested: got %d, wanted %d", cnt, expectedCount)
	}
}

func TestSQLDropMetricChunk(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *database, func(db *pgxpool.Pool, t *testing.T) {
		//this is the range_end of a chunk boundary (exclusive)
		chunkEnds := time.Date(2009, time.November, 11, 0, 0, 0, 0, time.UTC)

		ts := []prompb.TimeSeries{
			{
				//this series will be deleted along with it's label
				Labels: []prompb.Label{
					{Name: metricNameLabelName, Value: "test"},
					{Name: "name1", Value: "value1"},
				},
				Samples: []prompb.Sample{
					//this will be dropped (notice the - 1)
					{Timestamp: int64(model.TimeFromUnixNano(chunkEnds.UnixNano()) - 1), Value: 0.1},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: metricNameLabelName, Value: "test"},
					{Name: "name1", Value: "value2"},
				},
				Samples: []prompb.Sample{
					//this will remain after the drop
					{Timestamp: int64(model.TimeFromUnixNano(chunkEnds.UnixNano())), Value: 0.2},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: metricNameLabelName, Value: "test"},
					{Name: "name1", Value: "value3"},
				},
				Samples: []prompb.Sample{
					//this will be dropped (notice the - 1)
					{Timestamp: int64(model.TimeFromUnixNano(chunkEnds.UnixNano()) - 1), Value: 0.1},
					//this will not be dropped and is more than an hour newer
					{Timestamp: int64(model.TimeFromUnixNano(chunkEnds.Add(time.Hour * 5).UnixNano())), Value: 0.1},
				},
			},
		}
		ingestor := NewPgxIngestor(db)
		defer ingestor.Close()
		_, err := ingestor.Ingest(ts)
		if err != nil {
			t.Error(err)
		}

		wasDropped := false
		err = db.QueryRow(context.Background(), "SELECT _prom_catalog.drop_metric_chunks($1, $2)", "test", chunkEnds.Add(time.Second*5)).Scan(&wasDropped)
		if err != nil {
			t.Fatal(err)
		}
		if !wasDropped {
			t.Errorf("Expected chunk to be dropped")
		}

		count := 0
		err = db.QueryRow(context.Background(), `SELECT count(*) FROM prom.test`).Scan(&count)
		if err != nil {
			t.Error(err)
		}

		if count != 2 {
			t.Errorf("unexpected row count: %v", count)
		}

		err = db.QueryRow(context.Background(), `SELECT count(*) FROM _prom_catalog.series`).Scan(&count)
		if err != nil {
			t.Error(err)
		}

		if count != 2 {
			t.Errorf("unexpected series count: %v", count)
		}

		err = db.QueryRow(context.Background(), `SELECT count(*) FROM _prom_catalog.label where key='name1'`).Scan(&count)
		if err != nil {
			t.Error(err)
		}

		if count != 2 {
			t.Errorf("unexpected labels count: %v", count)
		}

		//rerun again -- nothing dropped
		err = db.QueryRow(context.Background(), "SELECT _prom_catalog.drop_metric_chunks($1, $2)", "test", chunkEnds.Add(time.Second*5)).Scan(&wasDropped)
		if err != nil {
			t.Fatal(err)
		}
		if wasDropped {
			t.Errorf("Expected chunk to not be dropped")
		}

	})
}

func TestSQLDropChunk(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *database, func(db *pgxpool.Pool, t *testing.T) {
		//a chunk way back in 2009
		chunkEnds := time.Date(2009, time.November, 11, 0, 0, 0, 0, time.UTC)

		ts := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: metricNameLabelName, Value: "test"},
					{Name: "name1", Value: "value1"},
				},
				Samples: []prompb.Sample{
					{Timestamp: int64(model.TimeFromUnixNano(chunkEnds.UnixNano()) - 1), Value: 0.1},
					{Timestamp: int64(model.TimeFromUnixNano(time.Now().UnixNano()) - 1), Value: 0.1},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: metricNameLabelName, Value: "test2"},
					{Name: "name1", Value: "value1"},
				},
				Samples: []prompb.Sample{
					{Timestamp: int64(model.TimeFromUnixNano(time.Now().UnixNano()) - 1), Value: 0.1},
				},
			},
		}
		ingestor := NewPgxIngestor(db)
		defer ingestor.Close()
		_, err := ingestor.Ingest(ts)
		if err != nil {
			t.Error(err)
		}

		cnt := 0
		err = db.QueryRow(context.Background(), "SELECT count(*) FROM show_chunks('prom.test')").Scan(&cnt)
		if err != nil {
			t.Fatal(err)
		}
		if cnt != 2 {
			t.Errorf("Expected there to be a chunk")
		}

		_, err = db.Exec(context.Background(), "CALL prom.drop_chunks()")
		if err != nil {
			t.Fatal(err)
		}
		err = db.QueryRow(context.Background(), "SELECT count(*) FROM show_chunks('prom.test')").Scan(&cnt)
		if err != nil {
			t.Fatal(err)
		}
		if cnt != 1 {
			t.Errorf("Expected the chunk to be dropped")
		}
		//noop works fine
		_, err = db.Exec(context.Background(), "CALL prom.drop_chunks()")
		if err != nil {
			t.Fatal(err)
		}
		//test2 isn't affected
		err = db.QueryRow(context.Background(), "SELECT count(*) FROM show_chunks('prom.test2')").Scan(&cnt)
		if err != nil {
			t.Fatal(err)
		}
		if cnt != 1 {
			t.Errorf("Expected the chunk to be dropped")
		}
	})
}

func TestMain(m *testing.M) {
	flag.Parse()
	ctx := context.Background()
	if !testing.Short() && *useDocker {
		container, err := startContainer(ctx)
		if err != nil {
			fmt.Println("Error setting up container", err)
			os.Exit(1)
		}
		defer func() {
			err := container.Terminate(ctx)
			if err != nil {
				panic(err)
			}
		}()
	}
	code := m.Run()
	os.Exit(code)
}

func PGConnectURL(t *testing.T, dbName string) string {
	template := "postgres://postgres:password@%s:%d/%s"
	return fmt.Sprintf(template, pgHost, pgPort.Int(), dbName)
}

func startContainer(ctx context.Context) (testcontainers.Container, error) {
	containerPort := nat.Port("5432/tcp")
	req := testcontainers.ContainerRequest{
		Image:        "timescale/timescaledb:latest-pg11",
		ExposedPorts: []string{string(containerPort)},
		WaitingFor:   wait.NewHostPortStrategy(containerPort),
		Env: map[string]string{
			"POSTGRES_PASSWORD": "password",
		},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	pgHost, err = container.Host(ctx)
	if err != nil {
		return nil, err
	}

	pgPort, err = container.MappedPort(ctx, containerPort)
	if err != nil {
		return nil, err
	}

	return container, nil
}

func withDB(t *testing.T, DBName string, f func(db *pgxpool.Pool, t *testing.T)) {
	db := dbSetup(t, DBName)
	defer func() {
		db.Close()
	}()
	performMigrate(t, DBName)
	f(db, t)
}

func performMigrate(t *testing.T, DBName string) {
	dbStd, err := sql.Open("pgx", PGConnectURL(t, DBName))
	defer func() {
		err := dbStd.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	if err != nil {
		t.Fatal(err)
	}
	err = Migrate(dbStd)
	if err != nil {
		t.Fatal(err)
	}
}

func dbSetup(t *testing.T, DBName string) *pgxpool.Pool {
	if len(*database) == 0 {
		t.Skip()
	}
	db, err := pgx.Connect(context.Background(), PGConnectURL(t, defaultDB))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", DBName))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE %s", DBName))
	if err != nil {
		t.Fatal(err)
	}
	err = db.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	dbPool, err := pgxpool.Connect(context.Background(), PGConnectURL(t, DBName))
	if err != nil {
		t.Fatal(err)
	}
	return dbPool
}
