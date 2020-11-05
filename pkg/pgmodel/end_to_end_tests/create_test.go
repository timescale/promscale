// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package end_to_end_tests

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	pgx "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/timescale/promscale/pkg/prompb"

	_ "github.com/jackc/pgx/v4/stdlib"

	. "github.com/timescale/promscale/pkg/pgmodel"
)

func TestSQLGetOrCreateMetricTableName(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		metricName := "test_metric_1"
		var metricID int
		var tableName string
		var possiblyNew bool
		err := db.QueryRow(context.Background(), "SELECT * FROM _prom_catalog.get_or_create_metric_table_name(metric_name => $1)", metricName).Scan(&metricID, &tableName, &possiblyNew)
		if err != nil {
			t.Fatal(err)
		}
		if metricName != tableName {
			t.Errorf("expected metric and table name to be the same: got %v wanted %v", metricName, tableName)
		}
		if metricID <= 0 {
			t.Errorf("metric_id should be >= 0:\ngot:%v", metricID)
		}
		if !possiblyNew {
			t.Errorf("unexpected value for possiblyNew %v", possiblyNew)
		}
		savedMetricID := metricID

		//query for same name should give same result
		err = db.QueryRow(context.Background(), "SELECT * FROM _prom_catalog.get_or_create_metric_table_name($1)", metricName).Scan(&metricID, &tableName, &possiblyNew)
		if err != nil {
			t.Fatal(err)
		}
		if metricName != tableName {
			t.Errorf("expected metric and table name to be the same unexpected:\ngot\n%v\nwanted\n%v", metricName, tableName)
		}
		if metricID != savedMetricID {
			t.Errorf("metric_id should be same:\nexpected:%v\ngot:%v", savedMetricID, metricID)
		}
		if possiblyNew {
			t.Errorf("unexpected value for possiblyNew %v", possiblyNew)
		}

		//different metric id should give new result
		metricName = "test_metric_2"
		err = db.QueryRow(context.Background(), "SELECT * FROM _prom_catalog.get_or_create_metric_table_name($1)", metricName).Scan(&metricID, &tableName, &possiblyNew)
		if err != nil {
			t.Fatal(err)
		}
		if metricName != tableName {
			t.Errorf("expected metric and table name to be the same unexpected:\ngot\n%v\nwanted\n%v", metricName, tableName)
		}
		if metricID == savedMetricID {
			t.Errorf("metric_id should not be same:\nexpected: != %v\ngot:%v", savedMetricID, metricID)
		}
		if !possiblyNew {
			t.Errorf("unexpected value for possiblyNew %v", possiblyNew)
		}
		savedMetricID = metricID

		//test long names that don't fit as table names
		metricName = "test_metric_very_very_long_name_have_to_truncate_it_longer_than_64_chars_1"
		err = db.QueryRow(context.Background(), "SELECT * FROM _prom_catalog.get_or_create_metric_table_name($1)", metricName).Scan(&metricID, &tableName, &possiblyNew)
		if err != nil {
			t.Fatal(err)
		}
		if metricName == tableName {
			t.Errorf("expected metric and table name to not be the same unexpected:\ngot\n%v", tableName)
		}
		if metricID == savedMetricID {
			t.Errorf("metric_id should not be same:\nexpected: != %v\ngot:%v", savedMetricID, metricID)
		}
		if !possiblyNew {
			t.Errorf("unexpected value for possiblyNew %v", possiblyNew)
		}
		savedTableName := tableName
		savedMetricID = metricID

		//another call return same info
		err = db.QueryRow(context.Background(), "SELECT * FROM _prom_catalog.get_or_create_metric_table_name($1)", metricName).Scan(&metricID, &tableName, &possiblyNew)
		if err != nil {
			t.Fatal(err)
		}
		if savedTableName != tableName {
			t.Errorf("expected table name to be the same:\ngot\n%v\nexpected\n%v", tableName, savedTableName)
		}
		if metricID != savedMetricID {
			t.Errorf("metric_id should be same:\nexpected:%v\ngot:%v", savedMetricID, metricID)
		}
		if possiblyNew {
			t.Errorf("unexpected value for possiblyNew %v", possiblyNew)
		}

		//changing just ending returns new table
		metricName = "test_metric_very_very_long_name_have_to_truncate_it_longer_than_64_chars_2"
		err = db.QueryRow(context.Background(), "SELECT * FROM _prom_catalog.get_or_create_metric_table_name($1)", metricName).Scan(&metricID, &tableName, &possiblyNew)
		if err != nil {
			t.Fatal(err)
		}
		if savedTableName == tableName {
			t.Errorf("expected table name to not be the same:\ngot\n%v\nnot =\n%v", tableName, savedTableName)
		}
		if metricID == savedMetricID {
			t.Errorf("metric_id should not be same:\nexpected:%v\ngot:%v", savedMetricID, metricID)
		}
		if !possiblyNew {
			t.Errorf("unexpected value for possiblyNew %v", possiblyNew)
		}
	})
}

func TestSQLChunkInterval(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	if !*useTimescaleDB {
		t.Skip("chunk intervals meaningless without TimescaleDB")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		ts := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "test"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.1},
					{Timestamp: 2, Value: 0.2},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "test2"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.1},
					{Timestamp: 2, Value: 0.2},
				},
			},
		}
		ingestor, err := NewPgxIngestor(db)
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()
		_, err = ingestor.Ingest(copyMetrics(ts), NewWriteRequest())
		if err != nil {
			t.Fatal(err)
		}
		verifyChunkInterval(t, db, "test", time.Duration(8*time.Hour))
		_, err = db.Exec(context.Background(), "SELECT prom_api.set_metric_chunk_interval('test2', INTERVAL '7 hours')")
		if err != nil {
			t.Error(err)
		}
		verifyChunkInterval(t, db, "test2", time.Duration(7*time.Hour))
		_, err = db.Exec(context.Background(), "SELECT prom_api.set_default_chunk_interval(INTERVAL '6 hours')")
		if err != nil {
			t.Error(err)
		}
		verifyChunkInterval(t, db, "test", time.Duration(6*time.Hour))
		verifyChunkInterval(t, db, "test2", time.Duration(7*time.Hour))
		_, err = db.Exec(context.Background(), "SELECT prom_api.reset_metric_chunk_interval('test2')")
		if err != nil {
			t.Error(err)
		}
		verifyChunkInterval(t, db, "test2", time.Duration(6*time.Hour))

		//set on a metric that doesn't exist should create the metric and set the parameter
		_, err = db.Exec(context.Background(), "SELECT prom_api.set_metric_chunk_interval('test_new_metric1', INTERVAL '7 hours')")
		if err != nil {
			t.Error(err)
		}
		verifyChunkInterval(t, db, "test_new_metric1", time.Duration(7*time.Hour))

		_, err = db.Exec(context.Background(), "SELECT prom_api.set_default_chunk_interval(INTERVAL '2 hours')")
		if err != nil {
			t.Error(err)
		}

		verifyChunkInterval(t, db, "test_new_metric1", time.Duration(7*time.Hour))

	})
}

func verifyChunkInterval(t testing.TB, db *pgxpool.Pool, tableName string, expectedDuration time.Duration) {
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
						{Name: MetricNameLabelName, Value: "test"},
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
						{Name: MetricNameLabelName, Value: "test"},
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
						{Name: MetricNameLabelName, Value: "test"},
						{Name: "foo", Value: "bar"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: MetricNameLabelName, Value: "test"},
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
						{Name: MetricNameLabelName, Value: "test"},
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
						{Name: MetricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			count:       1,
			countSeries: 1,
		},
		{
			name: "Two timeseries, one series",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: MetricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: MetricNameLabelName, Value: "test"},
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
						{Name: MetricNameLabelName, Value: "test1"},
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
						{Name: MetricNameLabelName, Value: "test2"},
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
			expectErr:   ErrNoMetricName,
		},
	}
	for tcIndex, c := range testCases {
		databaseName := fmt.Sprintf("%s_%d", *testDatabase, tcIndex)
		tcase := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			withDB(t, databaseName, func(db *pgxpool.Pool, t testing.TB) {
				ingestor, err := NewPgxIngestor(db)
				if err != nil {
					t.Fatal(err)
				}
				defer ingestor.Close()

				cnt, err := ingestor.Ingest(copyMetrics(tcase.metrics), NewWriteRequest())
				if err != nil && err != tcase.expectErr {
					t.Fatalf("got an unexpected error %v", err)
				}

				// our reporting of inserts is necessarily inexact due to
				// duplicates being dropped we report the number of samples the
				// ingestor handled, not necissarily how many were inserted
				// into the DB
				if cnt < tcase.count {
					t.Fatalf("incorrect counts: got %v expected %v\n", cnt, tcase.count)
				}

				if err != nil {
					return
				}

				tables := make(map[string]bool)
				for _, ts := range tcase.metrics {
					for _, l := range ts.Labels {
						if len(ts.Samples) > 0 && l.Name == MetricNameLabelName {
							tables[l.Value] = true
						}
					}
				}
				totalRows := 0
				for table := range tables {
					var rowsInTable int
					err := db.QueryRow(context.Background(), fmt.Sprintf("SELECT count(*) FROM prom_data.%s", table)).Scan(&rowsInTable)
					if err != nil {
						t.Fatal(err)
					}
					totalRows += rowsInTable
				}

				if totalRows != int(tcase.count) {
					t.Fatalf("counts not equal: got %v expected %v\n", totalRows, tcase.count)
				}

				err = ingestor.CompleteMetricCreation()
				if err != nil {
					t.Fatal(err)
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

func TestInsertCompressedDuplicates(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	if !*useTimescaleDB {
		t.Skip("compression meaningless without TimescaleDB")
	}
	if *useMultinode {
		t.Skip("compression not yet enabled for multinode")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		ts := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "tEsT"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 10000000, Value: 1.0},
				},
			},
		}
		ingestor, err := NewPgxIngestor(db)
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()
		_, err = ingestor.Ingest(copyMetrics(ts), NewWriteRequest())
		if err != nil {
			t.Fatal(err)
		}
		err = ingestor.CompleteMetricCreation()
		if err != nil {
			t.Fatal(err)
		}

		_, err = db.Exec(context.Background(), "SELECT compress_chunk(i) from show_chunks('prom_data.\"tEsT\"') i;")

		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.SQLState() == pgerrcode.DuplicateObject {
				//already compressed (could happen if policy already ran). This is fine
			} else {
				t.Fatal(err)
			}
		}

		ts = []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "tEsT"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.0},
				},
			},
		}

		_, err = ingestor.Ingest(copyMetrics(ts), NewWriteRequest())
		if err != nil {
			t.Fatal(err)
		}
		err = ingestor.CompleteMetricCreation()
		if err != nil {
			t.Fatal(err)
		}

		ts = []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "tEsT"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.2},
					{Timestamp: 10, Value: 3.0},
					{Timestamp: 10000000, Value: 4.0},
					{Timestamp: 10000001, Value: 5.0},
				},
			},
		}

		//ingest duplicate after compression
		_, err = ingestor.Ingest(copyMetrics(ts), NewWriteRequest())
		if err != nil {
			t.Fatal(err)
		}

		rows, err := db.Query(context.Background(), "SELECT value FROM prom_data.\"tEsT\" ORDER BY time")
		if err != nil {
			t.Fatal(err)
		}

		expected := []float64{0.0, 3.0, 1.0, 5.0}
		found := make([]float64, 0, 4)
		for rows.Next() {
			var value float64
			err = rows.Scan(&value)
			if err != nil {
				t.Fatal(err)
			}
			found = append(found, value)
		}
		if !reflect.DeepEqual(expected, found) {
			t.Errorf("wrong values in DB\nexpected:\n\t%v\ngot:\n\t%v", expected, found)
		}
	})
}

func TestInsertCompressed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	if !*useTimescaleDB {
		t.Skip("compression meaningless without TimescaleDB")
	}
	if *useMultinode {
		t.Skip("compression not yet enabled for multinode")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		ts := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "test"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.1},
				},
			},
		}
		ingestor, err := NewPgxIngestor(db)
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()
		_, err = ingestor.Ingest(copyMetrics(ts), NewWriteRequest())
		if err != nil {
			t.Fatal(err)
		}
		err = ingestor.CompleteMetricCreation()
		if err != nil {
			t.Fatal(err)
		}

		_, err = db.Exec(context.Background(), "SELECT compress_chunk(i) from show_chunks('prom_data.test') i;")
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.SQLState() == "42710" {
				//already compressed (could happen if policy already ran). This is fine
			} else {
				t.Fatal(err)
			}
		}
		//ingest after compression
		_, err = ingestor.Ingest(copyMetrics(ts), NewWriteRequest())
		if err != nil {
			t.Fatal(err)
		}
		var nextStartAfter time.Time
		var statsQuery string
		if *useTimescale2 {
			statsQuery = "SELECT next_start FROM timescaledb_information.job_stats WHERE hypertable_schema=$1::text AND hypertable_name = $2::text"
			err = db.QueryRow(context.Background(), statsQuery, "prom_data", "test").Scan(&nextStartAfter)
		} else {
			statsQuery = "SELECT next_start FROM timescaledb_information.policy_stats WHERE hypertable = $1::text::regclass"
			err = db.QueryRow(context.Background(), statsQuery, pgx.Identifier{"prom_data", "test"}.Sanitize()).Scan(&nextStartAfter)
		}
		if err != nil {
			t.Fatal(err)
		}
		if time.Until(nextStartAfter) < time.Hour*10 {
			t.Error("next_start was not changed enough")
		}
	})
}

func TestCompressionSetting(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	if !*useTimescaleDB {
		t.Skip("compression meaningless without TimescaleDB")
	}
	if *useMultinode {
		t.Skip("compression not yet enabled for multinode")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		var compressionEnabled bool
		err := db.QueryRow(context.Background(), "SELECT _prom_catalog.get_default_compression_setting()").Scan(&compressionEnabled)
		if err != nil {
			t.Fatal(err)
		}

		if !compressionEnabled {
			t.Error("compression should be enabled by default, was not")

		}
		_, err = db.Exec(context.Background(), "SELECT prom_api.set_default_compression_setting(false)")
		if err != nil {
			t.Fatal(err)
		}
		err = db.QueryRow(context.Background(), "SELECT _prom_catalog.get_default_compression_setting()").Scan(&compressionEnabled)
		if err != nil {
			t.Fatal(err)
		}
		if compressionEnabled {
			t.Error("compression should have been disabled")

		}
		ts := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "test"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.1},
				},
			},
		}
		ingestor, err := NewPgxIngestor(db)
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()
		_, err = ingestor.Ingest(copyMetrics(ts), NewWriteRequest())
		if err != nil {
			t.Fatal(err)
		}
		err = ingestor.CompleteMetricCreation()
		if err != nil {
			t.Fatal(err)
		}

		err = db.QueryRow(context.Background(), "SELECT _prom_catalog.get_metric_compression_setting('test')").Scan(&compressionEnabled)
		if err != nil {
			t.Fatal(err)
		}

		if compressionEnabled {
			t.Error("metric compression should be disabled as per default, was not")
		}

		_, err = db.Exec(context.Background(), "SELECT prom_api.set_metric_compression_setting('test', true)")
		if err != nil {
			t.Fatal(err)
		}

		_, err = db.Exec(context.Background(), "SELECT compress_chunk(i) from show_chunks('prom_data.test') i;")
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.SQLState() == "42710" {
				//already compressed (could happen if policy already ran). This is fine
			} else {
				t.Fatal(err)
			}
		}

		err = db.QueryRow(context.Background(), "SELECT _prom_catalog.get_metric_compression_setting('test')").Scan(&compressionEnabled)
		if err != nil {
			t.Fatal(err)
		}

		if !compressionEnabled {
			t.Fatal("metric compression should be enabled manually, was not")
		}

		_, err = db.Exec(context.Background(), "SELECT prom_api.reset_metric_compression_setting('test')")
		if err != nil {
			t.Fatal(err)
		}

		err = db.QueryRow(context.Background(), "SELECT _prom_catalog.get_metric_compression_setting('test')").Scan(&compressionEnabled)
		if err != nil {
			t.Fatal(err)
		}

		if compressionEnabled {
			t.Error("metric compression should be disabled as per default, was not")
		}
	})
}

// deep copy the metrics since we mutate them, and don't want to invalidate the tests
func copyMetrics(metrics []prompb.TimeSeries) []prompb.TimeSeries {
	out := make([]prompb.TimeSeries, len(metrics))
	copy(out, metrics)
	for i := range out {
		samples := make([]prompb.Sample, len(out[i].Samples))
		copy(samples, out[i].Samples)
		out[i].Samples = samples
	}
	return out
}
