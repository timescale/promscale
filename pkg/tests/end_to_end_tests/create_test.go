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
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
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
					{Name: model.MetricNameLabelName, Value: "Test"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.1},
					{Timestamp: 2, Value: 0.2},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "Test2"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.1},
					{Timestamp: 2, Value: 0.2},
				},
			},
		}
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db))
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()
		_, err = ingestor.Ingest(copyMetrics(ts), ingstr.NewWriteRequest())
		if err != nil {
			t.Fatal(err)
		}
		verifyChunkInterval(t, db, "Test", time.Duration(8*time.Hour))
		_, err = db.Exec(context.Background(), "SELECT prom_api.set_metric_chunk_interval('Test2', INTERVAL '7 hours')")
		if err != nil {
			t.Error(err)
		}
		verifyChunkInterval(t, db, "Test2", time.Duration(7*time.Hour))
		_, err = db.Exec(context.Background(), "SELECT prom_api.set_default_chunk_interval(INTERVAL '6 hours')")
		if err != nil {
			t.Error(err)
		}
		verifyChunkInterval(t, db, "Test", time.Duration(6*time.Hour))
		verifyChunkInterval(t, db, "Test2", time.Duration(7*time.Hour))
		_, err = db.Exec(context.Background(), "SELECT prom_api.reset_metric_chunk_interval('Test2')")
		if err != nil {
			t.Error(err)
		}
		verifyChunkInterval(t, db, "Test2", time.Duration(6*time.Hour))

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
						{Name: model.MetricNameLabelName, Value: "Test"},
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
						{Name: model.MetricNameLabelName, Value: "Test"},
						{Name: "test", Value: "test"},
					},
				},
			},
		},
		{
			name: "One metric, metric name all capital letters",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "TEST"},
						{Name: "test", Value: "test"},
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
			name: "Two timeseries",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "Test"},
						{Name: "foo", Value: "bar"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "Test"},
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
						{Name: model.MetricNameLabelName, Value: "Test"},
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
						{Name: model.MetricNameLabelName, Value: "Test"},
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
						{Name: model.MetricNameLabelName, Value: "Test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "Test"},
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
						{Name: model.MetricNameLabelName, Value: "Test1"},
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
						{Name: model.MetricNameLabelName, Value: "Test2"},
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
			expectErr:   errors.ErrNoMetricName,
		},
	}
	for tcIndex, c := range testCases {
		databaseName := fmt.Sprintf("%s_%d", *testDatabase, tcIndex)
		tcase := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			withDB(t, databaseName, func(db *pgxpool.Pool, t testing.TB) {
				ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db))
				if err != nil {
					t.Fatal(err)
				}
				defer ingestor.Close()

				cnt, err := ingestor.Ingest(copyMetrics(tcase.metrics), ingstr.NewWriteRequest())
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

				metricNames := make(map[string]bool)
				for _, ts := range tcase.metrics {
					if len(ts.Samples) > 0 {
						for _, l := range ts.Labels {
							if l.Name == model.MetricNameLabelName {
								metricNames[l.Value] = true
							}
						}
					}
				}

				totalRows := 0
				for metricName := range metricNames {
					var (
						tableName   string
						rowsInTable int
					)
					err := db.QueryRow(context.Background(), fmt.Sprintf("SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists('%s');", metricName)).Scan(&tableName)
					if err != nil {
						t.Fatal(err)
					}

					err = db.QueryRow(context.Background(), fmt.Sprintf(`SELECT count(*) FROM prom_data."%s"`, tableName)).Scan(&rowsInTable)
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
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		ts := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "tEsT"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 100000000, Value: 1.0},
				},
			},
		}
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db))
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()
		_, err = ingestor.Ingest(copyMetrics(ts), ingstr.NewWriteRequest())
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
					{Name: model.MetricNameLabelName, Value: "tEsT"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.0},
				},
			},
		}

		_, err = ingestor.Ingest(copyMetrics(ts), ingstr.NewWriteRequest())
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
					{Name: model.MetricNameLabelName, Value: "tEsT"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.2},
					{Timestamp: 10, Value: 3.0},
					{Timestamp: 100000000, Value: 4.0},
					{Timestamp: 100000001, Value: 5.0},
				},
			},
		}

		//ingest duplicate after compression
		_, err = ingestor.Ingest(copyMetrics(ts), ingstr.NewWriteRequest())
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
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		ts := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "Test"},
					{Name: "test", Value: "test"},
				},
				// Two samples that, by default, end up in different chunks.
				// This is to check that decompression works on all necessary chunks.
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.1},
					{Timestamp: 100000000, Value: 0.1},
				},
			},
		}
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db))
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()
		_, err = ingestor.Ingest(copyMetrics(ts), ingstr.NewWriteRequest())
		if err != nil {
			t.Fatal(err)
		}
		err = ingestor.CompleteMetricCreation()
		if err != nil {
			t.Fatal(err)
		}

		var tableName string
		err = db.QueryRow(context.Background(), "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists('Test');").Scan(&tableName)
		if err != nil {
			t.Fatal(err)
		}

		_, err = db.Exec(context.Background(), fmt.Sprintf(`SELECT compress_chunk(i) from show_chunks('prom_data."%s"') i;`, tableName))
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.SQLState() == "42710" {
				//already compressed (could happen if policy already ran). This is fine
			} else {
				t.Fatal(err)
			}
		}
		//ingest after compression
		_, err = ingestor.Ingest(copyMetrics(ts), ingstr.NewWriteRequest())
		if err != nil {
			t.Fatal(err)
		}
		var nextStartAfter time.Time
		var statsQuery string
		if *useTimescale2 {
			statsQuery = "SELECT delay_compression_until FROM _prom_catalog.metric " +
				"WHERE metric_name = $1::text"
			err = db.QueryRow(context.Background(), statsQuery, "Test").Scan(&nextStartAfter)
		} else {
			statsQuery = "SELECT next_start FROM timescaledb_information.policy_stats WHERE hypertable = $1::text::regclass"
			err = db.QueryRow(context.Background(), statsQuery, pgx.Identifier{"prom_data", tableName}.Sanitize()).Scan(&nextStartAfter)
		}
		if err != nil {
			t.Fatal(err)
		}
		if time.Until(nextStartAfter) < time.Hour*10 {
			t.Error("next_start was not changed enough")
		}
	})
}

func verifyNumDataNodes(t testing.TB, db *pgxpool.Pool, tableName string, expectedNodes int) {
	var numNodes int

	err := db.QueryRow(context.Background(),
		`SELECT array_length(data_nodes, 1)
		FROM timescaledb_information.hypertables
		WHERE hypertable_name = $1`,
		tableName).Scan(&numNodes)
	if err != nil {
		t.Error(err)
	}

	if expectedNodes != numNodes {
		t.Errorf("Unexpected num nodes for table %v: got %v want %v", tableName, numNodes, expectedNodes)
	}
}

func TestInsertMultinodeAddNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	if !*useTimescaleDB {
		t.Skip("compression meaningless without TimescaleDB")
	}
	if !*useMultinode {
		t.Skip("Only applies for multinode")
	}
	insertMultinodeAddNodes(t, true)
	insertMultinodeAddNodes(t, false)
}
func insertMultinodeAddNodes(t *testing.T, attachExisting bool) {

	withDBAttachNode(t, *testDatabase, attachExisting, func(db *pgxpool.Pool, t testing.TB) {
		ts := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "created_before_add_node"},
					{Name: "test", Value: "test"},
				},
				// Two samples that, by default, end up in different chunks.
				// This is to check that decompression works on all necessary chunks.
				Samples: []prompb.Sample{
					{Timestamp: 100, Value: 0.1},
				},
			},
		}
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db))
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()
		_, err = ingestor.Ingest(copyMetrics(ts), ingstr.NewWriteRequest())
		if err != nil {
			t.Fatal(err)
		}
		err = ingestor.CompleteMetricCreation()
		if err != nil {
			t.Fatal(err)
		}
		verifyNumDataNodes(t, db, "created_before_add_node", 1)
	},
		func(db *pgxpool.Pool, t testing.TB) {
			ts := []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "created_before_add_node"},
						{Name: "test", Value: "test"},
					},
					// Two samples that, by default, end up in different chunks.
					// This is to check that decompression works on all necessary chunks.
					Samples: []prompb.Sample{
						{Timestamp: 200, Value: 0.2},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "created_after_add_node"},
						{Name: "test", Value: "test"},
					},
					// Two samples that, by default, end up in different chunks.
					// This is to check that decompression works on all necessary chunks.
					Samples: []prompb.Sample{
						{Timestamp: 200, Value: 0.3},
					},
				},
			}
			ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db))
			if err != nil {
				t.Fatal(err)
			}
			defer ingestor.Close()
			_, err = ingestor.Ingest(copyMetrics(ts), ingstr.NewWriteRequest())
			if err != nil {
				t.Fatal(err)
			}
			err = ingestor.CompleteMetricCreation()
			if err != nil {
				t.Fatal(err)
			}
			if attachExisting {
				verifyNumDataNodes(t, db, "created_before_add_node", 2)
			} else {
				verifyNumDataNodes(t, db, "created_before_add_node", 1)
			}
			verifyNumDataNodes(t, db, "created_after_add_node", 2)
		})
}

func TestCompressionSetting(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	if !*useTimescaleDB {
		t.Skip("compression meaningless without TimescaleDB")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		checkJobs := func(t testing.TB, jobs_expected int) {
			countQuery := ""
			jobs := make([]string, 0)
			if *useTimescale2 {
				//compression does not effect #jobs in timescaledb2
				return
			}
			countQuery = "SELECT array_agg((s.*)::text) FROM _timescaledb_config.bgw_job s"
			err := db.QueryRow(context.Background(), countQuery).Scan(&jobs)
			if err != nil {
				t.Fatal(err)
			}
			if len(jobs) != jobs_expected {
				t.Errorf("unexpected jobs, expected 1 got %v", jobs)
			}
		}

		var compressionEnabled bool
		err := db.QueryRow(context.Background(), "SELECT _prom_catalog.get_default_compression_setting()").Scan(&compressionEnabled)
		if err != nil {
			t.Fatal(err)
		}

		if !compressionEnabled {
			t.Error("compression should be enabled by default, was not")
		}

		checkJobs(t, 1)

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

		checkJobs(t, 1)

		ts := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "Test"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.1},
				},
			},
		}
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db))
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()
		_, err = ingestor.Ingest(copyMetrics(ts), ingstr.NewWriteRequest())
		if err != nil {
			t.Fatal(err)
		}
		err = ingestor.CompleteMetricCreation()
		if err != nil {
			t.Fatal(err)
		}

		err = db.QueryRow(context.Background(), "SELECT _prom_catalog.get_metric_compression_setting('Test')").Scan(&compressionEnabled)
		if err != nil {
			t.Fatal(err)
		}

		if compressionEnabled {
			t.Error("metric compression should be disabled as per default, was not")
		}

		checkJobs(t, 1)

		_, err = db.Exec(context.Background(), "SELECT prom_api.set_metric_compression_setting('Test', true)")
		if err != nil {
			t.Fatal(err)
		}

		var tableName string
		err = db.QueryRow(context.Background(), "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists('Test');").Scan(&tableName)
		if err != nil {
			t.Fatal(err)
		}

		_, err = db.Exec(context.Background(), fmt.Sprintf(`SELECT compress_chunk(i) from show_chunks('prom_data."%s"') i;`, tableName))
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.SQLState() == "42710" {
				//already compressed (could happen if policy already ran). This is fine
			} else {
				t.Fatal(err)
			}
		}

		err = db.QueryRow(context.Background(), "SELECT _prom_catalog.get_metric_compression_setting('Test')").Scan(&compressionEnabled)
		if err != nil {
			t.Fatal(err)
		}

		if !compressionEnabled {
			t.Fatal("metric compression should be enabled manually, was not")
		}

		checkJobs(t, 2)

		if *useMultinode {
			//TODO turning compression off in multinode is broken upstream.
			return
		}

		_, err = db.Exec(context.Background(), "SELECT prom_api.reset_metric_compression_setting('Test')")
		if err != nil {
			t.Fatal(err)
		}

		err = db.QueryRow(context.Background(), "SELECT _prom_catalog.get_metric_compression_setting('Test')").Scan(&compressionEnabled)
		if err != nil {
			t.Fatal(err)
		}

		if compressionEnabled {
			t.Error("metric compression should be disabled as per default, was not")
		}
		checkJobs(t, 1)
	})
}

func TestCustomCompressionJob(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	if !*useTimescaleDB {
		t.Skip("compression meaningless without TimescaleDB")
	}
	if !*useTimescale2 {
		t.Skip("test meaningless without Timescale 2")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {

		ts := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "Test1"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.1},
				},
			},
		}
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db))
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()

		_, err = ingestor.Ingest(copyMetrics(ts), ingstr.NewWriteRequest())
		if err != nil {
			t.Fatal(err)
		}
		err = ingestor.CompleteMetricCreation()
		if err != nil {
			t.Fatal(err)
		}

		var tableName string
		err = db.QueryRow(context.Background(), "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists('Test1');").Scan(&tableName)
		if err != nil {
			t.Fatal(err)
		}

		// in early versions of Timescale multinode the compression catalog
		// would not be updated for distributed hypertables so we detect
		// compression using a probe INSERT
		chunkIsCompressed := func(time string) bool {
			insert := fmt.Sprintf(`INSERT INTO prom_data."%s" VALUES ('%s', 0.1, 1);`, tableName, time)
			_, err := db.Exec(context.Background(), insert)
			if err != nil {
				pgErr, ok := err.(*pgconn.PgError)
				if !ok {
					t.Fatal(err)
				}
				if pgErr.SQLState() == "42710" ||
					pgErr.SQLState() == "0A000" {
					//already compressed
					return true
				} else if pgErr.SQLState() == "23505" {
					// violates unique constraint
					return false
				}
				t.Fatal(err)
			}
			return false
		}

		if chunkIsCompressed("1970-01-01 00:00:00.001+00") {
			t.Error("chunk compressed too soon")
		}

		runCompressionJob := func() {
			//compress_metric_chunks and not execute_compression_policy since we don't want the decompression delay logic here.
			_, err = db.Exec(context.Background(), `CALL _prom_catalog.compress_metric_chunks('Test1')`)
			if err != nil {
				t.Fatal(err)
			}
		}
		runCompressionJob()

		// should not be compressed, not enough chunks
		if chunkIsCompressed("1970-01-01 00:00:00.001+00") {
			t.Error("chunk compressed too soon")
		}

		// add another chunk to each data node
		insert := fmt.Sprintf(`INSERT INTO prom_data."%s" VALUES ('1970-01-02 00:00:00.001+00', 0.1, 1);`, tableName)
		_, err = db.Exec(context.Background(), insert)
		if err != nil {
			t.Fatal(err)
		}

		insert = fmt.Sprintf(`INSERT INTO prom_data."%s" VALUES ('1970-01-03 00:00:00.001+00', 0.1, 1);`, tableName)
		_, err = db.Exec(context.Background(), insert)
		if err != nil {
			t.Fatal(err)
		}

		runCompressionJob()

		// first chunk should be compressed
		if !chunkIsCompressed("1970-01-01 00:00:00.001+00") {
			t.Fatal("first chunk not compressed")
		}

		// second chunk should not be
		if chunkIsCompressed("1970-02-01 00:00:00.001+00") {
			t.Error("second chunk compressed too soon")
		}

		// third chunk should not be
		if chunkIsCompressed("1970-03-01 00:00:00.001+00") {
			t.Error("second chunk compressed too soon")
		}

		// decompress the first chunk
		_, err = ingestor.Ingest(copyMetrics(ts), ingstr.NewWriteRequest())
		if err != nil {
			t.Fatal(err)
		}

		// first chunk should not be compressed
		if chunkIsCompressed("1970-01-01 00:00:00.001+00") {
			t.Error("first chunk still compressed")
		}

		// second chunk should not be compressed either
		if chunkIsCompressed("1970-02-01 00:00:00.001+00") {
			t.Error("second chunk compressed too soon")
		}

		// nor third
		if chunkIsCompressed("1970-03-01 00:00:00.001+00") {
			t.Error("second chunk compressed too soon")
		}

		runCompressionJob()

		// first chunk should be compressed
		if !chunkIsCompressed("1970-01-01 00:00:00.001+00") {
			t.Fatal("first chunk not compressed")
		}

		// in multinode the oldness check is per-datanode, so there should
		// always one uncompressed chunk on each datanode
		if *useMultinode {
			// second chunk should not be
			if chunkIsCompressed("1970-02-01 00:00:00.001+00") {
				t.Error("second chunk compressed too soon")
			}
		} else {
			if !chunkIsCompressed("1970-02-01 00:00:00.001+00") {
				t.Error("second chunk uncompressed")
			}
		}

		// nor third
		if chunkIsCompressed("1970-03-01 00:00:00.001+00") {
			t.Error("third chunk compressed too soon")
		}

		// Add an earlier chunk
		insert = fmt.Sprintf(`INSERT INTO prom_data."%s" VALUES ('1969-01-01 00:00:00.001+00', 0.1, 1);`, tableName)
		_, err = db.Exec(context.Background(), insert)
		if err != nil {
			t.Fatal(err)
		}

		runCompressionJob()
		// original chunk should be compressed
		if !chunkIsCompressed("1970-01-01 00:00:00.001+00") {
			t.Error("original chunk not compressed")
		}

		// earlier chunk should be compressed
		if !chunkIsCompressed("1969-01-01 00:00:00.001+00") {
			t.Error("earlier chunk not compressed")
		}

		if *useMultinode {
			// second chunk should not be
			if chunkIsCompressed("1970-02-01 00:00:00.001+00") {
				t.Error("second chunk compressed too soon")
			}
		} else {
			if !chunkIsCompressed("1970-02-01 00:00:00.001+00") {
				t.Error("second chunk uncompressed")
			}
		}

		// third chunk should not be
		if chunkIsCompressed("1970-03-01 00:00:00.001+00") {
			t.Error("third chunk compressed too soon")
		}
	})
}

func TestExecuteMaintenanceCompressionJob(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	if !*useTimescaleDB {
		t.Skip("compression meaningless without TimescaleDB")
	}
	if !*useTimescale2 {
		t.Skip("test meaningless without Timescale 2")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		// in early versions of Timescale multinode the compression catalog
		// would not be updated for distributed hypertables so we detect
		// compression using a probe INSERT
		chunkIsCompressed := func(time string) bool {
			insert := fmt.Sprintf("INSERT INTO prom_data.test VALUES ('%s', 0.1, 1);", time)
			_, err := db.Exec(context.Background(), insert)
			if err != nil {
				pgErr, ok := err.(*pgconn.PgError)
				if !ok {
					t.Fatal(err)
				}
				if pgErr.SQLState() == "42710" ||
					pgErr.SQLState() == "0A000" {
					//already compressed
					return true
				} else if pgErr.SQLState() == "23505" {
					// violates unique constraint
					return false
				}
				t.Fatal(err)
			}
			return false
		}

		ts := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "test"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.1},
				},
			},
		}
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db))
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()

		_, err = ingestor.Ingest(copyMetrics(ts), ingstr.NewWriteRequest())
		if err != nil {
			t.Fatal(err)
		}
		err = ingestor.CompleteMetricCreation()
		if err != nil {
			t.Fatal(err)
		}

		if chunkIsCompressed("1970-01-01 00:00:00.001+00") {
			t.Error("chunk compressed too soon")
		}

		_, err = db.Exec(context.Background(), "SELECT prom_api.set_metric_retention_period('test', INTERVAL '100 years')")
		if err != nil {
			t.Error(err)
		}

		runMaintenanceJob := func() {
			//execute_maintenance and not execute_compression_policy since we want to test end-to-end
			_, err = db.Exec(context.Background(), `CALL prom_api.execute_maintenance()`)
			if err != nil {
				t.Fatal(err)
			}
		}
		runMaintenanceJob()

		// should not be compressed, not enough chunks
		if chunkIsCompressed("1970-01-01 00:00:00.001+00") {
			t.Error("chunk compressed too soon")
		}

		// add another chunk to each data node
		insert := "INSERT INTO prom_data.test VALUES ('1970-01-02 00:00:00.001+00', 0.1, 1);"
		_, err = db.Exec(context.Background(), insert)
		if err != nil {
			t.Fatal(err)
		}

		insert = "INSERT INTO prom_data.test VALUES ('1970-01-03 00:00:00.001+00', 0.1, 1);"
		_, err = db.Exec(context.Background(), insert)
		if err != nil {
			t.Fatal(err)
		}

		runMaintenanceJob()

		// first chunk should be compressed
		if !chunkIsCompressed("1970-01-01 00:00:00.001+00") {
			t.Fatal("first chunk not compressed")
		}

		// second chunk should not be
		if chunkIsCompressed("1970-02-01 00:00:00.001+00") {
			t.Error("second chunk compressed too soon")
		}

		// third chunk should not be
		if chunkIsCompressed("1970-03-01 00:00:00.001+00") {
			t.Error("second chunk compressed too soon")
		}

		// decompress the first chunk
		_, err = ingestor.Ingest(copyMetrics(ts), ingstr.NewWriteRequest())
		if err != nil {
			t.Fatal(err)
		}

		// first chunk should not be compressed
		if chunkIsCompressed("1970-01-01 00:00:00.001+00") {
			t.Error("first chunk still compressed")
		}

		// second chunk should not be compressed either
		if chunkIsCompressed("1970-02-01 00:00:00.001+00") {
			t.Error("second chunk compressed too soon")
		}

		// nor third
		if chunkIsCompressed("1970-03-01 00:00:00.001+00") {
			t.Error("second chunk compressed too soon")
		}

		//this will have a delay on compression and so it will not recompress
		runMaintenanceJob()

		// first chunk should be compressed because of delay
		if chunkIsCompressed("1970-01-01 00:00:00.001+00") {
			t.Fatal("first chunk is compressed")
		}

		// in multinode the oldness check is per-datanode, so there should
		// always one uncompressed chunk on each datanode
		if *useMultinode {
			// second chunk should not be
			if chunkIsCompressed("1970-02-01 00:00:00.001+00") {
				t.Error("second chunk compressed too soon")
			}
		} else {
			if chunkIsCompressed("1970-02-01 00:00:00.001+00") {
				t.Error("second chunk compressed")
			}
		}

		// nor third
		if chunkIsCompressed("1970-03-01 00:00:00.001+00") {
			t.Error("third chunk compressed too soon")
		}
	})
}
func TestConfigMaintenanceJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	if !*useTimescaleDB {
		t.Skip("jobs meaningless without TimescaleDB")
	}
	if !*useTimescale2 {
		t.Skip("test meaningless without Timescale 2")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		cnt := 0
		err := db.QueryRow(context.Background(),
			"SELECT count(*) FROM timescaledb_information.jobs WHERE proc_schema = '_prom_catalog' AND proc_name = 'execute_maintenance_job'").
			Scan(&cnt)
		if err != nil {
			t.Fatal(err)
		}

		if cnt != 2 {
			t.Fatal("Incorrect number of jobs at startup")
		}

		changeJobs := func(numJobs int, scheduleInterval time.Duration) {
			_, err = db.Exec(context.Background(), "SELECT config_maintenance_jobs($1, $2)", numJobs, scheduleInterval)
			if err != nil {
				t.Fatal(err)
			}
			err = db.QueryRow(context.Background(),
				"SELECT count(*) FROM timescaledb_information.jobs WHERE proc_schema = '_prom_catalog' AND proc_name = 'execute_maintenance_job' AND schedule_interval = $1", scheduleInterval).
				Scan(&cnt)
			if err != nil {
				t.Fatal(err)
			}
			if cnt != numJobs {
				t.Fatalf("Unexpected number of jobs. Got %v, expected %v", cnt, numJobs)
			}
			err = db.QueryRow(context.Background(),
				"SELECT count(*) FROM timescaledb_information.jobs WHERE proc_schema = '_prom_catalog' AND proc_name = 'execute_maintenance_job' AND schedule_interval != $1", scheduleInterval).
				Scan(&cnt)
			if err != nil {
				t.Fatal(err)
			}
			if cnt != 0 {
				t.Fatalf("found %v jobs with wrong schedule interval", cnt)
			}
		}

		changeJobs(4, time.Minute*30)
		changeJobs(4, time.Minute*45)
		changeJobs(5, time.Minute*45)
		changeJobs(2, time.Minute*45)
		changeJobs(1, time.Minute*30)
		changeJobs(0, time.Minute*30)
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
