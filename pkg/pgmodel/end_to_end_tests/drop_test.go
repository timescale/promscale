// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package end_to_end_tests

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"

	. "github.com/timescale/timescale-prometheus/pkg/pgmodel"
)

func TestSQLRetentionPeriod(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
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
		ingestor := NewPgxIngestor(db)
		defer ingestor.Close()
		_, err := ingestor.Ingest(ts)
		if err != nil {
			t.Fatal(err)
		}
		verifyRetentionPeriod(t, db, "test", time.Duration(90*24*time.Hour))
		_, err = db.Exec(context.Background(), "SELECT prom_api.set_metric_retention_period('test2', INTERVAL '7 hours')")
		if err != nil {
			t.Error(err)
		}

		verifyRetentionPeriod(t, db, "test", time.Duration(90*24*time.Hour))
		verifyRetentionPeriod(t, db, "test2", time.Duration(7*time.Hour))
		_, err = db.Exec(context.Background(), "SELECT prom_api.set_default_retention_period(INTERVAL '6 hours')")
		if err != nil {
			t.Error(err)
		}
		verifyRetentionPeriod(t, db, "test", time.Duration(6*time.Hour))
		verifyRetentionPeriod(t, db, "test2", time.Duration(7*time.Hour))
		_, err = db.Exec(context.Background(), "SELECT prom_api.reset_metric_retention_period('test2')")
		if err != nil {
			t.Error(err)
		}
		verifyRetentionPeriod(t, db, "test2", time.Duration(6*time.Hour))

		//set on a metric that doesn't exist should create the metric and set the parameter
		_, err = db.Exec(context.Background(), "SELECT prom_api.set_metric_retention_period('test_new_metric1', INTERVAL '7 hours')")
		if err != nil {
			t.Error(err)
		}
		verifyRetentionPeriod(t, db, "test_new_metric1", time.Duration(7*time.Hour))

		_, err = db.Exec(context.Background(), "SELECT prom_api.set_default_retention_period(INTERVAL '2 hours')")
		if err != nil {
			t.Error(err)
		}

		verifyRetentionPeriod(t, db, "test_new_metric1", time.Duration(7*time.Hour))

		//get on non-existing metric returns default
		verifyRetentionPeriod(t, db, "test_new_metric2", time.Duration(2*time.Hour))
	})
}

func verifyRetentionPeriod(t testing.TB, db *pgxpool.Pool, metricName string, expectedDuration time.Duration) {
	var durS int
	var dur time.Duration

	err := db.QueryRow(context.Background(),
		`SELECT EXTRACT(epoch FROM _prom_catalog.get_metric_retention_period($1))`,
		metricName).Scan(&durS)
	if err != nil {
		t.Error(err)
	}
	dur = time.Duration(durS) * time.Second

	if dur != expectedDuration {
		t.Fatalf("Unexpected retention period for table %v: got %v want %v", metricName, dur, expectedDuration)
	}
}

func TestSQLDropChunk(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		//a chunk way back in 2009
		chunkEnds := time.Date(2009, time.November, 11, 0, 0, 0, 0, time.UTC)

		ts := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "test"},
					{Name: "name1", Value: "value1"},
				},
				Samples: []prompb.Sample{
					{Timestamp: int64(model.TimeFromUnixNano(chunkEnds.UnixNano()) - 1), Value: 0.1},
					{Timestamp: int64(model.TimeFromUnixNano(time.Now().UnixNano()) - 1), Value: 0.1},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "test2"},
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
		err = db.QueryRow(context.Background(), "SELECT count(*) FROM show_chunks('prom_data.test')").Scan(&cnt)
		if err != nil {
			t.Fatal(err)
		}
		if cnt != 2 {
			t.Errorf("Expected there to be a chunk")
		}

		_, err = db.Exec(context.Background(), "CALL prom_api.drop_chunks()")
		if err != nil {
			t.Fatal(err)
		}
		err = db.QueryRow(context.Background(), "SELECT count(*) FROM show_chunks('prom_data.test')").Scan(&cnt)
		if err != nil {
			t.Fatal(err)
		}
		if cnt != 1 {
			t.Errorf("Expected the chunk to be dropped")
		}
		//noop works fine
		_, err = db.Exec(context.Background(), "CALL prom_api.drop_chunks()")
		if err != nil {
			t.Fatal(err)
		}
		//test2 isn't affected
		err = db.QueryRow(context.Background(), "SELECT count(*) FROM show_chunks('prom_data.test2')").Scan(&cnt)
		if err != nil {
			t.Fatal(err)
		}
		if cnt != 1 {
			t.Errorf("Expected the chunk to be dropped")
		}
	})
}

func TestSQLDropMetricChunk(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		//this is the range_end of a chunk boundary (exclusive)
		chunkEnds := time.Date(2009, time.November, 11, 0, 0, 0, 0, time.UTC)

		ts := []prompb.TimeSeries{
			{
				//this series will be deleted along with it's label
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "test"},
					{Name: "name1", Value: "value1"},
				},
				Samples: []prompb.Sample{
					//this will be dropped (notice the - 1)
					{Timestamp: int64(model.TimeFromUnixNano(chunkEnds.UnixNano()) - 1), Value: 0.1},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "test"},
					{Name: "name1", Value: "value2"},
				},
				Samples: []prompb.Sample{
					//this will remain after the drop
					{Timestamp: int64(model.TimeFromUnixNano(chunkEnds.UnixNano())), Value: 0.2},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: MetricNameLabelName, Value: "test"},
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
		err = db.QueryRow(context.Background(), `SELECT count(*) FROM prom_data.test`).Scan(&count)
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
