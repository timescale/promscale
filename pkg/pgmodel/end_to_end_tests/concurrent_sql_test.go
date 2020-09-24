// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package end_to_end_tests

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/common/model"
	"github.com/timescale/promscale/pkg/prompb"

	_ "github.com/jackc/pgx/v4/stdlib"
	. "github.com/timescale/promscale/pkg/pgmodel"
)

func testConcurrentMetricTable(t testing.TB, db *pgxpool.Pool, metricName string) int64 {
	var id *int64
	var name *string
	err := db.QueryRow(context.Background(), "SELECT id, table_name FROM _prom_catalog.create_metric_table($1)", metricName).Scan(&id, &name)
	if err != nil {
		t.Fatal(err)
	}
	if id == nil || name == nil {
		t.Fatalf("NULL found")
	}
	return *id
}

func testConcurrentNewLabel(t testing.TB, db *pgxpool.Pool, labelName string) int64 {
	var id *int64
	err := db.QueryRow(context.Background(), "SELECT _prom_catalog.get_new_label_id($1, $1)", labelName).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	if id == nil {
		t.Fatalf("NULL found")
	}
	return *id
}

func testConcurrentCreateSeries(t testing.TB, db *pgxpool.Pool, index int, metricName string) int64 {
	var id *int64
	err := db.QueryRow(context.Background(), "SELECT _prom_catalog.create_series((SELECT id FROM _prom_catalog.metric WHERE metric_name=$3), $1, array[(SELECT id FROM _prom_catalog.label WHERE key = '__name__' AND value=$3), $2::int])",
		metricName, index, metricName).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	if id == nil {
		t.Fatalf("NULL found")
	}
	return *id
}

func testConcurrentGetSeries(t testing.TB, db *pgxpool.Pool, metricName string) int64 {
	var id *int64
	err := db.QueryRow(context.Background(), "SELECT series_id FROM _prom_catalog.get_or_create_series_id_for_kv_array($1, array['__name__'], array[$1])",
		metricName).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	if id == nil {
		t.Fatalf("NULL found")
	}
	return *id
}

func testConcurrentGOCMetricTable(t testing.TB, db *pgxpool.Pool, metricName string) int64 {
	var id *int64
	var name *string
	err := db.QueryRow(context.Background(), "SELECT id, table_name FROM _prom_catalog.get_or_create_metric_table_name($1)", metricName).Scan(&id, &name)
	if err != nil {
		t.Fatal(err)
	}
	if id == nil || name == nil {
		t.Fatalf("NULL found")
	}
	return *id
}

func TestConcurrentSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
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
				id1 = testConcurrentCreateSeries(t, db, i, fmt.Sprintf("metric_%d", i))
			}()
			go func() {
				defer wg.Done()
				id2 = testConcurrentCreateSeries(t, db, i, fmt.Sprintf("metric_%d", i))
			}()
			wg.Wait()

			if id1 != id2 {
				t.Fatalf("ids aren't equal: %d != %d", id1, id2)
			}

			/* test creating tables with same long name */
			wg.Add(3)
			go func() {
				defer wg.Done()
				_, err := db.Exec(context.Background(), "CALL _prom_catalog.finalize_metric_creation()")
				if err != nil {
					t.Fatal(err)
				}
			}()
			metric := fmt.Sprintf("test1"+strings.Repeat("long_name_", 10)+"goc_metric_%d", i)
			go func() {
				defer wg.Done()
				id1 = testConcurrentGOCMetricTable(t, db, metric)
			}()
			go func() {
				defer wg.Done()
				id2 = testConcurrentGOCMetricTable(t, db, metric)
			}()
			wg.Wait()

			if id1 != id2 {
				t.Fatalf("ids aren't equal: %d != %d", id1, id2)
			}

			/* test creating tables with long names and different suffixes */
			wg.Add(3)
			go func() {
				defer wg.Done()
				_, err := db.Exec(context.Background(), "CALL _prom_catalog.finalize_metric_creation()")
				if err != nil {
					t.Fatal(err)
				}
			}()
			metric = fmt.Sprintf("test2"+strings.Repeat("long_name_", 10)+"goc_metric_%d", i)
			go func() {
				defer wg.Done()
				id1 = testConcurrentGOCMetricTable(t, db, metric)
			}()
			go func() {
				defer wg.Done()
				id2 = testConcurrentGOCMetricTable(t, db, metric+"_diff")
			}()
			wg.Wait()

			wg.Add(2)
			go func() {
				defer wg.Done()
				id1 = testConcurrentGetSeries(t, db, fmt.Sprintf("gs_metric_%d", i))
			}()
			go func() {
				defer wg.Done()
				id2 = testConcurrentGetSeries(t, db, fmt.Sprintf("gs_metric_%d", i))
			}()
			wg.Wait()

			if id1 != id2 {
				t.Fatalf("ids aren't equal: %d != %d", id1, id2)
			}
		}
	})
}

func testConcurrentInsertSimple(t testing.TB, db *pgxpool.Pool, metric string) {
	metrics := []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: MetricNameLabelName, Value: metric},
				{Name: "node", Value: "brain"},
			},
			Samples: []prompb.Sample{
				{Timestamp: int64(model.Now()), Value: 0.5},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: MetricNameLabelName, Value: metric},
				{Name: "node", Value: "pinky"},
			},
			Samples: []prompb.Sample{
				{Timestamp: int64(model.Now()), Value: 0.5},
			},
		},
	}

	ingestor, err := NewPgxIngestor(db)
	if err != nil {
		t.Fatal(err)
	}
	defer ingestor.Close()
	_, err = ingestor.Ingest(copyMetrics(metrics), NewWriteRequest())
	if err != nil {
		t.Fatal(err)
	}
}

func testConcurrentInsertAdvanced(t testing.TB, db *pgxpool.Pool) {
	metrics := []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: MetricNameLabelName, Value: "cpu_usage_a"},
				{Name: "namespace", Value: "production"},
				{Name: "node", Value: "brain"},
			},
			Samples: []prompb.Sample{
				{Timestamp: int64(model.Now()), Value: 0.5},
				{Timestamp: int64(model.Now()), Value: 0.6},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: MetricNameLabelName, Value: "cpu_usage_a"},
				{Name: "namespace", Value: "dev"},
				{Name: "node", Value: "pinky"},
			},
			Samples: []prompb.Sample{
				{Timestamp: int64(model.Now()), Value: 0.1},
				{Timestamp: int64(model.Now()), Value: 0.2},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: MetricNameLabelName, Value: "cpu_total_a"},
				{Name: "namespace", Value: "production"},
				{Name: "node", Value: "brain"},
			},
			Samples: []prompb.Sample{
				{Timestamp: int64(model.Now()), Value: 0.5},
				{Timestamp: int64(model.Now()), Value: 0.6},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: MetricNameLabelName, Value: "cpu_total_a"},
				{Name: "namespace", Value: "dev"},
				{Name: "node", Value: "pinky"},
			},
			Samples: []prompb.Sample{
				{Timestamp: int64(model.Now()), Value: 0.1},
				{Timestamp: int64(model.Now()), Value: 0.2},
			},
		},
	}

	ingestor, err := NewPgxIngestor(db)
	if err != nil {
		t.Fatal(err)
	}

	defer ingestor.Close()
	_, err = ingestor.Ingest(copyMetrics(metrics), NewWriteRequest())
	if err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentInsert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(2)
			go func() {
				defer wg.Done()
				testConcurrentInsertSimple(t, db, fmt.Sprintf("metric_%d", i))
			}()
			go func() {
				defer wg.Done()
				testConcurrentInsertSimple(t, db, fmt.Sprintf("metric_%d", i))
			}()
			wg.Wait()

			wg.Add(2)
			go func() {
				defer wg.Done()
				testConcurrentInsertSimple(t, db, fmt.Sprintf("metric_1_%d", i))
			}()
			go func() {
				defer wg.Done()
				testConcurrentInsertSimple(t, db, fmt.Sprintf("metric_2_%d", i))
			}()
			wg.Wait()
		}

		for i := 0; i < 10; i++ {
			j := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				testConcurrentInsertSimple(t, db, fmt.Sprintf("metric_multi_%d", j))
			}()
			wg.Add(1)
			go func() {
				defer wg.Done()
				testConcurrentInsertSimple(t, db, fmt.Sprintf("metric_multi_%d", j))
			}()
		}
		wg.Wait()

		wg.Add(2)
		go func() {
			defer wg.Done()
			testConcurrentInsertAdvanced(t, db)
		}()
		go func() {
			defer wg.Done()
			testConcurrentInsertAdvanced(t, db)
		}()
		wg.Wait()
	})
}
