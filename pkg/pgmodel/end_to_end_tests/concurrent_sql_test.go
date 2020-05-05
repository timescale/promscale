// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package end_to_end_tests

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"

	_ "github.com/jackc/pgx/v4/stdlib"
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

func testConcurrentCreateSeries(t testing.TB, db *pgxpool.Pool, index int) int64 {
	var id *int64
	err := db.QueryRow(context.Background(), "SELECT _prom_catalog.create_series((SELECT id FROM _prom_catalog.metric WHERE metric_name=$3), $1, array[(SELECT id FROM _prom_catalog.label WHERE key = '__name__' AND value=$3), $2::int])",
		fmt.Sprintf("metric_%d", index), index, fmt.Sprintf("metric_%d", index)).Scan(&id)
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
