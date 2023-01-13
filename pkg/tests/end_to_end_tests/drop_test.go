// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package end_to_end_tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	pgmodel "github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
)

func TestSQLRetentionPeriod(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(dbOwner *pgxpool.Pool, t testing.TB) {
		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_admin")
		defer db.Close()

		ts := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: pgmodel.MetricNameLabelName, Value: "test"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.1},
					{Timestamp: 2, Value: 0.2},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: pgmodel.MetricNameLabelName, Value: "test2"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.1},
					{Timestamp: 2, Value: 0.2},
				},
			},
		}
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()
		_, _, err = ingestor.IngestMetrics(context.Background(), newWriteRequestWithTs(copyMetrics(ts)))
		if err != nil {
			t.Fatal(err)
		}
		verifyRetentionPeriod(t, db, "TEST", 90*24*time.Hour)
		_, err = db.Exec(context.Background(), "SELECT prom_api.set_metric_retention_period('prom_data', 'test2', INTERVAL '7 hours')")
		if err != nil {
			t.Error(err)
		}

		verifyRetentionPeriod(t, db, "TEST", 90*24*time.Hour)
		verifyRetentionPeriod(t, db, "test2", 7*time.Hour)
		_, err = db.Exec(context.Background(), "SELECT prom_api.set_default_retention_period(INTERVAL '6 hours')")
		if err != nil {
			t.Error(err)
		}
		verifyRetentionPeriod(t, db, "TEST", 6*time.Hour)
		verifyRetentionPeriod(t, db, "test2", 7*time.Hour)
		_, err = db.Exec(context.Background(), "SELECT prom_api.set_metric_retention_period('TEST', INTERVAL '8 hours')")
		if err != nil {
			t.Error(err)
		}
		_, err = db.Exec(context.Background(), "SELECT prom_api.reset_metric_retention_period('prom_data', 'test2')")
		if err != nil {
			t.Error(err)
		}
		verifyRetentionPeriod(t, db, "test2", 6*time.Hour)
		verifyRetentionPeriod(t, db, "TEST", 8*time.Hour)

		_, err = db.Exec(context.Background(), "SELECT prom_api.reset_metric_retention_period('TEST')")
		if err != nil {
			t.Error(err)
		}
		verifyRetentionPeriod(t, db, "TEST", 6*time.Hour)

		//set on a metric that doesn't exist should create the metric and set the parameter
		_, err = db.Exec(context.Background(), "SELECT prom_api.set_metric_retention_period('prom_data', 'test_new_metric1', INTERVAL '7 hours')")
		if err != nil {
			t.Error(err)
		}
		verifyRetentionPeriod(t, db, "test_new_metric1", 7*time.Hour)

		_, err = db.Exec(context.Background(), "SELECT prom_api.set_default_retention_period(INTERVAL '2 hours')")
		if err != nil {
			t.Error(err)
		}

		verifyRetentionPeriod(t, db, "test_new_metric1", 7*time.Hour)

		//get on non-existing metric returns default
		verifyRetentionPeriod(t, db, "test_new_metric2", 2*time.Hour)
	})
}

func verifyRetentionPeriod(t testing.TB, db *pgxpool.Pool, metricName string, expectedDuration time.Duration) {
	verifyRetentionPeriodWithSchema(t, db, "prom_data", metricName, expectedDuration)
}

func verifyRetentionPeriodWithSchema(t testing.TB, db *pgxpool.Pool, schemaName string, metricName string, expectedDuration time.Duration) {
	var durS int
	var dur time.Duration

	err := db.QueryRow(context.Background(),
		`SELECT EXTRACT(epoch FROM _prom_catalog.get_metric_retention_period($1, $2))`, schemaName,
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
		dbJob := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_maintenance")
		defer dbJob.Close()
		//a chunk way back in 2009
		chunkEnds := time.Date(2009, time.November, 11, 0, 0, 0, 0, time.UTC)

		ts := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: pgmodel.MetricNameLabelName, Value: "test"},
					{Name: "name1", Value: "value1"},
				},
				Samples: []prompb.Sample{
					{Timestamp: int64(model.TimeFromUnixNano(chunkEnds.UnixNano()) - 1), Value: 0.1},
					{Timestamp: int64(model.TimeFromUnixNano(time.Now().UnixNano()) - 1), Value: 0.1},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: pgmodel.MetricNameLabelName, Value: "test2"},
					{Name: "name1", Value: "value1"},
				},
				Samples: []prompb.Sample{
					{Timestamp: int64(model.TimeFromUnixNano(time.Now().UnixNano()) - 1), Value: 0.1},
				},
			},
		}
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()
		_, _, err = ingestor.IngestMetrics(context.Background(), newWriteRequestWithTs(copyMetrics(ts)))
		if err != nil {
			t.Error(err)
		}

		var tableName string
		err = db.QueryRow(context.Background(), "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists('prom_data', 'test')").Scan(&tableName)
		if err != nil {
			t.Fatal(err)
		}

		cnt := 0
		err = db.QueryRow(context.Background(), fmt.Sprintf(`SELECT count(*) FROM public.show_chunks('prom_data."%s"')`, tableName)).Scan(&cnt)
		if err != nil {
			t.Fatal(err)
		}
		if cnt != 2 {
			t.Errorf("Expected there to be a chunk")
		}

		_, err = dbJob.Exec(context.Background(), "CALL prom_api.execute_maintenance(log_verbose=>true)")
		if err != nil {
			t.Fatal(err)
		}
		err = db.QueryRow(context.Background(), fmt.Sprintf(`SELECT count(*) FROM public.show_chunks('prom_data."%s"')`, tableName)).Scan(&cnt)
		if err != nil {
			t.Fatal(err)
		}
		if cnt != 1 {
			t.Errorf("Expected the chunk to be dropped")
		}
		//noop works fine
		_, err = dbJob.Exec(context.Background(), "CALL prom_api.execute_maintenance()")
		if err != nil {
			t.Fatal(err)
		}
		//test2 isn't affected
		err = db.QueryRow(context.Background(), "SELECT count(*) FROM public.show_chunks('prom_data.test2')").Scan(&cnt)
		if err != nil {
			t.Fatal(err)
		}
		if cnt != 1 {
			t.Errorf("Expected the chunk to be dropped")
		}
	})
}

// TestSQLDropChunkWithLocked tests the case where some metrics are locked in the
// first loop of execute_data_retention_policy
func TestSQLDropChunkWithLocked(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		dbJob := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_maintenance")
		defer dbJob.Close()
		dbSuper, err := pgxpool.New(context.Background(), testhelpers.PgConnectURL(*testDatabase, testhelpers.Superuser))
		require.NoError(t, err)
		defer dbSuper.Close()
		//a chunk way back in 2009
		oldChunk1 := time.Date(2009, time.November, 11, 0, 0, 0, 0, time.UTC)

		ts := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: pgmodel.MetricNameLabelName, Value: "test"},
					{Name: "name1", Value: "value1"},
				},
				Samples: []prompb.Sample{
					{Timestamp: int64(model.TimeFromUnixNano(oldChunk1.UnixNano()) - 1), Value: 0.1},
					{Timestamp: int64(model.TimeFromUnixNano(time.Now().UnixNano()) - 1), Value: 0.1},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: pgmodel.MetricNameLabelName, Value: "test2"},
					{Name: "name1", Value: "value1"},
				},
				Samples: []prompb.Sample{
					{Timestamp: int64(model.TimeFromUnixNano(time.Now().UnixNano()) - 1), Value: 0.1},
				},
			},
		}
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()
		_, _, err = ingestor.IngestMetrics(context.Background(), newWriteRequestWithTs(copyMetrics(ts)))
		require.NoError(t, err)

		var tableName string
		var metricId int
		err = db.QueryRow(context.Background(), "SELECT id, table_name FROM _prom_catalog.get_metric_table_name_if_exists('', 'test')").Scan(&metricId, &tableName)
		require.NoError(t, err)

		cnt := 0
		err = db.QueryRow(context.Background(), fmt.Sprintf(`SELECT count(*) FROM public.show_chunks('prom_data."%s"')`, tableName)).Scan(&cnt)
		require.NoError(t, err)
		require.Equal(t, 2, int(cnt))

		//take the lock before executing execute_maintenance
		tx, err := db.Begin(context.Background())
		require.NoError(t, err)
		defer func() {
			_ = tx.Rollback(context.Background())
		}()
		_, err = tx.Exec(context.Background(), "SELECT _prom_catalog.lock_metric_for_maintenance($1)", metricId)
		require.NoError(t, err)

		wg := sync.WaitGroup{}
		wg.Add(1)

		maintenanceQuery := "CALL prom_api.execute_maintenance(log_verbose=>true)"
		go func() {
			defer wg.Done()
			//this will block until the lock is released
			_, err := dbJob.Exec(context.Background(), maintenanceQuery)
			require.NoError(t, err)
		}()

		//wait for the execute_maintenance to be in the second loop where it's waiting on the lock
		found_wait := false
		for i := 0; i < 5; i++ {
			waiting := false
			err = dbSuper.QueryRow(context.Background(),
				"SELECT wait_event IS NOT NULL FROM pg_stat_activity WHERE query = $1", maintenanceQuery).
				Scan(&waiting)
			if err != pgx.ErrNoRows {
				require.NoError(t, err)
			}
			if waiting {
				found_wait = true
				break
			}
			time.Sleep(time.Second)
		}
		require.True(t, found_wait, "failed to find the exec_maintenance query waiting on lock")

		//release the lock to allow execute maintenance to complete
		_, err = tx.Exec(context.Background(), "SELECT _prom_catalog.unlock_metric_for_maintenance($1)", metricId)
		require.NoError(t, err)
		err = tx.Commit(context.Background())
		require.NoError(t, err)

		//make sure the exec_maintenance completes
		wg.Wait()

		err = db.QueryRow(context.Background(), fmt.Sprintf(`SELECT count(*) FROM public.show_chunks('prom_data."%s"')`, tableName)).Scan(&cnt)
		require.NoError(t, err)
		require.Equal(t, 1, int(cnt), "Expected the chunk to be dropped")
	})
}

func TestSQLDropDataWithoutTimescaleDB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		//a chunk way back in 2009
		chunkEnds := time.Date(2009, time.November, 11, 0, 0, 0, 0, time.UTC)

		ts := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: pgmodel.MetricNameLabelName, Value: "test"},
					{Name: "name1", Value: "value1"},
				},
				Samples: []prompb.Sample{
					{Timestamp: int64(model.TimeFromUnixNano(chunkEnds.UnixNano()) - 1), Value: 0.1},
					{Timestamp: int64(model.TimeFromUnixNano(time.Now().UnixNano()) - 1), Value: 0.1},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: pgmodel.MetricNameLabelName, Value: "test2"},
					{Name: "name1", Value: "value1"},
				},
				Samples: []prompb.Sample{
					{Timestamp: int64(model.TimeFromUnixNano(time.Now().UnixNano()) - 1), Value: 0.1},
				},
			},
		}
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()
		_, _, err = ingestor.IngestMetrics(context.Background(), newWriteRequestWithTs(copyMetrics(ts)))
		if err != nil {
			t.Error(err)
		}

		var tableName string
		err = db.QueryRow(context.Background(), "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists('prom_data', 'test')").Scan(&tableName)
		if err != nil {
			t.Fatal(err)
		}

		cnt := 0
		err = db.QueryRow(context.Background(), fmt.Sprintf(`SELECT count(*) FROM prom_data."%s"`, tableName)).Scan(&cnt)
		if err != nil {
			t.Fatal(err)
		}
		if cnt != 2 {
			t.Errorf("Expected there to be a data")
		}

		_, err = db.Exec(context.Background(), "CALL prom_api.execute_maintenance()")
		if err != nil {
			t.Fatal(err)
		}
		err = db.QueryRow(context.Background(), fmt.Sprintf(`SELECT count(*) FROM prom_data."%s"`, tableName)).Scan(&cnt)
		if err != nil {
			t.Fatal(err)
		}
		if cnt != 1 {
			t.Errorf("Expected some data to be dropped")
		}
		//noop works fine
		_, err = db.Exec(context.Background(), "CALL prom_api.execute_maintenance()")
		if err != nil {
			t.Fatal(err)
		}
		//test2 isn't affected
		err = db.QueryRow(context.Background(), "SELECT count(*) FROM prom_data.test2").Scan(&cnt)
		if err != nil {
			t.Fatal(err)
		}
		if cnt != 1 {
			t.Errorf("Expected data left")
		}
	})
}

func TestSQLDropMetricChunk(t *testing.T) {
	t.Skip() // Skip the test for now, since it causes failure in promscale_extension repo CI. More info at https://github.com/timescale/promscale/pull/1484
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		scache := cache.NewSeriesCache(cache.DefaultConfig, nil)
		lcache := cache.NewInvertedLabelsCache(cache.DefaultConfig, nil)
		//this is the range_end of a chunk boundary (exclusive)
		chunkEnds := time.Date(2009, time.November, 11, 0, 0, 0, 0, time.UTC)

		ts := []prompb.TimeSeries{
			{
				//this series will be deleted along with it's label
				Labels: []prompb.Label{
					{Name: pgmodel.MetricNameLabelName, Value: "test"},
					{Name: "name1", Value: "value1"},
				},
				Samples: []prompb.Sample{
					//this will be dropped (notice the - 1)
					{Timestamp: int64(model.TimeFromUnixNano(chunkEnds.UnixNano()) - 1), Value: 0.1},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: pgmodel.MetricNameLabelName, Value: "test"},
					{Name: "name1", Value: "value2"},
				},
				Samples: []prompb.Sample{
					//this will remain after the drop
					{Timestamp: int64(model.TimeFromUnixNano(chunkEnds.UnixNano())), Value: 0.2},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: pgmodel.MetricNameLabelName, Value: "test"},
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
		// Avoid randomness in chunk interval size by setting explicitly.
		_, err := db.Exec(context.Background(), "SELECT _prom_catalog.get_or_create_metric_table_name($1)", "test")
		if err != nil {
			t.Fatal(err)
		}

		_, err = db.Exec(context.Background(), "SELECT public.set_chunk_time_interval('prom_data.test', interval '8 hour')")
		if err != nil {
			t.Fatal(err)
		}

		c := cache.NewMetricCache(cache.DefaultConfig)
		ingestor, err := ingstr.NewPgxIngestor(pgxconn.NewPgxConn(db), c, scache, nil, lcache, &ingstr.Cfg{
			DisableEpochSync:        true,
			InvertedLabelsCacheSize: cache.DefaultConfig.InvertedLabelsCacheSize,
			NumCopiers:              2,
		})
		if err != nil {
			t.Fatal(err)
		}

		_, _, err = ingestor.IngestMetrics(context.Background(), newWriteRequestWithTs(copyMetrics(ts)))
		if err != nil {
			t.Error(err)
		}
		err = ingestor.CompleteMetricCreation(context.Background())
		if err != nil {
			t.Error(err)
		}

		beforeDropCorrect := func(numDataRows int, loc string) {
			count := 0
			err = db.QueryRow(context.Background(), `SELECT count(*) FROM prom_data.test`).Scan(&count)
			if err != nil {
				t.Error(loc, err)
			}
			if count != numDataRows {
				t.Errorf("unexpected row count: %v @ %v", count, loc)
			}

			err = db.QueryRow(context.Background(), `SELECT count(*) FROM _prom_catalog.series`).Scan(&count)
			if err != nil {
				t.Error(loc, err)
			}

			// none of the series should be removed yet
			if count != 3 {
				t.Errorf("unexpected series count: %v @ %v", count, loc)
			}

			err = db.QueryRow(context.Background(), `SELECT count(*) FROM _prom_catalog.series WHERE delete_epoch IS NOT NULL`).Scan(&count)
			if err != nil {
				t.Error(loc, err)
			}

			// none of the series should be marked for deletion
			if count != 0 {
				t.Errorf("unexpected series count: %v @ %v", count, loc)
			}

			err = db.QueryRow(context.Background(), `SELECT count(*) FROM _prom_catalog.label where key='name1'`).Scan(&count)
			if err != nil {
				t.Error(loc, err)
			}

			// none of the labels should deleted yet
			if count != 3 {
				t.Errorf("unexpected labels count: %v @ %v", count, loc)
			}
		}

		beforeDropCorrect(4, "before drop")

		_, err = db.Exec(context.Background(), "CALL _prom_catalog.drop_metric_chunks($1, $2, $3)", "prom_data", "test", chunkEnds.Add(time.Second*5))
		if err != nil {
			t.Fatal(err)
		}

		beforeDeleteCorrect := func(loc string) {
			count := 0
			err = db.QueryRow(context.Background(), `SELECT count(*) FROM prom_data.test`).Scan(&count)
			if err != nil {
				t.Error(loc, err)
			}
			if count != 2 {
				t.Errorf("unexpected row count: %v @ %v", count, loc)
			}

			err = db.QueryRow(context.Background(), `SELECT count(*) FROM _prom_catalog.series`).Scan(&count)
			if err != nil {
				t.Error(loc, err)
			}

			// none of the series should be removed yet
			if count != 3 {
				t.Errorf("unexpected series count: %v @ %v", count, loc)
			}

			err = db.QueryRow(context.Background(), `SELECT count(*) FROM _prom_catalog.series WHERE delete_epoch IS NOT NULL`).Scan(&count)
			if err != nil {
				t.Error(loc, err)
			}

			// one of the series should be marked for deletion
			if count != 1 {
				t.Errorf("unexpected series count: %v @ %v", count, loc)
			}

			err = db.QueryRow(context.Background(), `SELECT count(*) FROM _prom_catalog.label where key='name1'`).Scan(&count)
			if err != nil {
				t.Error(loc, err)
			}

			// none of the labels should deleted yet
			if count != 3 {
				t.Errorf("unexpected labels count: %v @ %v", count, loc)
			}
		}

		beforeDeleteCorrect("after first")

		//rerun again -- nothing changes
		_, err = db.Exec(context.Background(), "CALL _prom_catalog.drop_metric_chunks($1, $2, $3)", "prom_data", "test", chunkEnds.Add(time.Second*5))
		if err != nil {
			t.Fatal(err)
		}
		beforeDeleteCorrect("after first repeat")

		// reruns don't change anything until the dead series are actually dropped
		for i := 0; i < 5; i++ {
			drop := fmt.Sprintf("CALL _prom_catalog.drop_metric_chunks($1, $2, $3, now()+'%v hours')", i)
			_, err = db.Exec(context.Background(), drop, "prom_data", "test", chunkEnds.Add(time.Second*5))
			if err != nil {
				t.Fatal(err)
			}

			beforeDeleteCorrect(fmt.Sprintf("after loop %v", i))
		}

		beforeDeleteCorrect("after all loops")

		afterDeleteCorrect := func(loc string) {
			count := 0
			err = db.QueryRow(context.Background(), `SELECT count(*) FROM prom_data.test`).Scan(&count)
			if err != nil {
				t.Error(loc, err)
			}
			if count != 2 {
				t.Errorf("unexpected row count: %v @ %v", count, loc)
			}

			err = db.QueryRow(context.Background(), `SELECT count(*) FROM _prom_catalog.series`).Scan(&count)
			if err != nil {
				t.Error(loc, err)
			}

			// one of the series should be removed
			if count != 2 {
				t.Errorf("unexpected series count: %v @ %v", count, loc)
			}

			err = db.QueryRow(context.Background(), `SELECT count(*) FROM _prom_catalog.series WHERE delete_epoch IS NOT NULL`).Scan(&count)
			if err != nil {
				t.Error(loc, err)
			}

			// no series should be marked for deletion
			if count != 0 {
				t.Errorf("unexpected series count: %v @ %v", count, loc)
			}

			err = db.QueryRow(context.Background(), `SELECT count(*) FROM _prom_catalog.label where key='name1'`).Scan(&count)
			if err != nil {
				t.Error(loc, err)
			}

			// unused labels should be deleted
			if count != 2 {
				t.Errorf("unexpected labels count: %v @ %v", count, loc)
			}
		}

		for i := 5; i < 10; i++ {
			drop := fmt.Sprintf("CALL _prom_catalog.drop_metric_chunks($1, $2, $3, now()+'%v hours')", i)
			_, err = db.Exec(context.Background(), drop, "prom_data", "test", chunkEnds.Add(time.Second*5))
			if err != nil {
				t.Fatal(err)
			}

			afterDeleteCorrect(fmt.Sprintf("after loop %v", i))
		}

		afterDeleteCorrect("after all loops")

		resurrected := []prompb.TimeSeries{
			{
				//this series will be deleted along with it's label
				Labels: []prompb.Label{
					{Name: pgmodel.MetricNameLabelName, Value: "test"},
					{Name: "name1", Value: "value1"},
				},
				Samples: []prompb.Sample{
					{Timestamp: int64(model.TimeFromUnixNano(chunkEnds.Add(time.Hour * 5).UnixNano())), Value: 0.1},
				},
			},
		}

		_, _, err = ingestor.IngestMetrics(context.Background(), newWriteRequestWithTs(copyMetrics(resurrected)))
		if err == nil {
			t.Error("expected ingest to fail due to old epoch")
		}

		scache.Reset()

		ingestor.Close()
		ingestor2, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor2.Close()

		_, _, err = ingestor2.IngestMetrics(context.Background(), newWriteRequestWithTs(copyMetrics(resurrected)))
		if err != nil {
			t.Error(err)
		}

		beforeDropCorrect(3, "after resurrection")
	})
}

// Tests case that all metric data was dropped and then the metric came back alive
func TestSQLDropAllMetricData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(dbOwner *pgxpool.Pool, t testing.TB) {
		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_modifier")
		defer db.Close()
		dbMaint := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_maintenance")
		defer dbMaint.Close()
		//this is the range_end of a chunk boundary (exclusive)
		chunkEnds := time.Date(2009, time.November, 11, 0, 0, 0, 0, time.UTC)

		ts := []prompb.TimeSeries{
			{
				//this series will be deleted along with it's label
				Labels: []prompb.Label{
					{Name: pgmodel.MetricNameLabelName, Value: "test"},
					{Name: "name1", Value: "value1"},
				},
				Samples: []prompb.Sample{
					//this will be dropped (notice the - 1)
					{Timestamp: int64(model.TimeFromUnixNano(chunkEnds.UnixNano()) - 1), Value: 0.1},
				},
			},
		}
		// Avoid randomness in chunk interval size by setting explicitly.
		_, err := db.Exec(context.Background(), "SELECT _prom_catalog.get_or_create_metric_table_name($1)", "test")
		if err != nil {
			t.Fatal(err)
		}

		//owner not admin since using timescale func to avoid randomness
		_, err = dbOwner.Exec(context.Background(), "SELECT public.set_chunk_time_interval('prom_data.test', interval '8 hour')")
		if err != nil {
			t.Fatal(err)
		}

		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		if err != nil {
			t.Fatal(err)
		}

		_, _, err = ingestor.IngestMetrics(context.Background(), newWriteRequestWithTs(copyMetrics(ts)))
		if err != nil {
			t.Error(err)
		}
		err = ingestor.CompleteMetricCreation(context.Background())
		if err != nil {
			t.Error(err)
		}

		_, err = dbMaint.Exec(context.Background(), "CALL _prom_catalog.drop_metric_chunks($1, $2, $3)", "prom_data", "test", chunkEnds.Add(time.Second*5))
		if err != nil {
			t.Fatal(err)
		}

		count := 0
		err = db.QueryRow(context.Background(), `SELECT count(*) FROM prom_data.test`).Scan(&count)
		if err != nil {
			t.Error(err)
		}

		if count != 0 {
			t.Errorf("unexpected row count: %v", count)
		}

		err = db.QueryRow(context.Background(), `SELECT count(*) FROM _prom_catalog.series`).Scan(&count)
		if err != nil {
			t.Error(err)
		}

		if count != 1 {
			t.Errorf("unexpected series count: %v", count)
		}

		err = db.QueryRow(context.Background(), `SELECT count(*) FROM _prom_catalog.series WHERE delete_epoch IS NOT NULL`).Scan(&count)
		if err != nil {
			t.Error(err)
		}

		if count != 1 {
			t.Errorf("unexpected series count: %v", count)
		}

		err = db.QueryRow(context.Background(), `SELECT count(*) FROM _prom_catalog.label`).Scan(&count)
		if err != nil {
			t.Error(err)
		}

		if count != 2 {
			t.Errorf("unexpected label count: %v", count)
		}

		ts = []prompb.TimeSeries{
			{
				//this series will be deleted along with it's label
				Labels: []prompb.Label{
					{Name: pgmodel.MetricNameLabelName, Value: "test"},
					{Name: "name1", Value: "value1"},
				},
				Samples: []prompb.Sample{
					//this will remain after the drop
					{Timestamp: int64(model.TimeFromUnixNano(chunkEnds.UnixNano())), Value: 0.2},
				},
			},
		}

		//Restart ingestor to avoid stale cache issues.
		//Other tests should check for that
		ingestor.Close()
		ingestor2, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		if err != nil {
			t.Fatal(err)
		}

		defer ingestor2.Close()
		_, _, err = ingestor2.IngestMetrics(context.Background(), newWriteRequestWithTs(copyMetrics(ts)))
		if err != nil {
			t.Fatal(err)
		}

		err = db.QueryRow(context.Background(), `SELECT count(*) FROM prom_data.test`).Scan(&count)
		if err != nil {
			t.Error(err)
		}

		if count != 1 {
			t.Errorf("unexpected row count: %v", count)
		}

		err = db.QueryRow(context.Background(), `SELECT count(*) FROM _prom_catalog.series`).Scan(&count)
		if err != nil {
			t.Error(err)
		}

		if count != 1 {
			t.Errorf("unexpected series count: %v", count)
		}

		err = db.QueryRow(context.Background(), `SELECT count(*) FROM _prom_catalog.series WHERE delete_epoch IS NOT NULL`).Scan(&count)
		if err != nil {
			t.Error(err)
		}

		if count != 0 {
			t.Errorf("unexpected series count: %v", count)
		}
	})
}
