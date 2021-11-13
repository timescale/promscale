package end_to_end_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/lreader"
	pgmodel "github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgmodel/querier"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/promql"
	"github.com/timescale/promscale/pkg/query"
)

func TestContinuousAggDownsampling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	if !*useTimescaleDB {
		t.Skip("continuous aggregates need TimescaleDB support")
	}
	if *useTimescaleOSS {
		t.Skip("continuous aggregates need non-OSS version of TimescaleDB")
	}
	if *useMultinode {
		t.Skip("continuous aggregates not supported in multinode TimescaleDB setup")
	}

	testCases := []struct {
		name    string
		query   string
		startMs int64
		endMs   int64
		stepMs  int64
		res     promql.Result
	}{
		{
			name:    "Query non-existant column, empty result",
			query:   `cagg{__column__="nonexistant"}`,
			startMs: startTime,
			endMs:   endTime,
			stepMs:  360 * 1000,
			res: promql.Result{
				Value: promql.Matrix{},
			},
		},
		{
			name:    "Query default column",
			query:   `cagg{instance="1"}`,
			startMs: startTime,
			endMs:   startTime + 4*3600*1000 - 1, // -1ms to exclude fifth value
			stepMs:  3600 * 1000,
			res: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Metric: labels.Labels{
							labels.Label{Name: "__name__", Value: "cagg"},
							labels.Label{Name: "__schema__", Value: "cagg_schema"},
							labels.Label{Name: "foo", Value: "bat"},
							labels.Label{Name: "instance", Value: "1"},
						},
						Points: []promql.Point{
							promql.Point{T: 1577836800000, V: 952},
							promql.Point{T: 1577840400000, V: 1912},
							promql.Point{T: 1577844000000, V: 2872},
							promql.Point{T: 1577847600000, V: 3832},
						},
					},
				},
			},
		},
		{
			name:    "Query max column",
			query:   `cagg{__column__="max",instance="1"}`,
			startMs: startTime,
			endMs:   startTime + 4*3600*1000 - 1, // -1ms to exclude fifth value
			stepMs:  3600 * 1000,
			res: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Metric: labels.Labels{
							labels.Label{Name: "__column__", Value: "max"},
							labels.Label{Name: "__name__", Value: "cagg"},
							labels.Label{Name: "__schema__", Value: "cagg_schema"},
							labels.Label{Name: "foo", Value: "bat"},
							labels.Label{Name: "instance", Value: "1"},
						},
						Points: []promql.Point{
							promql.Point{T: 1577836800000, V: 952},
							promql.Point{T: 1577840400000, V: 1912},
							promql.Point{T: 1577844000000, V: 2872},
							promql.Point{T: 1577847600000, V: 3832},
						},
					},
				},
			},
		},
		{
			name:    "Query min column",
			query:   `cagg{__column__="min",instance="1"}`,
			startMs: startTime,
			endMs:   startTime + 4*3600*1000 - 1, // -1ms to exclude fifth value
			stepMs:  3600 * 1000,
			res: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Metric: labels.Labels{
							labels.Label{Name: "__column__", Value: "min"},
							labels.Label{Name: "__name__", Value: "cagg"},
							labels.Label{Name: "__schema__", Value: "cagg_schema"},
							labels.Label{Name: "foo", Value: "bat"},
							labels.Label{Name: "instance", Value: "1"},
						},
						Points: []promql.Point{
							promql.Point{T: 1577836800000, V: 0},
							promql.Point{T: 1577840400000, V: 960},
							promql.Point{T: 1577844000000, V: 1920},
							promql.Point{T: 1577847600000, V: 2880},
						},
					},
				},
			},
		},
		{
			name:    "Query avg column",
			query:   `cagg{__column__="avg",instance="1"}`,
			startMs: startTime,
			endMs:   startTime + 4*3600*1000 - 1, // -1ms to exclude fifth value
			stepMs:  3600 * 1000,
			res: promql.Result{
				Value: promql.Matrix{
					promql.Series{
						Metric: labels.Labels{
							labels.Label{Name: "__column__", Value: "avg"},
							labels.Label{Name: "__name__", Value: "cagg"},
							labels.Label{Name: "__schema__", Value: "cagg_schema"},
							labels.Label{Name: "foo", Value: "bat"},
							labels.Label{Name: "instance", Value: "1"},
						},
						Points: []promql.Point{
							promql.Point{T: 1577836800000, V: 476},
							promql.Point{T: 1577840400000, V: 1436},
							promql.Point{T: 1577844000000, V: 2396},
							promql.Point{T: 1577847600000, V: 3356},
						},
					},
				},
			},
		},
	}

	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		ts := []prompb.TimeSeries{
			{
				// This series will be deleted along with it's label once the samples
				// have been deleted from raw metric and cagg.
				Labels: []prompb.Label{
					{Name: pgmodel.MetricNameLabelName, Value: "metric_2"},
					{Name: "name1", Value: "value1"},
				},
				Samples: []prompb.Sample{
					//this will be dropped (notice the - 1000)
					{Timestamp: startTime - 1000, Value: 0.1},
				},
			},
		}

		// Ingest test dataset.
		ingestQueryTestDataset(db, t, append(generateLargeTimeseries(), ts...))

		if _, err := db.Exec(context.Background(), "CALL _prom_catalog.finalize_metric_creation()"); err != nil {
			t.Fatalf("unexpected error while ingesting test dataset: %s", err)
		}
		if _, err := db.Exec(context.Background(), "CREATE SCHEMA cagg_schema"); err != nil {
			t.Fatalf("unexpected error while creating view schema: %s", err)
		}
		if *useTimescale2 {
			if _, err := db.Exec(context.Background(),
				`CREATE MATERIALIZED VIEW cagg_schema.cagg( time, series_id, value, max, min, avg)
WITH (timescaledb.continuous) AS
  SELECT time_bucket('1hour', time), series_id, max(value) as value, max(value) as max, min(value) as min, avg(value) as avg
    FROM prom_data.metric_2
    GROUP BY time_bucket('1hour', time), series_id`); err != nil {
				t.Fatalf("unexpected error while creating metric view: %s", err)
			}
		} else {
			// Using TimescaleDB 1.x
			if _, err := db.Exec(context.Background(),
				`CREATE VIEW cagg_schema.cagg( time, series_id, value, max, min, avg)
WITH (timescaledb.continuous,  timescaledb.ignore_invalidation_older_than = '1 min', timescaledb.refresh_lag = '-2h') AS
  SELECT time_bucket('1hour', time), series_id, max(value) as value, max(value) as max, min(value) as min, avg(value) as avg
    FROM prom_data.metric_2
    GROUP BY time_bucket('1hour', time), series_id`); err != nil {
				t.Fatalf("unexpected error while creating metric view: %s", err)
			}
			if _, err := db.Exec(context.Background(),
				`REFRESH MATERIALIZED VIEW cagg_schema.cagg`); err != nil {
				t.Fatalf("unexpected error while creating metric view: %s", err)
			}
		}
		if _, err := db.Exec(context.Background(), "SELECT prom_api.register_metric_view('cagg_schema', 'cagg')"); err != nil {
			t.Fatalf("unexpected error while registering metric view: %s", err)
		}

		// Getting a read-only connection to ensure read path is idempotent.
		readOnly := testhelpers.GetReadOnlyConnection(t, *testDatabase)
		defer readOnly.Close()

		var tester *testing.T
		var ok bool
		if tester, ok = t.(*testing.T); !ok {
			t.Fatalf("Cannot run test, not an instance of testing.T")
			return
		}

		mCache := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
		lCache := clockcache.WithMax(100)
		dbConn := pgxconn.NewPgxConn(readOnly)
		labelsReader := lreader.NewLabelsReader(dbConn, lCache)
		r := querier.NewQuerier(dbConn, mCache, labelsReader, nil, nil)
		queryable := query.NewQueryable(r, labelsReader)
		queryEngine, err := query.NewEngine(log.GetLogger(), time.Minute, time.Minute*5, time.Minute, 50000000, []string{})
		if err != nil {
			t.Fatal(err)
		}

		for _, c := range testCases {
			tc := c
			tester.Run(c.name, func(t *testing.T) {
				var qry promql.Query
				var err error

				if tc.stepMs == 0 {
					qry, err = queryEngine.NewInstantQuery(queryable, c.query, model.Time(tc.endMs).Time())
				} else {
					qry, err = queryEngine.NewRangeQuery(queryable, tc.query, model.Time(tc.startMs).Time(), model.Time(tc.endMs).Time(), time.Duration(tc.stepMs)*time.Millisecond)
				}
				if err != nil {
					t.Fatal(err)
				}

				res := qry.Exec(context.Background())
				require.Equal(t, tc.res, *res)
			})
		}

		count := 0
		err = db.QueryRow(context.Background(), `SELECT count(*) FROM prom_data.metric_2`).Scan(&count)
		if err != nil {
			t.Error("error fetching count of raw metric timeseries", err)
		}

		seriesCount := 0
		err = db.QueryRow(context.Background(), `SELECT count(*) FROM _prom_catalog.series`).Scan(&seriesCount)
		if err != nil {
			t.Error("error fetching series count:", err)
		}

		// Drop some raw metric data and check that the series data is not marked for deletion.
		// NOTE: we cannot drop all the raw data becuase we are getting `too far behind` issues with 1.x caggs
		_, err = db.Exec(context.Background(), "CALL _prom_catalog.drop_metric_chunks($1, $2, $3)", schema.Data, "metric_2", time.Unix(endTime/1000-20000, 0))
		if err != nil {
			t.Fatalf("unexpected error while dropping metric chunks: %s", err)
		}

		afterCount := 0
		err = db.QueryRow(context.Background(), `SELECT count(*) FROM prom_data.metric_2`).Scan(&afterCount)
		if err != nil {
			t.Error("error fetching count of raw metric timeseries", err)
		}
		if afterCount >= count {
			t.Errorf("unexpected row count: got %v, wanted less then %v", afterCount, count)
		}

		err = db.QueryRow(context.Background(), `SELECT count(*) FROM _prom_catalog.series`).Scan(&afterCount)
		if err != nil {
			t.Error("error fetching series count after drop:", err)
		}

		// None of the series should be removed.
		if afterCount != seriesCount {
			t.Errorf("unexpected series count: got %v, wanted %v", afterCount, seriesCount)
		}

		err = db.QueryRow(context.Background(), `SELECT count(*) FROM _prom_catalog.series WHERE delete_epoch IS NOT NULL`).Scan(&count)
		if err != nil {
			t.Error("error fetching series marked for deletion count", err)
		}

		// None of the series should be marked for deletion.
		if count != 0 {
			t.Errorf("unexpected series count: got %v, wanted 0", count)
		}

		// Count cagg rows before dropping.
		err = db.QueryRow(context.Background(), `SELECT count(*) FROM cagg_schema.cagg`).Scan(&count)
		if err != nil {
			t.Error("error fetching count of raw metric timeseries", err)
		}

		// Drop all of the cagg data.
		_, err = db.Exec(context.Background(), "CALL _prom_catalog.drop_metric_chunks($1, $2, $3)", "cagg_schema", "cagg", time.Now())
		if err != nil {
			t.Fatalf("unexpected error while dropping cagg chunks: %s", err)
		}

		err = db.QueryRow(context.Background(), `SELECT count(*) FROM cagg_schema.cagg`).Scan(&afterCount)
		if err != nil {
			t.Error("error fetching count of cagg metric timeseries", err)
		}
		if afterCount >= count {
			t.Errorf("unexpected row count: got %v, wanted less then %v", afterCount, count)
		}

		err = db.QueryRow(context.Background(), `SELECT count(*) FROM _prom_catalog.series`).Scan(&afterCount)
		if err != nil {
			t.Error("error fetching series count after drop:", err)
		}

		// None of the series should be removed.
		if afterCount != seriesCount {
			t.Errorf("unexpected series count: got %v, wanted %v", afterCount, seriesCount)
		}

		err = db.QueryRow(context.Background(), `SELECT count(*) FROM _prom_catalog.series WHERE delete_epoch IS NOT NULL`).Scan(&count)
		if err != nil {
			t.Error("error fetching series marked for deletion count", err)
		}

		// Now that the raw metric and cagg metric have been deleted, one series that was completely
		// deleted will be marked for deletion.
		if count != 1 {
			t.Errorf("unexpected series count: got %v, wanted 1", count)
		}
	})
}

func TestContinuousAggDataRetention(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	if !*useTimescaleDB {
		t.Skip("continuous aggregates need TimescaleDB support")
	}
	if *useTimescaleOSS {
		t.Skip("continuous aggregates need non-OSS version of TimescaleDB")
	}
	if *useMultinode {
		t.Skip("continuous aggregates not supported in multinode TimescaleDB setup")
	}

	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		dbJob := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_maintenance")
		defer dbJob.Close()
		dbSuper, err := pgxpool.Connect(context.Background(), testhelpers.PgConnectURL(*testDatabase, testhelpers.Superuser))
		require.NoError(t, err)
		defer dbSuper.Close()
		//a chunk way back in 2009
		oldChunk := time.Date(2009, time.November, 11, 0, 0, 0, 0, time.UTC)
		chunkInRetentionPolicy := time.Now().Add(-100 * 24 * time.Hour)

		ts := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: pgmodel.MetricNameLabelName, Value: "test"},
					{Name: "name1", Value: "value1"},
				},
				Samples: []prompb.Sample{
					{Timestamp: int64(model.TimeFromUnixNano(oldChunk.UnixNano()) - 1), Value: 0.1},
					{Timestamp: int64(model.TimeFromUnixNano(chunkInRetentionPolicy.UnixNano()) - 1), Value: 0.1},
					{Timestamp: int64(model.TimeFromUnixNano(time.Now().UnixNano()) - 1), Value: 0.1},
				},
			},
		}
		ingestQueryTestDataset(db, t, ts)

		_, err = db.Exec(context.Background(), "CALL _prom_catalog.finalize_metric_creation()")
		require.NoError(t, err)
		_, err = db.Exec(context.Background(), "CREATE SCHEMA cagg_schema")
		require.NoError(t, err)

		if *useTimescale2 {
			_, err = db.Exec(context.Background(),
				`CREATE MATERIALIZED VIEW cagg_schema.cagg( time, series_id, value, max, min, avg)
WITH (timescaledb.continuous) AS
  SELECT time_bucket('1hour', time), series_id, max(value) as value, max(value) as max, min(value) as min, avg(value) as avg
    FROM prom_data.test
    GROUP BY time_bucket('1hour', time), series_id`)
			require.NoError(t, err)
		} else {
			// Using TimescaleDB 1.x
			_, err = db.Exec(context.Background(),
				`CREATE VIEW cagg_schema.cagg( time, series_id, value, max, min, avg)
WITH (timescaledb.continuous, timescaledb.max_interval_per_job = '1000 weeks', timescaledb.ignore_invalidation_older_than = '1 min') AS
  SELECT time_bucket('1hour', time), series_id, max(value) as value, max(value) as max, min(value) as min, avg(value) as avg
    FROM prom_data.test
    GROUP BY time_bucket('1hour', time), series_id`)
			require.NoError(t, err)
			_, err = db.Exec(context.Background(), `REFRESH MATERIALIZED VIEW cagg_schema.cagg`)
			require.NoError(t, err)
		}
		_, err = db.Exec(context.Background(), "SELECT prom_api.register_metric_view('cagg_schema', 'cagg')")
		require.NoError(t, err)

		_, err = db.Exec(context.Background(), "SELECT prom_api.set_metric_retention_period('cagg_schema', 'cagg', INTERVAL '180 days')")
		require.NoError(t, err)

		caggHypertable := ""
		err = db.QueryRow(context.Background(), "SELECT hypertable_relation FROM _prom_catalog.get_storage_hypertable_info('cagg_schema', 'cagg', true)").Scan(&caggHypertable)
		require.NoError(t, err)

		cnt := 0
		err = db.QueryRow(context.Background(), fmt.Sprintf(`SELECT count(*) FROM public.show_chunks('%s', older_than => NOW())`, caggHypertable)).Scan(&cnt)
		require.NoError(t, err)
		require.Equal(t, 2, int(cnt), "Expected for cagg to have exactly 2 chunks")

		_, err = dbJob.Exec(context.Background(), "CALL prom_api.execute_maintenance(log_verbose=>true)")
		require.NoError(t, err)

		err = db.QueryRow(context.Background(), fmt.Sprintf(`SELECT count(*) FROM public.show_chunks('%s', older_than => NOW())`, caggHypertable)).Scan(&cnt)
		require.NoError(t, err)
		require.Equal(t, 1, int(cnt), "Expected for cagg to have exactly 1 chunk that is outside of 180 day retention period previously set for this metric")

		_, err = db.Exec(context.Background(), "SELECT prom_api.reset_metric_retention_period('cagg_schema', 'cagg')")
		require.NoError(t, err)

		_, err = dbJob.Exec(context.Background(), "CALL prom_api.execute_maintenance(log_verbose=>true)")
		require.NoError(t, err)

		err = db.QueryRow(context.Background(), fmt.Sprintf(`SELECT count(*) FROM public.show_chunks('%s', older_than => NOW())`, caggHypertable)).Scan(&cnt)
		require.NoError(t, err)
		require.Equal(t, 0, int(cnt), "Expected for cagg to have exactly 0 chunks since only data left in default retention period is too new to materialize")
	})
}

func TestContinuousAgg2StepAgg(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	if !*useTimescale2 {
		t.Skip("2-step continuous aggregates need TimescaleDB 2.x support")
	}
	if !*useExtension {
		t.Skip("2-step continuous aggregates need TimescaleDB 2.x HA image")
	}
	if *useTimescaleOSS {
		t.Skip("continuous aggregates need non-OSS version of TimescaleDB")
	}
	if *useMultinode {
		t.Skip("continuous aggregates not supported in multinode TimescaleDB setup")
	}

	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		dbJob := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_maintenance")
		defer dbJob.Close()
		dbSuper, err := pgxpool.Connect(context.Background(), testhelpers.PgConnectURL(*testDatabase, testhelpers.Superuser))
		require.NoError(t, err)
		defer dbSuper.Close()
		_, err = dbSuper.Exec(context.Background(), "CREATE EXTENSION timescaledb_toolkit")
		require.NoError(t, err)

		// Ingest test dataset.
		ingestQueryTestDataset(db, t, generateLargeTimeseries())

		if _, err := db.Exec(context.Background(), "CALL _prom_catalog.finalize_metric_creation()"); err != nil {
			t.Fatalf("unexpected error while ingesting test dataset: %s", err)
		}
		if _, err := db.Exec(context.Background(),
			`CREATE MATERIALIZED VIEW twa_cagg( time, series_id, tw)
WITH (timescaledb.continuous) AS
  SELECT time_bucket('1hour', time), series_id, time_weight('Linear', time, value) as tw
    FROM prom_data.metric_2
    GROUP BY time_bucket('1hour', time), series_id`); err != nil {
			t.Fatalf("unexpected error while creating metric view: %s", err)
		}
		if _, err := db.Exec(context.Background(),
			`CREATE VIEW tw_1hour( time, series_id, value) AS
  SELECT time, series_id, average(tw) as value
    FROM twa_cagg`); err != nil {
			t.Fatalf("unexpected error while creating metric view: %s", err)
		}

		if _, err := db.Exec(context.Background(), "SELECT prom_api.register_metric_view('public', 'tw_1hour')"); err != nil {
			t.Fatalf("unexpected error while registering metric view: %s", err)
		}

		caggHypertable := ""
		err = db.QueryRow(context.Background(), "SELECT hypertable_relation FROM _prom_catalog.get_storage_hypertable_info('public', 'tw_1hour', true)").Scan(&caggHypertable)
		require.NoError(t, err)

		cnt := 0
		err = db.QueryRow(context.Background(), fmt.Sprintf(`SELECT count(*) FROM public.show_chunks('%s', older_than => NOW())`, caggHypertable)).Scan(&cnt)
		require.NoError(t, err)
		require.Greater(t, int(cnt), 0, "Expected for cagg to have at least one chunk")

		_, err = dbJob.Exec(context.Background(), "CALL prom_api.execute_maintenance(log_verbose=>true)")
		require.NoError(t, err)
		err = db.QueryRow(context.Background(), fmt.Sprintf(`SELECT count(*) FROM public.show_chunks('%s', older_than => NOW())`, caggHypertable)).Scan(&cnt)
		require.NoError(t, err)
		require.Equal(t, 0, int(cnt), "Expected for cagg to have no chunks, all outside of data retention period")
	})
}

func TestContinuousAgg2StepAggCounter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	if !*useTimescale2 {
		t.Skip("2-step continuous aggregates need TimescaleDB 2.x support")
	}
	if !*useExtension {
		t.Skip("2-step continuous aggregates need TimescaleDB 2.x HA image")
	}
	if *useTimescaleOSS {
		t.Skip("continuous aggregates need non-OSS version of TimescaleDB")
	}
	if *useMultinode {
		t.Skip("continuous aggregates not supported in multinode TimescaleDB setup")
	}

	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		dbJob := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_maintenance")
		defer dbJob.Close()
		dbSuper, err := pgxpool.Connect(context.Background(), testhelpers.PgConnectURL(*testDatabase, testhelpers.Superuser))
		require.NoError(t, err)
		defer dbSuper.Close()
		_, err = dbSuper.Exec(context.Background(), "CREATE EXTENSION timescaledb_toolkit")
		require.NoError(t, err)

		// Ingest test dataset.
		ingestQueryTestDataset(db, t, generateLargeTimeseries())

		if _, err := db.Exec(context.Background(), "CALL _prom_catalog.finalize_metric_creation()"); err != nil {
			t.Fatalf("unexpected error while ingesting test dataset: %s", err)
		}
		if _, err := db.Exec(context.Background(),
			`
CREATE MATERIALIZED VIEW counter_cagg
WITH (timescaledb.continuous) AS
  SELECT
  	time_bucket('1hour', time) as time,
	series_id as series_id,
	toolkit_experimental.counter_agg(time, value, toolkit_experimental.time_bucket_range('1 hour'::interval, time)) as aggregated_counter
  FROM prom_data.metric_counter
  GROUP BY time_bucket('1hour', time), series_id
    `); err != nil {
			t.Fatalf("unexpected error while creating metric view: %s", err)
		}
		if _, err := db.Exec(context.Background(),
			`
set timescaledb_toolkit_acknowledge_auto_drop to 'true';
CREATE VIEW counter_1hour AS
  SELECT time + '1 hour' as time,
         series_id as series_id,
	 toolkit_experimental.extrapolated_rate(aggregated_counter, 'prometheus') as value,
	 toolkit_experimental.extrapolated_delta(aggregated_counter, 'prometheus') as increase,
	 toolkit_experimental.extrapolated_rate(aggregated_counter, method => 'prometheus') as rate,
	 toolkit_experimental.irate_right(aggregated_counter) as irate,
	 toolkit_experimental.num_resets(aggregated_counter)::float as resets
    FROM counter_cagg`); err != nil {
			t.Fatalf("unexpected error while creating metric view: %s", err)
		}
		if _, err := db.Exec(context.Background(), "SELECT _prom_catalog.register_metric_view('public', 'counter_1hour')"); err != nil {
			t.Fatalf("unexpected error while registering metric view: %s", err)
		}
		if _, err := db.Exec(context.Background(),
			`
		set timescaledb_toolkit_acknowledge_auto_drop to 'true';
		CREATE VIEW counter_3hour AS
		  SELECT time_bucket('3hour', time) + '3 hour' as time,
			 series_id as series_id,
			 toolkit_experimental.extrapolated_rate(
			   toolkit_experimental.with_bounds(
			     toolkit_experimental.rollup(aggregated_counter),
			     toolkit_experimental.time_bucket_range('3 hour'::interval, time_bucket('3hour', time))
			   )
			   , 'prometheus') as value,
			 toolkit_experimental.extrapolated_delta(
			   toolkit_experimental.with_bounds(
			     toolkit_experimental.rollup(aggregated_counter),
			     toolkit_experimental.time_bucket_range('3 hour'::interval, time_bucket('3hour', time))
			   )
			   , 'prometheus') as increase,
			 toolkit_experimental.extrapolated_rate(
			   toolkit_experimental.with_bounds(
			     toolkit_experimental.rollup(aggregated_counter),
			     toolkit_experimental.time_bucket_range('3 hour'::interval, time_bucket('3hour', time))
			   )
			   , method => 'prometheus') as rate,
			 toolkit_experimental.irate_right(
			   toolkit_experimental.with_bounds(
			     toolkit_experimental.rollup(aggregated_counter),
			     toolkit_experimental.time_bucket_range('3 hour'::interval, time_bucket('3hour', time))
			   )
			 ) as irate,
			 toolkit_experimental.num_resets(
			   toolkit_experimental.with_bounds(
			     toolkit_experimental.rollup(aggregated_counter),
			     toolkit_experimental.time_bucket_range('3 hour'::interval, time_bucket('3hour', time))
			   )
			)::float as resets
		    FROM counter_cagg
		    GROUP BY time_bucket('3hour', time), series_id
		    `); err != nil {
			t.Fatalf("unexpected error while creating metric view: %s", err)
		}
		if _, err := db.Exec(context.Background(), "SELECT _prom_catalog.register_metric_view('public', 'counter_3hour')"); err != nil {
			t.Fatalf("unexpected error while registering metric view: %s", err)
		}

		cnt := 0
		err = db.QueryRow(context.Background(), "SELECT count(*) FROM counter_1hour").Scan(&cnt)
		require.NoError(t, err)
		require.Greater(t, cnt, 0)

	})

	// Getting a read-only connection to ensure read path is idempotent.
	readOnly := testhelpers.GetReadOnlyConnection(t, *testDatabase)
	defer readOnly.Close()

	mCache := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
	lCache := clockcache.WithMax(100)
	//dbConn := pgxconn.NewQueryLoggingPgxConn(readOnly)
	dbConn := pgxconn.NewPgxConn(readOnly)
	labelsReader := lreader.NewLabelsReader(dbConn, lCache)
	r := querier.NewQuerier(dbConn, mCache, labelsReader, nil, nil)
	queryable := query.NewQueryable(r, labelsReader)
	queryEngine, err := query.NewEngine(log.GetLogger(), time.Minute, time.Minute*5, time.Minute, 50000000, []string{})
	if err != nil {
		t.Fatal(err)
	}

	runSuite(t, queryEngine, queryable, "metric_counter", "counter_1hour", time.Hour)
	runSuite(t, queryEngine, queryable, "metric_counter", "counter_3hour", 3*time.Hour)
}

func runSuite(t *testing.T, queryEngine *promql.Engine, queryable promql.Queryable, raw_metric, cagg_metric string, duration time.Duration) {

	durationMs := duration.Milliseconds()
	/* subtract a millisecond from Prom since its inclusive on BOTH ends */
	durationPromString := fmt.Sprintf("%vms", (duration - time.Millisecond).Milliseconds())
	testCases := []struct {
		name      string
		queryRaw  string
		queryCagg string
	}{
		{
			name:      "basic rate on default column",
			queryRaw:  fmt.Sprintf("rate(%s[%s])", raw_metric, durationPromString),
			queryCagg: cagg_metric,
		},
		{
			name:      "rate",
			queryRaw:  fmt.Sprintf("rate(%s[%s])", raw_metric, durationPromString),
			queryCagg: fmt.Sprintf(`%s{__column__="rate"}`, cagg_metric),
		},
		{
			name:      "irate",
			queryRaw:  fmt.Sprintf("irate(%s[%s])", raw_metric, durationPromString),
			queryCagg: fmt.Sprintf(`%s{__column__="irate"}`, cagg_metric),
		},
		{
			name:      "resets",
			queryRaw:  fmt.Sprintf("resets(%s[%s])", raw_metric, durationPromString),
			queryCagg: fmt.Sprintf(`%s{__column__="resets"}`, cagg_metric),
		},
		{
			name:      "increase",
			queryRaw:  fmt.Sprintf("increase(%s[%s])", raw_metric, durationPromString),
			queryCagg: fmt.Sprintf(`%s{__column__="increase"}`, cagg_metric),
		},
	}

	for _, c := range testCases {
		tc := c
		times := map[string]int64{
			"end of last full period": ((endTime / durationMs) * durationMs),
			"last period":             ((endTime / durationMs) * durationMs) + durationMs,
			"first period":            ((startTime / durationMs) * durationMs) + durationMs,
			"before first period":     ((startTime / durationMs) * durationMs),
		}
		for desc, endMs := range times {
			t.Run(cagg_metric+" "+c.name+" "+desc, func(t *testing.T) {
				var qryRaw promql.Query
				var qryCagg promql.Query
				var err error

				qryRaw, err = queryEngine.NewInstantQuery(queryable, tc.queryRaw, model.Time(endMs-1).Time()) /* note the -1. Prom is inclusive on both ends, so we use a duration = range - 1 ms to mimick bucketing behavior and start it one millisecond before */
				require.NoError(t, err)
				qryCagg, err = queryEngine.NewInstantQuery(queryable, tc.queryCagg, model.Time(endMs).Time())
				require.NoError(t, err)

				resRaw := qryRaw.Exec(context.Background())
				resCagg := qryCagg.Exec(context.Background())

				vec, err := resCagg.Vector()
				if err == nil {
					for i := range vec {
						sample := vec[i]
						vec[i].Metric = sample.Metric.WithoutLabels("__column__", "__name__", "__schema__")
						vec[i].Point.T = sample.Point.T - 1 /* see note about subtracting 1ms above */
					}
				}
				require.Equal(t, *resRaw, *resCagg)
			})
		}
	}
}
