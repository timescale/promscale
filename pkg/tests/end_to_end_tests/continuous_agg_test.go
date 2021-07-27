package end_to_end_tests

import (
	"context"
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
	"github.com/timescale/promscale/pkg/pgmodel/lreader"
	"github.com/timescale/promscale/pkg/pgmodel/querier"
	"github.com/timescale/promscale/pkg/pgxconn"
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
		// Ingest test dataset.
		ingestQueryTestDataset(db, t, generateLargeTimeseries())

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
		if _, err := db.Exec(context.Background(), "SELECT _prom_catalog.register_metric_view('cagg_schema', 'cagg')"); err != nil {
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
		r := querier.NewQuerier(dbConn, mCache, labelsReader, nil)
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
		_, err = db.Exec(context.Background(), "CALL _prom_catalog.drop_metric_chunks($1, $2)", "metric_2", time.Unix(endTime/1000-20000, 0))
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

		// none of the series should be removed
		if afterCount != seriesCount {
			t.Errorf("unexpected series count: got %v, wanted %v", afterCount, seriesCount)
		}

		err = db.QueryRow(context.Background(), `SELECT count(*) FROM _prom_catalog.series WHERE delete_epoch IS NOT NULL`).Scan(&count)
		if err != nil {
			t.Error("error fetching series marked for deletion count", err)
		}

		// none of the series should be marked for deletion
		if count != 0 {
			t.Errorf("unexpected series count: got %v, wanted 0", count)
		}
	})
}
