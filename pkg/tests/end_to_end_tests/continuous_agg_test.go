package end_to_end_tests

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
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
				`CREATE MATERIALIZED VIEW cagg_schema.cagg( time, series_id, value)
WITH (timescaledb.continuous) AS
  SELECT time_bucket('1hour', time), series_id, max(value) as value
    FROM prom_data.metric_2
    GROUP BY time_bucket('1hour', time), series_id`); err != nil {
				t.Fatalf("unexpected error while creating metric view: %s", err)
			}
		} else {
			// Using TimescaleDB 1.x
			if _, err := db.Exec(context.Background(),
				`CREATE VIEW cagg_schema.cagg( time, series_id, value)
WITH (timescaledb.continuous,  timescaledb.ignore_invalidation_older_than = '1 min', timescaledb.refresh_lag = '-2h') AS
  SELECT time_bucket('1hour', time), series_id, max(value) as value
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

		count := 0
		err := db.QueryRow(context.Background(), `SELECT count(*) FROM prom_data.metric_2`).Scan(&count)
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
