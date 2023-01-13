// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/timescale/promscale/pkg/internal/testhelpers"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/promql"
	"github.com/timescale/promscale/pkg/runner"
	"github.com/timescale/promscale/pkg/telemetry"
)

func generateUUID() uuid.UUID {
	return uuid.New()
}

func setTobsEnv(prop string) error {
	return os.Setenv(fmt.Sprintf("TOBS_TELEMETRY_%s", prop), prop)
}

// e2eTelemetry allows calling Sync() to sync telemetry collection while running tests.
type e2eTelemetry interface {
	telemetry.Engine
	Sync() error
	IsActive() bool
}

func telemetryEngineForE2E(t testing.TB, conn pgxconn.PgxConn, qryable promql.Queryable) e2eTelemetry {
	engine, err := telemetry.NewEngine(conn, generateUUID(), qryable)
	require.NoError(t, err)
	return engine.(e2eTelemetry)
}

func TestPromscaleTobsMetadata(t *testing.T) {
	withDB(t, *testDatabase, func(dbOwner *pgxpool.Pool, t testing.TB) {
		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_writer")
		defer db.Close()

		require.NoError(t, setTobsEnv("random"))
		conn := pgxconn.NewPgxConn(db)

		telemetryEngineForE2E(t, conn, nil)

		// Check if metadata is written.
		var sysName string
		err := conn.QueryRow(context.Background(), "select value from _timescaledb_catalog.metadata where key = 'promscale_os_sys_name'").Scan(&sysName)
		require.NoError(t, err)
		require.NotEqual(t, "", sysName)

		var str string
		err = conn.QueryRow(context.Background(), "select value from _timescaledb_catalog.metadata where key = 'promscale_tobs_telemetry_random'").Scan(&str) // 'promscale_' prefix is added by the promscale_extension.
		require.NoError(t, err)
		require.Equal(t, "random", str)
	})
}

func TestTelemetryInfoTableWrite(t *testing.T) {
	withDB(t, *testDatabase, func(dbOwner *pgxpool.Pool, t testing.TB) {
		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_writer")
		defer db.Close()

		conn := pgxconn.NewPgxConn(db)

		engine := telemetryEngineForE2E(t, conn, nil)
		engine.Start()
		defer engine.Stop()

		mockMetric1 := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "test",
			Name:      "counter",
		})
		mockMetric1.Add(100)
		mockMetric2 := prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "test",
			Name:      "gauge",
		})
		mockMetric2.Set(10)

		require.NoError(t, engine.RegisterMetric("promscale_ingested_samples_total", mockMetric1))
		require.NoError(t, engine.RegisterMetric("promscale_metrics_queries_failed_total", mockMetric2))

		require.NoError(t, engine.Sync())

		var value float64
		err := conn.QueryRow(context.Background(), "SELECT sum(promscale_ingested_samples_total) FROM _ps_catalog.promscale_instance_information").Scan(&value)
		require.NoError(t, err)
		require.Equal(t, float64(100), value)

		err = conn.QueryRow(context.Background(), "SELECT sum(promscale_metrics_queries_failed_total) FROM _ps_catalog.promscale_instance_information").Scan(&value)
		require.NoError(t, err)
		require.Equal(t, float64(10), value)

		mockMetric1.Add(50)
		mockMetric2.Add(5)

		require.NoError(t, engine.Sync())

		err = conn.QueryRow(context.Background(), "SELECT sum(promscale_ingested_samples_total) FROM _ps_catalog.promscale_instance_information").Scan(&value)
		require.NoError(t, err)
		require.Equal(t, float64(150), value)

		err = conn.QueryRow(context.Background(), "SELECT sum(promscale_metrics_queries_failed_total) FROM _ps_catalog.promscale_instance_information").Scan(&value)
		require.NoError(t, err)
		require.Equal(t, float64(15), value)
	})
}

func TestOnlyOneHousekeeper(t *testing.T) {
	withDB(t, *testDatabase, func(dbOwner *pgxpool.Pool, t testing.TB) {
		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_writer")
		defer db.Close()

		conn := pgxconn.NewPgxConn(db)

		// Should error due to unique constraint for counter_reset_row, as there already exists a counter reset row.
		_, err := conn.Exec(context.Background(), `INSERT INTO _ps_catalog.promscale_instance_information (uuid, last_updated, is_counter_reset_row) VALUES ('00000000-0000-0000-0000-000000000000', current_timestamp, TRUE)`)
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), `violates unique constraint "promscale_instance_information_pkey"`))
	})
}

func TestHousekeeper(t *testing.T) {
	withDB(t, *testDatabase, func(dbOwner *pgxpool.Pool, t testing.TB) {
		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_writer")
		defer db.Close()

		conn := pgxconn.NewPgxConn(db)

		engine := telemetryEngineForE2E(t, conn, nil)
		engine.Start()
		defer engine.Stop()

		mockMetric := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "test",
			Name:      "counter",
		})
		require.NoError(t, engine.RegisterMetric("promscale_ingested_samples_total", mockMetric))

		var val string
		err := conn.QueryRow(context.Background(), "select value from _timescaledb_catalog.metadata where key = 'promscale_ingested_samples_total'").Scan(&val)
		require.Error(t, err)
		require.Equal(t, "no rows in result set", err.Error())

		mockMetric.Add(100)

		require.NoError(t, engine.Sync())

		err = conn.QueryRow(context.Background(), "select value from _timescaledb_catalog.metadata where key = 'promscale_ingested_samples_total'").Scan(&val)
		require.NoError(t, err)
		require.Equal(t, "100", val)
	})
}

func TestCleanStalePromscales(t *testing.T) {
	withDB(t, *testDatabase, func(dbOwner *pgxpool.Pool, t testing.TB) {
		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_writer")
		defer db.Close()

		conn := pgxconn.NewPgxConn(db)

		engine := telemetryEngineForE2E(t, conn, nil)
		engine.Start()
		defer engine.Stop()

		var cnt int64
		err := conn.QueryRow(context.Background(), "SELECT count(*) FROM _ps_catalog.promscale_instance_information").Scan(&cnt)
		require.NoError(t, err)
		require.Equal(t, 1, int(cnt)) // The counter reset row.

		require.NoError(t, engine.Sync())

		err = conn.QueryRow(context.Background(), "SELECT count(*) FROM _ps_catalog.promscale_instance_information").Scan(&cnt)
		require.NoError(t, err)
		require.Equal(t, 2, int(cnt)) // The counter reset row.

		// Update the last_updated of counter_reset row as it just got updated, otherwise the next allowed run would be after 1 hour.
		_, err = conn.Exec(context.Background(), "UPDATE _ps_catalog.promscale_instance_information SET last_updated = current_timestamp - INTERVAL '1 HOUR' WHERE is_counter_reset_row = TRUE")
		require.NoError(t, err)

		// Insert a stale Promscale instance row.
		_, err = conn.Exec(context.Background(), `INSERT INTO _ps_catalog.promscale_instance_information (uuid, last_updated, promscale_ingested_samples_total, is_counter_reset_row) VALUES ('10000000-0000-0000-0000-000000000000', current_timestamp - INTERVAL '1 DAY', 100, FALSE)`)
		require.NoError(t, err)

		err = conn.QueryRow(context.Background(), "SELECT count(*) FROM _ps_catalog.promscale_instance_information").Scan(&cnt)
		require.NoError(t, err)
		require.Equal(t, 3, int(cnt)) // The counter reset row + row added due to Sync() + promscale instance row added above.

		err = conn.QueryRow(context.Background(), "SELECT promscale_ingested_samples_total FROM _ps_catalog.promscale_instance_information WHERE is_counter_reset_row = TRUE").Scan(&cnt)
		require.NoError(t, err)
		require.Equal(t, 0, int(cnt))

		exists := false
		err = conn.QueryRow(context.Background(), "SELECT count(*) > 0 FROM _ps_catalog.promscale_instance_information WHERE uuid = '10000000-0000-0000-0000-000000000000'").Scan(&exists)
		require.NoError(t, err)
		require.True(t, exists)

		// Clean up stale promscale rows.
		require.NoError(t, engine.Sync())

		err = conn.QueryRow(context.Background(), "SELECT count(*) > 0 FROM _ps_catalog.promscale_instance_information WHERE uuid = '10000000-0000-0000-0000-000000000000'").Scan(&exists)
		require.NoError(t, err)
		require.False(t, exists)

		// Check counter reset row's value.
		err = conn.QueryRow(context.Background(), "SELECT promscale_ingested_samples_total FROM _ps_catalog.promscale_instance_information WHERE is_counter_reset_row = TRUE").Scan(&cnt)
		require.NoError(t, err)
		require.Equal(t, 100, int(cnt))

		var lastRunWasAt time.Time
		err = conn.QueryRow(context.Background(), "SELECT last_updated FROM _ps_catalog.promscale_instance_information WHERE is_counter_reset_row = TRUE").Scan(&lastRunWasAt)
		require.NoError(t, err)

		require.NoError(t, engine.Sync())

		// Check if everything is same, when the last run is not beyond the 1 hour.
		expected := lastRunWasAt
		err = conn.QueryRow(context.Background(), "SELECT promscale_ingested_samples_total, last_updated FROM _ps_catalog.promscale_instance_information WHERE is_counter_reset_row = TRUE").Scan(&cnt, &lastRunWasAt)
		require.NoError(t, err)
		require.Equal(t, 100, int(cnt))
		require.Equal(t, expected, lastRunWasAt)
	})
}

func TestTelemetryEngineWhenTelemetryIsSetToOff(t *testing.T) {
	withDB(t, *testDatabase, func(dbOwner *pgxpool.Pool, t testing.TB) {
		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_writer")
		defer db.Close()

		conn := pgxconn.NewPgxConn(db)

		// Do not check the error since this test will run in plain postgres as well,
		// where it will error out, but we are fine with it.
		_, err2 := conn.Exec(context.Background(), "SELECT set_config('timescaledb.telemetry_level', 'off', false)")
		require.NoError(t, err2)

		testIfTelemetryIsOff(t, conn)
	})
}

func testIfTelemetryIsOff(t testing.TB, conn pgxconn.PgxConn) {
	engine := telemetryEngineForE2E(t, conn, nil)

	numTelemetryWritten := 0
	err := conn.QueryRow(context.Background(), "SELECT count(*) FROM _timescaledb_catalog.metadata;").Scan(&numTelemetryWritten)
	require.NoError(t, err)

	require.NoError(t, engine.Sync())
	numTelemetryAfterSync := 0
	err = conn.QueryRow(context.Background(), "SELECT count(*) FROM _timescaledb_catalog.metadata;").Scan(&numTelemetryAfterSync)
	require.NoError(t, err)

	require.Equal(t, numTelemetryWritten, numTelemetryAfterSync)
}

func TestTelemetrySQLStats(t *testing.T) {
	withDB(t, *testDatabase, func(dbOwner *pgxpool.Pool, t testing.TB) {
		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_admin")
		defer db.Close()

		conn := pgxconn.NewPgxConn(db)

		engine := telemetryEngineForE2E(t, conn, nil)
		engine.Start()
		defer engine.Stop()

		// Verify that no SQL stats have been written yet.
		var metrics string
		err := conn.QueryRow(context.Background(), "SELECT value FROM _timescaledb_catalog.metadata WHERE key = 'promscale_metrics_total' AND value IS NOT NULL").Scan(&metrics)
		require.Error(t, err) // No such rows exists.
		require.Equal(t, "no rows in result set", err.Error())

		require.NoError(t, engine.Sync())

		err = conn.QueryRow(context.Background(), "SELECT value FROM _timescaledb_catalog.metadata WHERE key = 'promscale_metrics_total' AND value IS NOT NULL").Scan(&metrics)
		require.NoError(t, err)
		require.Equal(t, "0", metrics) // Without promscale_extension, this will give error saying "no rows in result set".

		// Add dummy metric.
		_, err = conn.Exec(context.Background(), "SELECT _prom_catalog.create_metric_table('test_metric')")
		require.NoError(t, err)

		// Update the last_updated of counter_reset row so that it allows us to scan. Otherwise, the next scan allowed would be after 1 hour.
		_, err = conn.Exec(context.Background(), "UPDATE _ps_catalog.promscale_instance_information SET last_updated = current_timestamp - INTERVAL '1 HOUR' WHERE is_counter_reset_row = TRUE")
		require.NoError(t, err)

		require.NoError(t, engine.Sync())

		err = conn.QueryRow(context.Background(), "SELECT value FROM _timescaledb_catalog.metadata WHERE key = 'promscale_metrics_total' AND value IS NOT NULL").Scan(&metrics)
		require.NoError(t, err)
		require.Equal(t, "1", metrics)
	})
}

func TestPromQLBasedTelemetry(t *testing.T) {
	withDB(t, *testDatabase, func(dbOwner *pgxpool.Pool, t testing.TB) {
		conn, err := dbOwner.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		pgxcfg := conn.Conn().Config()
		cfg := runner.Config{
			Migrate:          false,
			StopAfterMigrate: false,
			UseVersionLease:  true,
			PgmodelCfg: pgclient.Config{
				AppName:        pgclient.DefaultApp,
				Database:       *testDatabase,
				Host:           pgxcfg.Host,
				Port:           int(pgxcfg.Port),
				User:           pgxcfg.User,
				Password:       pgxcfg.Password,
				SslMode:        "allow",
				MaxConnections: -1,
				CacheConfig:    cache.DefaultConfig,
				WriterPoolSize: pgclient.MinPoolSize,
				ReaderPoolSize: pgclient.MinPoolSize,
				MaintPoolSize:  pgclient.MinPoolSize,
			},
		}
		defer conn.Release()

		reader, err := runner.CreateClient(prometheus.NewRegistry(), &cfg)
		require.NoError(t, err)
		defer reader.Close()

		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_writer")
		defer db.Close()

		c := pgxconn.NewPgxConn(db)

		engine := telemetryEngineForE2E(t, c, reader.Queryable())

		var value string
		err = c.QueryRow(context.Background(), "SELECT value FROM _timescaledb_catalog.metadata WHERE key = 'promscale_promql_query_execution_time_p99' AND value IS NOT NULL").Scan(&value)
		require.Error(t, err)
		require.Equal(t, "no rows in result set", err.Error())

		require.NoError(t, engine.Sync())

		err = c.QueryRow(context.Background(), "SELECT value FROM _timescaledb_catalog.metadata WHERE key = 'promscale_promql_query_execution_time_p99' AND value IS NOT NULL").Scan(&value)
		require.NoError(t, err)
		require.Equal(t, "0.0000", value)
	})
}
