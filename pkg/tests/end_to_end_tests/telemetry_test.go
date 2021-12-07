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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/timescale/promscale/pkg/internal/testhelpers"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/telemetry"
)

func generateUUID() uuid.UUID {
	return uuid.New()
}

func setTobsEnv(prop string) error {
	return os.Setenv(fmt.Sprintf("TOBS_TELEMETRY_%s", prop), prop)
}

func TestPromscaleTobsMetadata(t *testing.T) {
	if !*useExtension {
		t.Skip("promscale extension not installed, skipping")
	}
	withDB(t, *testDatabase, func(dbOwner *pgxpool.Pool, t testing.TB) {
		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_writer")
		defer db.Close()

		require.NoError(t, setTobsEnv("random"))
		conn := pgxconn.NewPgxConn(db)

		_, err := telemetry.NewEngine(conn, generateUUID())
		require.NoError(t, err)

		// Check if metadata is written.
		var sysName string
		err = conn.QueryRow(context.Background(), "select value from _timescaledb_catalog.metadata where key = 'promscale_os_sys_name'").Scan(&sysName)
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

		engine, err := telemetry.NewEngine(conn, generateUUID())
		require.NoError(t, err)

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
		err = conn.QueryRow(context.Background(), "SELECT sum(promscale_ingested_samples_total) FROM _ps_catalog.promscale_instance_information").Scan(&value)
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

		_, err := conn.Exec(context.Background(), `INSERT INTO _ps_catalog.promscale_instance_information (uuid, last_updated, is_counter_reset_row) VALUES ('00000000-0000-0000-0000-000000000000', current_timestamp, TRUE)`)
		require.NoError(t, err)

		// Should error due to unique constraint for counter_reset_row.
		_, err = conn.Exec(context.Background(), `INSERT INTO _ps_catalog.promscale_instance_information (uuid, last_updated, is_counter_reset_row) VALUES ('00000000-0000-0000-0000-000000000000', current_timestamp, TRUE)`)
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), `violates unique constraint "promscale_instance_information_pkey"`))
	})
}
