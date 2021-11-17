// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/timescale/promscale/pkg/internal/testhelpers"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/telemetry"
)

func generateUUID() uuid.UUID {
	return uuid.New()
}

func setTobsEnv(prop string) error {
	return os.Setenv(fmt.Sprintf("tobs_telemetry_%s", prop), prop)
}

func TestTelemetryHousekeeper(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		//db = testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_writer")
		defer db.Close()

		require.NoError(t, setTobsEnv("random"))
		engine, err := telemetry.NewTelemetryEngine(pgxconn.NewPgxConn(db), generateUUID(), testhelpers.PgConnectURL(*testDatabase, testhelpers.NoSuperuser))
		require.NoError(t, err)
		defer engine.Close()

		// Check if metadata is written.
		var sysName string
		err = db.QueryRow(context.Background(), "select value from _timescaledb_catalog.metadata where key = 'promscale_os_sys_name'").Scan(&sysName)
		require.NoError(t, err)
		require.NotEqual(t, "", sysName)

		var str string
		err = db.QueryRow(context.Background(), "select value from _timescaledb_catalog.metadata where key = 'tobs_telemetry_random'").Scan(&str)
		require.NoError(t, err)
		require.Equal(t, "random", str)

		engine.StartRoutineAsync()

		success, err := engine.BecomeHousekeeper()
		require.NoError(t, err)
		require.True(t, success)

		require.NoError(t, engine.DoHouseKeepingAsync())

		fmt.Println("done", sysName)
	})
}

func TestTelemetryHousekeeperSwitchOver(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		//db = testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_writer")
		defer db.Close()

		// Imagine first Promscale.
		engine1, err := telemetry.NewTelemetryEngine(pgxconn.NewPgxConn(db), generateUUID(), testhelpers.PgConnectURL(*testDatabase, testhelpers.NoSuperuser))
		require.NoError(t, err)
		defer engine1.Close()

		engine1.StartRoutineAsync()

		success1, err := engine1.BecomeHousekeeper()
		require.NoError(t, err)
		require.True(t, success1)

		require.NoError(t, engine1.DoHouseKeepingAsync())

		// Second Promscale.
		engine2, err := telemetry.NewTelemetryEngine(pgxconn.NewPgxConn(db), generateUUID(), testhelpers.PgConnectURL(*testDatabase, testhelpers.NoSuperuser))
		require.NoError(t, err)
		defer engine2.Close()

		engine2.StartRoutineAsync()

		success2, err := engine2.BecomeHousekeeper()
		require.NoError(t, err)
		require.False(t, success2)

		require.Error(t, engine2.DoHouseKeepingAsync()) // Error as we are not the housekeeper.

		// Stop engine 1 and attempt for housekeeper for engine 2.
		engine1.Close()

		success2, err = engine2.BecomeHousekeeper()
		require.NoError(t, err)
		require.True(t, success2)

		fmt.Println("done")
	})
}
