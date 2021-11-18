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
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		defer db.Close()

		require.NoError(t, setTobsEnv("random"))
		_, err := telemetry.NewTelemetryEngine(pgxconn.NewPgxConn(db), generateUUID())
		require.NoError(t, err)

		// Check if metadata is written.
		var sysName string
		err = db.QueryRow(context.Background(), "select value from _timescaledb_catalog.metadata where key = 'promscale_os_sys_name'").Scan(&sysName)
		require.NoError(t, err)
		require.NotEqual(t, "", sysName)

		var str string
		err = db.QueryRow(context.Background(), "select value from _timescaledb_catalog.metadata where key = 'tobs_telemetry_random'").Scan(&str)
		require.NoError(t, err)
		require.Equal(t, "random", str)
	})
}
