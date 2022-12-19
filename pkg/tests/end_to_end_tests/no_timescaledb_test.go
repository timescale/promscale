// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package end_to_end_tests

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/runner"
	"testing"
)

func TestMigrationFailsIfNoTimescaleDBAvailable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	// We set our own test options because we want to run without timescaledb, regardless of which mode the overall
	// tests are being run in.
	var thisTestOptions testhelpers.TestOptions
	thisTestOptions.SetTimescaleDockerImage(testOptions.GetDockerImageName())
	// Note: We use the testHelpers.WithDB here, which gives us a "fresh", empty database, without extension installed.
	testhelpers.WithDB(t, *testDatabase, testhelpers.NoSuperuser, true, thisTestOptions, func(db *pgxpool.Pool, _ testing.TB, connectUrl string) {
		conn, err := db.Acquire(context.Background())
		if err != nil {
			return
		}
		defer conn.Release()

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
			},
		}

		_, err = runner.CreateClient(prometheus.NewRegistry(), &cfg)

		require.Equal(t, "dependency error: problem with extension: the timescaledb extension is not installed, unable to run Promscale without timescaledb", err.Error())
	})
}
