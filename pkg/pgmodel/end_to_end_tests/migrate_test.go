// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package end_to_end_tests

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/blang/semver/v4"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/timescale/promscale/pkg/api"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/pgmodel"
	"github.com/timescale/promscale/pkg/pgmodel/test_migrations"
	"github.com/timescale/promscale/pkg/runner"
	"github.com/timescale/promscale/pkg/version"
)

func TestMigrate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		var dbVersion string
		err := db.QueryRow(context.Background(), "SELECT version FROM prom_schema_migrations").Scan(&dbVersion)
		if err != nil {
			t.Fatal(err)
		}
		if dbVersion != version.Version {
			t.Errorf("Version unexpected:\ngot\n%s\nwanted\n%s", dbVersion, version.Version)
		}

		readOnly := testhelpers.GetReadOnlyConnection(t, *testDatabase)
		defer readOnly.Close()
		conn, err := readOnly.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		err = pgmodel.CheckDependencies(conn.Conn(), pgmodel.VersionInfo{Version: version.Version})
		if err != nil {
			t.Error(err)
		}

		err = pgmodel.CheckDependencies(conn.Conn(), pgmodel.VersionInfo{Version: "100.0.0"})
		if err == nil {
			t.Errorf("Expected error in CheckDependencies")
		}
	})
}

func TestMigrateLock(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, _ testing.TB) {
		conn, err := db.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		pgxcfg := conn.Conn().Config()
		cfg := runner.Config{
			Migrate:          false,
			StopAfterMigrate: false,
			UseVersionLease:  true,
			PgmodelCfg: pgclient.Config{
				Database:                *testDatabase,
				Host:                    pgxcfg.Host,
				Port:                    int(pgxcfg.Port),
				User:                    pgxcfg.User,
				Password:                pgxcfg.Password,
				SslMode:                 "allow",
				MaxConnections:          -1,
				WriteConnectionsPerProc: 1,
			},
		}
		conn.Release()
		metrics := api.InitMetrics()
		reader, err := runner.CreateClient(&cfg, metrics)
		// reader on its own should start
		if err != nil {
			t.Fatal(err)
		}
		cfg2 := cfg
		cfg2.Migrate = true
		migrator, err := runner.CreateClient(&cfg2, metrics)
		// a regular migrator will just become a reader
		if err != nil {
			t.Fatal(err)
		}

		cfg3 := cfg2
		cfg3.StopAfterMigrate = true
		_, err = runner.CreateClient(&cfg3, metrics)
		if err == nil {
			t.Fatalf("migration should fail due to lock")
		}
		if !strings.Contains(err.Error(), "Could not acquire migration lock") {
			t.Fatalf("Incorrect error, expected lock failure, foud: %v", err)
		}

		reader.Close()
		migrator.Close()

		only_migrator, err := runner.CreateClient(&cfg3, metrics)
		if err != nil {
			t.Fatal(err)
		}
		if only_migrator != nil {
			t.Fatal(only_migrator)
		}

		migrator, err = runner.CreateClient(&cfg2, metrics)
		// a regular migrator should still start
		if err != nil {
			t.Fatal(err)
		}
		defer migrator.Close()

		reader, err = runner.CreateClient(&cfg, metrics)
		// reader should still be able to start
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
	})
}

func TestMigrateTwice(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	testhelpers.WithDB(t, *testDatabase, testhelpers.NoSuperuser, func(db *pgxpool.Pool, t testing.TB, connectURL string) {
		performMigrate(t, connectURL, testhelpers.PgConnectURL(*testDatabase, testhelpers.Superuser))
		if *useExtension && !pgmodel.ExtensionIsInstalled {
			t.Errorf("extension is not installed, expected it to be installed")
		}

		//reset the flag to make sure it's set correctly again.
		pgmodel.ExtensionIsInstalled = false

		performMigrate(t, connectURL, testhelpers.PgConnectURL(*testDatabase, testhelpers.Superuser))
		if *useExtension && !pgmodel.ExtensionIsInstalled {
			t.Errorf("extension is not installed, expected it to be installed")
		}

		if *useTimescaleDB {
			var versionString string
			err := db.QueryRow(context.Background(), "SELECT value FROM _timescaledb_catalog.metadata WHERE key='promscale_version'").Scan(&versionString)
			if err != nil {
				if err == pgx.ErrNoRows && !*useExtension {
					//Without an extension, metadata will not be written if running as non-superuser
					return
				}
				t.Fatal(err)
			}

			if versionString != version.Version {
				t.Fatalf("wrong version, expected %v got %v", version.Version, versionString)
			}
		}
	})
}

func verifyLogs(t testing.TB, db *pgxpool.Pool, expected []string) {
	rows, err := db.Query(context.Background(), "SELECT msg FROM log ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}

	found := make([]string, 0)
	for rows.Next() {
		var value string
		err = rows.Scan(&value)
		if err != nil {
			t.Fatal(err)
		}
		found = append(found, value)
	}
	if !reflect.DeepEqual(expected, found) {
		t.Errorf("wrong values in DB\nexpected:\n\t%v\ngot:\n\t%v", expected, found)
	}
}

func TestMigrationLib(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	testhelpers.WithDB(t, *testDatabase, testhelpers.NoSuperuser, func(db *pgxpool.Pool, t testing.TB, connectURL string) {
		testTOC := map[string][]string{
			"idempotent": {
				"2-toc-run_first.sql",
				"1-toc-run_second.sql",
			},
		}

		expected := []string{
			"setup",
			"idempotent 1",
			"idempotent 2",
		}

		migrate_to := func(version string) {
			c, err := db.Acquire(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Release()
			mig := pgmodel.NewMigrator(c.Conn(), test_migrations.MigrationFiles, testTOC)

			err = mig.Migrate(semver.MustParse(version))
			if err != nil {
				t.Fatal(err)
			}
		}

		migrate_to("0.1.1")
		verifyLogs(t, db, expected)

		//does nothing
		migrate_to("0.1.1")
		verifyLogs(t, db, expected)

		//migration + idempotent files on update
		expected = append(expected,
			"migration 0.2.0",
			"idempotent 1",
			"idempotent 2")

		migrate_to("0.2.0")
		verifyLogs(t, db, expected)

		//does nothing, since non-dev and same version as before
		migrate_to("0.2.0")
		verifyLogs(t, db, expected)

		//even if no version upgrades, idempotent files apply
		expected = append(expected,
			"idempotent 1",
			"idempotent 2")
		migrate_to("0.8.0")
		verifyLogs(t, db, expected)

		//staying on same version does nothing
		migrate_to("0.8.0")
		verifyLogs(t, db, expected)

		//migrate two version 0.9.0 and 0.10.0 at once to make sure ordered correctly
		expected = append(expected,
			"migration 0.9.0",
			"migration 0.10.0=1",
			"migration 0.10.0=2",
			"idempotent 1",
			"idempotent 2")
		migrate_to("0.10.0")
		verifyLogs(t, db, expected[0:13])

		//upgrading version, idempotent files apply
		expected = append(expected,
			"idempotent 1",
			"idempotent 2")
		migrate_to("0.10.1-dev")
		verifyLogs(t, db, expected)

		//even if no version upgrades, idempotent files apply if it's a dev version
		expected = append(expected,
			"idempotent 1",
			"idempotent 2")
		migrate_to("0.10.1-dev")
		verifyLogs(t, db, expected)

		//now test logic within a release:
		expected = append(expected,
			"migration 0.10.1=1",
			"idempotent 1",
			"idempotent 2")
		migrate_to("0.10.1-dev.1")
		verifyLogs(t, db, expected[0:20])

		expected = append(expected,
			"migration 0.10.1=2",
			"idempotent 1",
			"idempotent 2")
		migrate_to("0.10.1-dev.2")
		verifyLogs(t, db, expected)

		//test beta tags
		expected = append(expected,
			"migration 0.10.2-beta=1",
			"idempotent 1",
			"idempotent 2")
		migrate_to("0.10.2-beta.dev.1")
		verifyLogs(t, db, expected)
	})
}
