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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/timescale/promscale/pkg/internal/testhelpers"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/pgmodel"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/common/extension"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/runner"
	"github.com/timescale/promscale/pkg/telemetry"
	"github.com/timescale/promscale/pkg/tests/test_migrations"
	"github.com/timescale/promscale/pkg/version"
)

func TestCheckVersions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, _ testing.TB) {
		conn, err := db.Acquire(context.Background())
		if err != nil {
			return
		}
		defer conn.Release()

		err = extension.CheckPromscaleVersion(conn.Conn())
		if err != nil {
			t.Fatal(err)
		}

		err = extension.CheckVersions(conn.Conn(), false, extension.ExtensionMigrateOptions{
			Install:           true,
			Upgrade:           true,
			UpgradePreRelease: false,
		})
		if err != nil {
			t.Fatal(err)
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
		conn.Release()
		reader, err := runner.CreateClient(prometheus.NewRegistry(), &cfg)
		// reader on its own should start
		if err != nil {
			t.Fatal(err)
		}
		cfg2 := cfg
		cfg2.Migrate = true
		migrator, err := runner.CreateClient(prometheus.NewRegistry(), &cfg2)
		// a regular migrator will just become a reader
		if err != nil {
			t.Fatal(err)
		}

		cfg3 := cfg2
		cfg3.StopAfterMigrate = true
		_, err = runner.CreateClient(prometheus.NewRegistry(), &cfg3)
		if err == nil {
			t.Fatalf("migration should fail due to lock")
		}
		if !strings.Contains(err.Error(), "Could not acquire migration lock") {
			t.Fatalf("Incorrect error, expected lock failure, foud: %v", err)
		}

		reader.Close()
		migrator.Close()

		onlyMigrator, err := runner.CreateClient(prometheus.NewRegistry(), &cfg3)
		if err != nil {
			t.Fatal(err)
		}
		if onlyMigrator != nil {
			t.Fatal(onlyMigrator)
		}

		migrator, err = runner.CreateClient(prometheus.NewRegistry(), &cfg2)
		// a regular migrator should still start
		if err != nil {
			t.Fatal(err)
		}
		defer migrator.Close()

		reader, err = runner.CreateClient(prometheus.NewRegistry(), &cfg)
		// reader should still be able to start
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
	})
}

func verifyExtensionExists(t *testing.T, db *pgxpool.Pool, name string, expectExists bool) {
	var count int
	err := db.QueryRow(context.Background(), `SELECT count(*) FROM pg_extension where extname=$1`, name).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	actualExists := count > 0
	if expectExists != actualExists {
		t.Fatalf("extension %v is not in the right exists state. Expected %v got %v.", name, expectExists, actualExists)
	}
}

func TestInstallFlagPromscaleExtension(t *testing.T) {
	// TODO (james): remove this skip
	t.Skip("XXX skipping failing test")
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
			Migrate:           true,
			InstallExtensions: false,
			StopAfterMigrate:  false,
			UseVersionLease:   true,
			PgmodelCfg: pgclient.Config{
				AppName:        pgclient.DefaultApp,
				Database:       *testDatabase,
				Host:           pgxcfg.Host,
				Port:           int(pgxcfg.Port),
				User:           pgxcfg.User,
				Password:       pgxcfg.Password,
				SslMode:        "allow",
				MaxConnections: -1,
			},
		}
		conn.Release()
		_, err = db.Exec(context.Background(), "DROP EXTENSION IF EXISTS promscale")
		if err != nil {
			t.Fatal(err)
		}
		verifyExtensionExists(t, db, "promscale", false)

		cfg.InstallExtensions = false
		migrator, err := runner.CreateClient(prometheus.NewRegistry(), &cfg)
		if err == nil {
			t.Fatal("expected an error as the promscale extension is required but was not installed")
		}
		if migrator != nil {
			migrator.Close()
		}

		verifyExtensionExists(t, db, "promscale", false)

		cfg.InstallExtensions = true
		migrator, err = runner.CreateClient(prometheus.NewRegistry(), &cfg)
		if err != nil {
			t.Fatal(err)
		}
		migrator.Close()
		verifyExtensionExists(t, db, "promscale", true)
	})
}

func TestMigrateTwice(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	testhelpers.WithDB(t, *testDatabase, testhelpers.NoSuperuser, false, testOptions, func(dbOwner *pgxpool.Pool, t testing.TB, connectURL string) {
		performMigrate(t, connectURL)

		performMigrate(t, connectURL)

		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_writer")
		defer db.Close()

		_, err := telemetry.NewEngine(pgxconn.NewPgxConn(db), [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, nil)
		if err != nil {
			t.Fatal("creating telemetry engine: %w", err)
		}
		var versionString string
		err = db.QueryRow(context.Background(), "SELECT value FROM _timescaledb_catalog.metadata WHERE key='promscale_version'").Scan(&versionString)
		if err != nil {
			t.Fatal(err)
		}

		if versionString != version.Promscale {
			t.Fatalf("wrong version, expected %v got %v", version.Promscale, versionString)
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
	testhelpers.WithDB(t, *testDatabase, testhelpers.NoSuperuser, false, testOptions, func(db *pgxpool.Pool, t testing.TB, connectURL string) {
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

		migrate_to := func(version string, expectErr bool) {
			c, err := db.Acquire(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Release()
			mig := pgmodel.NewMigrator(c.Conn(), test_migrations.MigrationFiles, testTOC)

			err = mig.Migrate(semver.MustParse(version))
			if !expectErr && err != nil {
				t.Fatal(err)
			}
			if expectErr && err == nil {
				t.Fatal("Expected error but none found")
			}
		}

		migrate_to("0.1.1", false)
		verifyLogs(t, db, expected)

		//does nothing
		migrate_to("0.1.1", false)
		verifyLogs(t, db, expected)

		//migration + idempotent files on update
		expected = append(expected,
			"migration 0.2.0",
			"idempotent 1",
			"idempotent 2")

		migrate_to("0.2.0", false)
		verifyLogs(t, db, expected)

		//does nothing, since non-dev and same version as before
		migrate_to("0.2.0", false)
		verifyLogs(t, db, expected)

		//even if no version upgrades, idempotent files apply
		expected = append(expected,
			"idempotent 1",
			"idempotent 2")
		migrate_to("0.8.0", false)
		verifyLogs(t, db, expected)

		//staying on same version does nothing
		migrate_to("0.8.0", false)
		verifyLogs(t, db, expected)

		//migrate two version 0.9.0 and 0.10.0 at once to make sure ordered correctly
		expected = append(expected,
			"migration 0.9.0",
			"migration 0.10.0=1",
			"migration 0.10.0=2",
			"idempotent 1",
			"idempotent 2")
		migrate_to("0.10.0", false)
		verifyLogs(t, db, expected[0:13])

		//upgrading version, idempotent files apply
		expected = append(expected,
			"idempotent 1",
			"idempotent 2")
		migrate_to("0.10.1-dev", false)
		verifyLogs(t, db, expected)

		//even if no version upgrades, idempotent files apply if it's a dev version
		expected = append(expected,
			"idempotent 1",
			"idempotent 2")
		migrate_to("0.10.1-dev", false)
		verifyLogs(t, db, expected)

		//now test logic within a release:
		expected = append(expected,
			"migration 0.10.1=1",
			"idempotent 1",
			"idempotent 2")
		migrate_to("0.10.1-dev.1", false)
		verifyLogs(t, db, expected[0:20])

		expected = append(expected,
			"migration 0.10.1=2",
			"idempotent 1",
			"idempotent 2")
		migrate_to("0.10.1-dev.2", false)
		verifyLogs(t, db, expected)

		//test beta tags
		expected = append(expected,
			"migration 0.10.2-beta=1",
			"idempotent 1",
			"idempotent 2")
		migrate_to("0.10.2-beta.dev.1", false)
		verifyLogs(t, db, expected)

		//test errors - namely test that the versioned update scripts are applied transactionally
		//errors in later update scripts cause everything to roll back.
		migrate_to("0.11.0", true)
		verifyLogs(t, db, expected)
	})
}
