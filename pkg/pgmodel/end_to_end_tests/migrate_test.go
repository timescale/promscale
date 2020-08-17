// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package end_to_end_tests

import (
	"context"
	"reflect"
	"testing"

	"github.com/blang/semver/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/timescale/timescale-prometheus/pkg/internal/testhelpers"
	"github.com/timescale/timescale-prometheus/pkg/pgmodel"
	"github.com/timescale/timescale-prometheus/pkg/pgmodel/test_migrations"
	"github.com/timescale/timescale-prometheus/pkg/version"
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
	})
}

func TestMigrateTwice(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	testhelpers.WithDB(t, *testDatabase, testhelpers.NoSuperuser, func(db *pgxpool.Pool, t testing.TB, connectURL string) {
		performMigrate(t, connectURL)
		if *useExtension && !pgmodel.ExtensionIsInstalled {
			t.Errorf("extension is not installed, expected it to be installed")
		}

		//reset the flag to make sure it's set correctly again.
		pgmodel.ExtensionIsInstalled = false

		performMigrate(t, connectURL)
		if *useExtension && !pgmodel.ExtensionIsInstalled {
			t.Errorf("extension is not installed, expected it to be installed")
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

		mig := pgmodel.NewMigrator(db, test_migrations.MigrationFiles, testTOC)

		err := mig.Migrate(semver.MustParse("0.1.1"))
		if err != nil {
			t.Fatal(err)
		}

		verifyLogs(t, db, expected)

		//does nothing
		err = mig.Migrate(semver.MustParse("0.1.1"))
		if err != nil {
			t.Fatal(err)
		}
		verifyLogs(t, db, expected)

		//migration + idempotent files on update
		expected = append(expected,
			"migration 0.2.0",
			"idempotent 1",
			"idempotent 2")

		err = mig.Migrate(semver.MustParse("0.2.0"))
		if err != nil {
			t.Fatal(err)
		}
		verifyLogs(t, db, expected)

		//does nothing, since non-dev and same version as before
		err = mig.Migrate(semver.MustParse("0.2.0"))
		if err != nil {
			t.Fatal(err)
		}
		verifyLogs(t, db, expected)

		//even if no version upgrades, idempotent files apply
		expected = append(expected,
			"idempotent 1",
			"idempotent 2")
		err = mig.Migrate(semver.MustParse("0.8.0"))
		if err != nil {
			t.Fatal(err)
		}
		verifyLogs(t, db, expected)

		//staying on same version does nothing
		err = mig.Migrate(semver.MustParse("0.8.0"))
		if err != nil {
			t.Fatal(err)
		}
		verifyLogs(t, db, expected)

		//migrate two version 0.9.0 and 0.10.0 at once to make sure ordered correctly
		expected = append(expected,
			"migration 0.9.0",
			"migration 0.10.0=1",
			"migration 0.10.0=2",
			"idempotent 1",
			"idempotent 2")
		err = mig.Migrate(semver.MustParse("0.10.0"))
		if err != nil {
			t.Fatal(err)
		}
		verifyLogs(t, db, expected[0:13])

		//upgrading version, idempotent files apply
		expected = append(expected,
			"idempotent 1",
			"idempotent 2")
		err = mig.Migrate(semver.MustParse("0.10.1-dev"))
		if err != nil {
			t.Fatal(err)
		}
		verifyLogs(t, db, expected)

		//even if no version upgrades, idempotent files apply if it's a dev version
		expected = append(expected,
			"idempotent 1",
			"idempotent 2")
		err = mig.Migrate(semver.MustParse("0.10.1-dev"))
		if err != nil {
			t.Fatal(err)
		}
		verifyLogs(t, db, expected)

		//now test logic within a release:
		expected = append(expected,
			"migration 0.10.1=1",
			"idempotent 1",
			"idempotent 2")
		err = mig.Migrate(semver.MustParse("0.10.1-dev.1"))
		if err != nil {
			t.Fatal(err)
		}
		verifyLogs(t, db, expected[0:20])
		expected = append(expected,
			"migration 0.10.1=2",
			"idempotent 1",
			"idempotent 2")
		err = mig.Migrate(semver.MustParse("0.10.1-dev.2"))
		if err != nil {
			t.Fatal(err)
		}
		verifyLogs(t, db, expected)

		//test beta tags
		expected = append(expected,
			"migration 0.10.2-beta=1",
			"idempotent 1",
			"idempotent 2")
		err = mig.Migrate(semver.MustParse("0.10.2-beta.dev.1"))
		if err != nil {
			t.Fatal(err)
		}
		verifyLogs(t, db, expected)
	})
}
