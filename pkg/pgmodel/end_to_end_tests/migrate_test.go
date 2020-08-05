// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package end_to_end_tests

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/timescale/timescale-prometheus/pkg/internal/testhelpers"
	"github.com/timescale/timescale-prometheus/pkg/pgmodel"
)

const (
	expectedVersion = "0.0.1-alpha"
)

func TestMigrate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		var version string
		err := db.QueryRow(context.Background(), "SELECT version FROM prom_schema_migrations").Scan(&version)
		if err != nil {
			t.Fatal(err)
		}
		if version != expectedVersion {
			t.Errorf("Version unexpected:\ngot\n%s\nwanted\n%s", version, expectedVersion)
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
