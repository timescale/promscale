// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package common

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/timescale/promscale/pkg/pgmodel"
	"github.com/timescale/promscale/pkg/pgmodel/common/extension"
	"github.com/timescale/promscale/pkg/runner"
	"github.com/timescale/promscale/pkg/version"
)

// PerformMigrate performs schema migrations and extension installations in the database after receiving connection from the pool.
func PerformMigrate(t testing.TB, connectURL, superConnectURL string, useTimescaleDB, useExtension bool) {
	extOptions := extension.ExtensionMigrateOptions{Install: true, Upgrade: true, UpgradePreRelease: true}
	if useTimescaleDB {
		migrateURL := connectURL
		if !useExtension {
			// The docker image without an extension does not have pgextwlist
			// Thus, you have to use the superuser to install TimescaleDB
			migrateURL = superConnectURL
		}
		err := extension.InstallUpgradeTimescaleDBExtensions(migrateURL, extOptions)
		if err != nil {
			t.Fatal(err)
		}
	}

	migratePool, err := pgxpool.Connect(context.Background(), connectURL)
	if err != nil {
		t.Fatal(err)
	}
	defer migratePool.Close()
	conn, err := migratePool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	err = runner.SetupDBState(conn.Conn(), pgmodel.VersionInfo{Version: version.Promscale, CommitHash: "azxtestcommit"}, nil, extOptions)
	if err != nil {
		t.Fatal(err)
	}
}
