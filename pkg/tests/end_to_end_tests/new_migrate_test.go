// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package end_to_end_tests

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgmodel"
	"github.com/timescale/promscale/pkg/pgmodel/common/extension"
	"github.com/timescale/promscale/pkg/version"
	"testing"
)

func TestMigrationFailesIfMultiplePromSchemaMigrationsTables(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, _ testing.TB) {
		conn, err := db.Acquire(context.Background())
		if err != nil {
			return
		}
		defer conn.Release()

		var queries = []string{
			"CREATE TABLE public.prom_schema_migrations (version text not null primary key)",
			"CREATE SCHEMA myschema",
			"CREATE TABLE myschema.prom_schema_migrations (version text not null primary key)",
		}
		for _, query := range queries {
			_, err = conn.Exec(context.Background(), query)
			if err != nil {
				t.Fatal(err)
			}
		}

		extOptions := extension.ExtensionMigrateOptions{Install: true, Upgrade: true, UpgradePreRelease: true}

		err = pgmodel.Migrate(conn.Conn(), pgmodel.VersionInfo{Version: version.Promscale, CommitHash: "azxtestcommit"}, nil, extOptions)

		require.Equal(t, err.Error(), "failed to determine whether the prom_schema_migrations table existed: `prom_schema_migrations` exists in multiple schemas: [public myschema]")
	})
}
