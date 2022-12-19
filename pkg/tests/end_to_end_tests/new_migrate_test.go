// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package end_to_end_tests

import (
	"context"
	"github.com/blang/semver/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
	"github.com/timescale/promscale/pkg/migrations"
	"github.com/timescale/promscale/pkg/pgmodel"
	"github.com/timescale/promscale/pkg/pgmodel/common/extension"
	"github.com/timescale/promscale/pkg/version"
	"testing"
)

func TestMigrationFailsIfMultiplePromSchemaMigrationsTables(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	// Note: We use the testHelpers.WithDB here, which gives us a "fresh", empty database, without extension installed.
	testhelpers.WithDB(t, *testDatabase, testhelpers.NoSuperuser, true, testOptions, func(db *pgxpool.Pool, _ testing.TB, connectUrl string) {
		conn, err := db.Acquire(context.Background())
		if err != nil {
			return
		}
		defer conn.Release()

		// Use the old migration method to install all Promscale SQL up to
		// 0.11.0 (the last Promscale version which has its own SQL).
		// This will create the table `public.prom_schema_migrations`.
		mig := pgmodel.NewMigrator(conn.Conn(), migrations.MigrationFiles, pgmodel.TableOfContents)
		if err = mig.Migrate(semver.MustParse("0.11.0")); err != nil {
			t.Fatal(err)
		}

		// Add the additional schema, to cause "actual" migration to fail.
		var queries = []string{
			"CREATE SCHEMA myschema",
			"CREATE TABLE myschema.prom_schema_migrations (version text not null primary key)",
		}
		for _, query := range queries {
			_, err = conn.Exec(context.Background(), query)
			if err != nil {
				t.Fatal(err)
			}
		}

		// Use the new migration method which looks for tables named
		// `prom_schema_migrations`, and should fail because there are two
		extOptions := extension.ExtensionMigrateOptions{Install: true, Upgrade: true, UpgradePreRelease: true}
		err = pgmodel.Migrate(conn.Conn(), pgmodel.VersionInfo{Version: version.Promscale, CommitHash: "azxtestcommit"}, nil, extOptions)

		require.Equal(t, "failed to determine whether the prom_schema_migrations table existed: `prom_schema_migrations` exists in multiple schemas: [public myschema]", err.Error())
	})
}
