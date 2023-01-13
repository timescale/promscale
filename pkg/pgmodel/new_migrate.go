// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgmodel

import (
	"context"
	"fmt"
	"sync"

	"github.com/blang/semver/v4"
	"github.com/jackc/pgx/v5"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/migrations"
	"github.com/timescale/promscale/pkg/pgmodel/common/extension"
	"github.com/timescale/promscale/pkg/util"
)

var (
	MigrationLockError = fmt.Errorf("Could not acquire migration lock. Ensure there are no other connectors running and try again.")
	migrateMutex       = &sync.Mutex{}
)

func doesSchemaMigrationTableExist(db *pgx.Conn) (exists bool, err error) {
	// We expect there to be either 0 or 1 tables. If there are more then
	// something really weird has happened, and we can't safely proceed.
	const stmt = "SELECT schemaname FROM pg_catalog.pg_tables WHERE tablename = 'prom_schema_migrations'"
	var schemas []string
	rows, err := db.Query(context.Background(), stmt)
	if err != nil {
		return false, err
	}
	for rows.Next() {
		var schema string
		err = rows.Scan(&schema)
		if err != nil {
			return false, err
		}
		schemas = append(schemas, schema)
	}

	switch len(schemas) {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("`prom_schema_migrations` exists in multiple schemas: %s", schemas)
	}
}

func removeOldExtensionIfExists(db *pgx.Conn) error {
	const (
		// transition is the first version of the extension that does the
		// migrations the new way (i.e. in the extension rather than from promscale connector)
		transition = "0.5.0"
		stmt       = "DROP EXTENSION IF EXISTS promscale"
	)

	installedVersion, installed, err := extension.FetchInstalledExtensionVersion(db, "promscale")
	if err != nil {
		return fmt.Errorf("error fetching extension version while dropping old extension: %w", err)
	}

	if installed && installedVersion.GT(semver.MustParse("0.0.1")) && installedVersion.LT(semver.MustParse(transition)) {
		log.Info("msg", "Dropping extension at version '"+installedVersion.String()+"'")
		_, err := db.Exec(
			context.Background(),
			stmt,
		)
		if err != nil {
			return fmt.Errorf("error dropping old extension: %w", err)
		}
	}

	return nil
}

func installExtensionAllBalls(db *pgx.Conn) error {
	log.Info("msg", "Installing extension at version '0.0.0'")
	const stmt = "CREATE EXTENSION promscale SCHEMA public VERSION '0.0.0'"
	_, err := db.Exec(
		context.Background(),
		stmt,
	)
	if err != nil {
		return fmt.Errorf("error installing Promscale extension at version 0.0.0: %w", err)
	}
	return nil
}

func Migrate(conn *pgx.Conn, appVersion VersionInfo, leaseLock *util.PgAdvisoryLock, extOptions extension.ExtensionMigrateOptions) error {
	appSemver, err := semver.Make(appVersion.Version)
	if err != nil {
		return err
	}

	// At startup migrators attempt to grab the schema-version lock. If this
	// fails that means some other connector is running. All is not lost: some
	// other connector may have migrated the DB to the correct version. We warn,
	// then start the connector as normal. If we are on the wrong version, the
	// normal version-check code will prevent us from running.
	if leaseLock != nil {
		locked, err := leaseLock.GetAdvisoryLock()
		if err != nil {
			return fmt.Errorf("error while acquiring migration lock %w", err)
		}
		if !locked {
			return MigrationLockError
		}
		defer func() {
			_, err := leaseLock.Unlock()
			if err != nil {
				log.Error("msg", "error while releasing migration lock", "err", err)
			}
		}()
	} else {
		log.Warn("msg", "skipping migration lock")
	}

	migrateMutex.Lock()
	defer migrateMutex.Unlock()

	// make sure a supported version of the extension is available before making any database changes
	available, err := extension.AreSupportedPromscaleExtensionVersionsAvailable(conn, extOptions)
	switch {
	case err != nil:
		log.Warn("msg", "as of Promscale version 0.11.0, the Promscale database extension is mandatory. please install the latest version of this extension in your TimescaleDB/PostgreSQL database in order to proceed.")
		return fmt.Errorf("error while checking for supported and available promscale extension versions: %w", err)
	case !available:
		log.Warn("msg", "as of Promscale version 0.11.0, the Promscale database extension is mandatory. please install the latest version of the extension in your TimescaleDB/PostgreSQL database in order to proceed.")
		return fmt.Errorf("no supported promscale extension versions are available and the extension is required")
	}

	// if the old prom_schema_migrations table exists, then we need to apply any outstanding
	// migrations from the old way of doing migrations, then transition to the new way
	// the prom_schema_migrations table will be dropped as a part of the transition
	schemaMigrationTableExists, err := doesSchemaMigrationTableExist(conn)
	if err != nil {
		return fmt.Errorf("failed to determine whether the prom_schema_migrations table existed: %w", err)
	}
	if schemaMigrationTableExists {
		if err = upgradeThroughAllBalls(conn, appSemver, extOptions); err != nil {
			return err
		}
	} else {
		if err = installUpgradePromscaleExtension(conn, extOptions); err != nil {
			return err
		}
	}
	return nil
}

func upgradeThroughAllBalls(conn *pgx.Conn, appSemver semver.Version, extOptions extension.ExtensionMigrateOptions) error {
	var err error
	ctx := context.Background()
	// apply any pending migrations from the "old" way
	mig := NewMigrator(conn, migrations.MigrationFiles, TableOfContents)
	if err = mig.Migrate(appSemver); err != nil {
		return fmt.Errorf("Error encountered during migration: %w", err)
	}
	// now that we're up-to-date, remove the old extension, install version 0.0.0, and then upgrade to latest
	if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
		return fmt.Errorf("unable to start transaction: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(ctx, "ROLLBACK")
	}()
	if err = removeOldExtensionIfExists(conn); err != nil {
		return fmt.Errorf("error while dropping old promscale extension: %w", err)
	}
	if err = installExtensionAllBalls(conn); err != nil {
		return fmt.Errorf("error while installing promscale extension version 0.0.0: %w", err)
	}
	log.Info("msg", "upgrading old version of Promscale to version 0.11.0, the Promscale database extension is required moving forward")
	err = extension.InstallUpgradePromscaleExtensions(conn, extOptions)
	if err != nil {
		return fmt.Errorf("failed to install/upgrade promscale extension: %w", err)
	}
	if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("unable to commit migration transaction: %w", err)
	}
	return nil
}

func installUpgradePromscaleExtension(conn *pgx.Conn, extOptions extension.ExtensionMigrateOptions) error {
	var err error
	ctx := context.Background()
	if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
		return fmt.Errorf("unable to start transaction: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(ctx, "ROLLBACK")
	}()
	err = extension.InstallUpgradePromscaleExtensions(conn, extOptions)
	if err != nil {
		return fmt.Errorf("failed to install/upgrade promscale extension: %w", err)
	}
	if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("unable to commit migration transaction: %w", err)
	}
	return nil
}
