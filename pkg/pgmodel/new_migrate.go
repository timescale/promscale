// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgmodel

import (
	"context"
	"fmt"
	"sync"

	"github.com/blang/semver/v4"
	"github.com/jackc/pgx/v4"
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
	const stmt = "SELECT count(*) FILTER (WHERE tablename = 'prom_schema_migrations') > 0 FROM pg_tables"
	err = db.QueryRow(
		context.Background(),
		stmt,
	).Scan(&exists)
	return
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
	const stmt = "CREATE EXTENSION promscale SCHEMA _prom_ext VERSION '0.0.0'"
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
	if err != nil {
		return fmt.Errorf("error while checking for supported and available promscale extension versions: %w", err)
	}
	if !available {
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
		mig := NewMigrator(conn, migrations.MigrationFiles, tableOfContents)
		if err = mig.Migrate(appSemver); err != nil {
			return fmt.Errorf("Error encountered during migration: %w", err)
		}
		if err = removeOldExtensionIfExists(conn); err != nil {
			return fmt.Errorf("error while dropping old promscale extension: %w", err)
		}
		if err = installExtensionAllBalls(conn); err != nil {
			return fmt.Errorf("error while installing promscale extension version 0.0.0: %w", err)
		}
	}

	err = extension.InstallUpgradePromscaleExtensions(conn, extOptions)
	if err != nil {
		return fmt.Errorf("failed to install/upgrade promscale extension: %w", err)
	}

	return nil
}
