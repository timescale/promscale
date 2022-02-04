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
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/common/extension"
	"github.com/timescale/promscale/pkg/util"
)

const (
	firstAppVersionWithNewMigration = "0.11.0" // TODO decide the correct version to go here
	firstExtVersionWithNewMigration = "0.5.0"  // TODO decide the correct version to go here

	sqlDoesSchemaMigrationTableExist = "SELECT count(*) FILTER (WHERE tablename = 'prom_schema_migrations') > 0 FROM pg_tables"
	sqlRemoveOldExtensionIfExists    = "DROP EXTENSION IF EXISTS promscale" // TODO to cascade or not to cascade?
	sqlInstallExtensionAllBalls      = "CREATE EXTENSION promscale SCHEMA _prom_ext VERSION '0.0.0'"
)

var (
	migrateMutex = &sync.Mutex{}

	MigrationLockError = fmt.Errorf("Could not acquire migration lock. Ensure there are no other connectors running and try again.")
)

type VersionInfo struct {
	Version    string
	CommitHash string
}

func Migrate(conn *pgx.Conn, appVersion VersionInfo, leaseLock *util.PgAdvisoryLock, extOptions extension.ExtensionMigrateOptions) error {
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

	firstAppVersionWithNewMigration, err := semver.Make(firstAppVersionWithNewMigration)
	if err != nil {
		return errors.ErrInvalidSemverFormat
	}
	appSemver, err := semver.Make(appVersion.Version)
	if err != nil {
		return errors.ErrInvalidSemverFormat
	}

	if appSemver.LT(firstAppVersionWithNewMigration) {
		// before the transition to doing all db migrations via the extension
		err = oldMigration(conn, appSemver)
		if err != nil {
			return fmt.Errorf("Error while trying to migrate DB: %w", err)
		}

		_, err = extension.InstallUpgradePromscaleExtensions(conn, extOptions)
		if err != nil {
			return err
		}
	} else {
		// the app version is after the transition to doing migrations via the extension

		exists, err := doesSchemaMigrationTableExist(conn)
		if err != nil {
			return err
		}

		if exists {
			// if the prev app version was before the transition, then we need to:
			// 1. make sure any remaining migrations app-side are applied
			// 2. drop the old version of the extension if it exists
			// 3. install the extension at version 0.0.0
			// 4. upgrade the extension

			// get the prev app version from reading the db table
			prevAppSemver, err := getSchemaVersion(conn)
			if err != nil {
				return err
			}

			// prev < first and first <= app
			// transition to the new world
			// this *should only happen once
			if prevAppSemver.LT(firstAppVersionWithNewMigration) {
				// make sure any remaining migrations app-side are applied
				err = oldMigration(conn, appSemver)
				if err != nil {
					return fmt.Errorf("Error while trying to migrate DB: %w", err)
				}

				// drop the old version of the extension if it exists
				if err = removeOldExtensionIfExists(conn); err != nil {
					return err
				}
				// install the extension at version 0.0.0
				if err = installExtensionAllBalls(conn); err != nil {
					return err
				}
			}
		}

		// new way of doing migrations via extension upgrades only
		extOptions.Install = true // TODO decide whether it is appropriate to force this to true
		extOptions.Upgrade = true // TODO decide whether it is appropriate to force this to true
		_, err = extension.InstallUpgradePromscaleExtensions(conn, extOptions)
		if err != nil {
			return err
		}
	}

	return nil
}

func oldMigration(db *pgx.Conn, appVersion semver.Version) (err error) {
	migrateMutex.Lock()
	defer migrateMutex.Unlock()

	mig := NewMigrator(db, migrations.MigrationFiles, tableOfContents)

	err = mig.Migrate(appVersion)
	if err != nil {
		return fmt.Errorf("Error encountered during migration: %w", err)
	}

	return nil
}

func doesSchemaMigrationTableExist(db *pgx.Conn) (exists bool, err error) {
	err = db.QueryRow(
		context.Background(),
		sqlDoesSchemaMigrationTableExist,
	).Scan(&exists)
	return
}

func removeOldExtensionIfExists(db *pgx.Conn) (err error) {
	firstExtVersionWithNewMigration, err := semver.Make(firstExtVersionWithNewMigration)
	if err != nil {
		return errors.ErrInvalidSemverFormat
	}
	version, installed, err := extension.FetchInstalledExtensionVersion(db, "promscale")
	if err != nil {
		return err
	}

	if installed && version.LT(firstExtVersionWithNewMigration) {
		_, err := db.Exec(
			context.Background(),
			sqlRemoveOldExtensionIfExists,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func installExtensionAllBalls(db *pgx.Conn) (err error) {
	_, err = db.Exec(
		context.Background(),
		sqlInstallExtensionAllBalls,
	)
	return
}

// CheckDependencies makes sure all project dependencies, including the DB schema
// the extension, are set up correctly. This will set the ExtensionIsInstalled
// flag and thus should only be called once, at initialization.
func CheckDependencies(db *pgx.Conn, versionInfo VersionInfo, migrationFailedDueToLockError bool, extOptions extension.ExtensionMigrateOptions) (err error) {
	// TODO decide how to handle this after the switch
	if err = CheckSchemaVersion(context.Background(), db, versionInfo, migrationFailedDueToLockError); err != nil {
		return err
	}
	return extension.CheckVersions(db, migrationFailedDueToLockError, extOptions)
}

// CheckSchemaVersion checks the DB schema version without checking the extension
func CheckSchemaVersion(ctx context.Context, conn *pgx.Conn, versionInfo VersionInfo, migrationFailedDueToLockError bool) error {
	// TODO decide how to handle this after the switch
	expectedVersion := semver.MustParse(versionInfo.Version)
	dbVersion, err := getSchemaVersionOnConnection(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to check schema version: %w", err)
	}
	if versionCompare := dbVersion.Compare(expectedVersion); versionCompare != 0 {
		if versionCompare < 0 && migrationFailedDueToLockError {
			return fmt.Errorf("Failed to acquire the migration lock to upgrade the schema version and unable to run with the old version. Please ensure that no other Promscale connectors with the old schema version are running. Received schema version %v but expected %v", dbVersion, expectedVersion)
		}
		return fmt.Errorf("Error while comparing schema version: received schema version %v but expected %v", dbVersion, expectedVersion)
	}
	return nil
}
