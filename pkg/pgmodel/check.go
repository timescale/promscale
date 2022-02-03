package pgmodel

import (
	"context"
	"fmt"

	"github.com/blang/semver/v4"
	"github.com/jackc/pgx/v4"
	"github.com/timescale/promscale/pkg/pgmodel/common/extension"
	"github.com/timescale/promscale/pkg/pgmodel/migrate"
)

// CheckDependencies makes sure all project dependencies, including the DB schema
// the extension, are set up correctly. This will set the ExtensionIsInstalled
// flag and thus should only be called once, at initialization.
func CheckDependencies(db *pgx.Conn, versionInfo migrate.VersionInfo, migrationFailedDueToLockError bool, extOptions extension.ExtensionMigrateOptions) (err error) {
	if err = CheckSchemaVersion(context.Background(), db, versionInfo, migrationFailedDueToLockError); err != nil {
		return err
	}
	return extension.CheckVersions(db, migrationFailedDueToLockError, extOptions)
}

// CheckSchemaVersion checks the DB schema version without checking the extension
func CheckSchemaVersion(ctx context.Context, conn *pgx.Conn, versionInfo migrate.VersionInfo, migrationFailedDueToLockError bool) error {
	expectedVersion := semver.MustParse(versionInfo.Version)
	dbVersion, err := migrate.GetSchemaVersionOnConnection(ctx, conn)
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
