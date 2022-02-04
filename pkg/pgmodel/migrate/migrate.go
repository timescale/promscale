package migrate

import (
	"context"
	"fmt"

	"github.com/blang/semver/v4"
	"github.com/jackc/pgx/v4"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/common/extension"
	"github.com/timescale/promscale/pkg/util"
)

const (
	getVersion = "SELECT version FROM prom_schema_migrations LIMIT 1"
)

var (
	MigrationLockError = fmt.Errorf("could not acquire migration lock. Ensure there are no other connectors running and try again")
)

type VersionInfo struct {
	Version    string
	CommitHash string
}

func Run(conn *pgx.Conn, appVersion VersionInfo, leaseLock *util.PgAdvisoryLock, extOptions extension.ExtensionMigrateOptions) error {
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

	err := migrate(conn, appVersion)
	if err != nil {
		return fmt.Errorf("error while trying to Migrate DB: %w", err)
	}

	_, err = extension.InstallUpgradePromscaleExtensions(conn, extOptions)
	if err != nil {
		return err
	}

	return nil
}

func GetSchemaVersionOnConnection(ctx context.Context, db *pgx.Conn) (semver.Version, error) {
	var version semver.Version
	res, err := db.Query(ctx, getVersion)
	if err != nil {
		return version, fmt.Errorf("Error getting DB version: %w", err)
	}
	defer res.Close()

	for res.Next() {
		err = res.Scan(&version)
	}
	if err != nil {
		return version, fmt.Errorf("Error getting DB version: %w", err)
	}
	err = res.Err()
	if err != nil {
		return version, fmt.Errorf("Error getting DB version: %w", err)
	}

	return version, nil
}
