package migrate

import (
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel"
	"github.com/timescale/promscale/pkg/pgmodel/common/extension"
	"github.com/timescale/promscale/pkg/util"
)

var (
	MigrationLockError = fmt.Errorf("could not acquire migration lock. Ensure there are no other connectors running and try again")
)

func SetupDBState(conn *pgx.Conn, appVersion pgmodel.VersionInfo, leaseLock *util.PgAdvisoryLock, extOptions extension.ExtensionMigrateOptions) error {
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

	err := pgmodel.Migrate(conn, appVersion, extOptions)
	if err != nil {
		return fmt.Errorf("error while trying to migrate DB: %w", err)
	}

	_, err = extension.InstallUpgradePromscaleExtensions(conn, extOptions)
	if err != nil {
		return err
	}

	return nil
}
