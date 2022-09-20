// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package runner

import (
	"context"
	"fmt"
	"strconv"

	"github.com/grafana/regexp"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/timescale/promscale/pkg/dataset"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/pgmodel"
	"github.com/timescale/promscale/pkg/pgmodel/common/extension"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/tenancy"
	"github.com/timescale/promscale/pkg/util"
	"github.com/timescale/promscale/pkg/version"
)

var (
	appVersion = pgmodel.VersionInfo{Version: version.Promscale, CommitHash: version.CommitHash}
)

func CreateClient(r prometheus.Registerer, cfg *Config) (*pgclient.Client, error) {
	// The TimescaleDB migration has to happen before other connections
	// are open, also it has to happen as the first command on a connection.
	// Thus we cannot rely on the migration lock here. Instead we assume
	// that upgrading TimescaleDB will not break existing connectors.
	// (upgrading the DB will force-close all existing connections, so we may
	// add a reconnect check that the DB has an appropriate version)
	connStr := cfg.PgmodelCfg.GetConnectionStr()
	extOptions := extension.ExtensionMigrateOptions{
		Install:           cfg.InstallExtensions,
		Upgrade:           cfg.UpgradeExtensions,
		UpgradePreRelease: cfg.UpgradePrereleaseExtensions,
	}

	if cfg.InstallExtensions {
		err := extension.InstallUpgradeTimescaleDBExtensions(connStr, extOptions)
		if err != nil {
			return nil, err
		}
	}

	// Migration lock logic:
	// We don't want to upgrade the schema version while we still have connectors
	// attached who think the schema is at the old version. To prevent this, as
	// best we can, each normal connection attempts to grab a (shared) advisory
	// lock on schemaLockId, and we attempt to grab an exclusive lock on it
	// before running migrate. This implies that migration must be run when no
	// other connector is running.
	schemaVersionLease, err := util.NewPgAdvisoryLock(schema.LockID, connStr)
	if err != nil {
		log.Error("msg", "error creating schema version lease", "err", err)
		return nil, startupError
	}
	// after the client has started it's in charge of maintaining the leases
	defer schemaVersionLease.Close()

	migrationFailedDueToLockError := true
	if cfg.Migrate {
		conn, err := schemaVersionLease.Conn()
		if err != nil {
			return nil, fmt.Errorf("migration error: %w", err)
		}
		lease := schemaVersionLease
		if !cfg.UseVersionLease {
			lease = nil
		}
		err = pgmodel.Migrate(conn, appVersion, lease, extOptions)
		migrationFailedDueToLockError = err == pgmodel.MigrationLockError
		if err != nil && err != pgmodel.MigrationLockError {
			return nil, fmt.Errorf("migration error: %w", err)
		}

		if cfg.StopAfterMigrate {
			if err != nil {
				return nil, err
			}
			log.Info("msg", "Migration successful, exiting")
			return nil, nil
		}
	} else {
		log.Info("msg", "Skipping migration")
	}

	if cfg.UseVersionLease {
		// Grab a lease to protect version checking and client creation. This
		// lease will be released when the schemaVersionLease is closed.
		locked, err := schemaVersionLease.GetSharedAdvisoryLock()
		if err != nil {
			return nil, fmt.Errorf("could not acquire schema version lease due to: %w", err)
		}
		if !locked {
			return nil, fmt.Errorf("could not acquire schema version lease. is a migration in progress?")
		}
	} else {
		log.Warn("msg", "skipping schema version lease")
	}

	// Check the database is on the correct version.
	// This must be done even if we attempted a migrate; if we failed to acquire
	// the lock we'll start up as-if we were never supposed to migrate in the
	// first place. This is also needed as checkDependencies populates our
	// extension metadata
	conn, err := schemaVersionLease.Conn()
	if err != nil {
		return nil, fmt.Errorf("Dependency checking error while trying to open DB connection: %w", err)
	}
	err = extension.CheckVersions(conn, migrationFailedDueToLockError, extOptions)
	if err != nil {
		err = fmt.Errorf("dependency error: %w", err)
		if migrationFailedDueToLockError {
			log.Error("msg", "Unable to run migrations; failed to acquire the lock. If the database is on an incorrect version, ensure there are no other connectors running and try again.", "err", err)
		}
		return nil, err
	}

	if cfg.InstallExtensions {
		// Only check for background workers if TimescaleDB is installed.
		if notOk, err := isBGWLessThanDBs(conn); err != nil {
			return nil, fmt.Errorf("Error checking the number of background workers: %w", err)
		} else if notOk {
			log.Warn("msg", "Maximum background worker setting is too low for the number of databases in your system. "+
				"Please increase your timescaledb.max_background_workers setting. See https://docs.timescale.com/latest/getting-started/configuring#workers for more information.")
		}
	}

	_, isLicenseOSS, err := getDatabaseDetails(conn)
	if err != nil {
		return nil, fmt.Errorf("fetching license information: %w", err)
	}

	if isLicenseOSS {
		log.Warn("msg", "WARNING: Using the Apache2 version of TimescaleDB. This version does not include "+
			"compression and thus performance and disk usage will be significantly negatively effected.")
	}

	if !cfg.APICfg.HighAvailability && !cfg.APICfg.ReadOnly {
		log.Info("msg", "Prometheus HA is not enabled")
	}

	leasingFunction := getSchemaLease
	if !cfg.UseVersionLease {
		leasingFunction = nil
	}

	multiTenancy := tenancy.NewNoopAuthorizer()
	if cfg.TenancyCfg.EnableMultiTenancy {
		multiTenancyConfig := tenancy.NewAllowAllTenantsConfig(cfg.TenancyCfg.AllowNonMTWrites)
		if !cfg.TenancyCfg.SkipTenantValidation {
			multiTenancyConfig = tenancy.NewSelectiveTenancyConfig(cfg.TenancyCfg.ValidTenantsList, cfg.TenancyCfg.AllowNonMTWrites, cfg.TenancyCfg.UseExperimentalLabelQueries)
		}
		multiTenancy, err = tenancy.NewAuthorizer(multiTenancyConfig)
		if err != nil {
			return nil, fmt.Errorf("new tenancy: %w", err)
		}
		cfg.APICfg.MultiTenancy = multiTenancy
	}

	if cfg.DatasetConfig != "" {
		err = ApplyDatasetConfig(conn, cfg.DatasetConfig)
		if err != nil {
			return nil, fmt.Errorf("error applying dataset configuration: %w", err)
		}
	}

	// client has to be initiated after migrate since migrate
	// can change database GUC settings
	client, err := pgclient.NewClient(r, &cfg.PgmodelCfg, multiTenancy, leasingFunction, cfg.APICfg.ReadOnly)
	if err != nil {
		return nil, fmt.Errorf("client creation error: %w", err)
	}
	if err = client.InitPromQLEngine(&cfg.PromQLCfg); err != nil {
		return nil, fmt.Errorf("initializing PromQL Engine: %w", err)
	}

	return client, nil
}

func getDatabaseDetails(conn *pgx.Conn) (isTimescaleDB, isLicenseOSS bool, err error) {
	err = conn.QueryRow(context.Background(), "SELECT _prom_catalog.is_timescaledb_installed()").Scan(&isTimescaleDB)
	if err != nil {
		return false, false, fmt.Errorf("error fetching whether TimescaleDB is installed: %w", err)
	}
	if !isTimescaleDB {
		// Return false so that we don't warn for OSS TimescaleDB.
		return false, false, nil
	}
	err = conn.QueryRow(context.Background(), "SELECT _prom_catalog.is_timescaledb_oss()").Scan(&isLicenseOSS)
	if err != nil {
		return isTimescaleDB, false, fmt.Errorf("error fetching TimescaleDB license: %w", err)
	}
	return isTimescaleDB, isLicenseOSS, nil
}

// isBGWLessThanDBs checks if the background workers count is less than the database count. It should be
// called only if TimescaleDB is installed.
func isBGWLessThanDBs(conn *pgx.Conn) (bool, error) {
	var (
		dbs       int
		maxBGWStr string
	)
	err := conn.QueryRow(context.Background(), "SHOW timescaledb.max_background_workers").Scan(&maxBGWStr)
	if err != nil {
		return false, fmt.Errorf("Unable to fetch timescaledb.max_background_workers: %w", err)
	}
	maxBGWs, err := strconv.Atoi(maxBGWStr)
	if err != nil {
		return false, fmt.Errorf("maxBGw string conversion: %w", err)
	}
	err = conn.QueryRow(context.Background(), "SELECT count(*) from pg_catalog.pg_database").Scan(&dbs)
	if err != nil {
		return false, fmt.Errorf("Unable to fetch count of all databases: %w", err)
	}
	if maxBGWs < dbs+2 {
		return true, nil
	}
	return false, nil
}

func ApplyDatasetConfig(conn *pgx.Conn, cfgFilename string) error {
	cfg, err := dataset.NewConfig(cfgFilename)
	if err != nil {
		return err
	}

	return cfg.Apply(conn)
}

func compileAnchoredRegexString(s string) (*regexp.Regexp, error) {
	r, err := regexp.Compile("^(?:" + s + ")$")
	if err != nil {
		return nil, err
	}
	return r, nil
}

// Except for migration, every connection that communicates with the DB must be
// guarded by an instance of the schema-version lease to ensure that no other
// connector can migrate the DB out from under it. We do not bother to release
// said lease; in such an event the connector will be shutdown anyway, and
// connection-death will close the connection.
func getSchemaLease(ctx context.Context, conn *pgx.Conn) error {
	if err := util.GetSharedLease(ctx, conn, schema.LockID); err != nil {
		return err
	}
	return extension.CheckPromscaleVersion(conn)
}
