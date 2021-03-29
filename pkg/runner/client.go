// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package runner

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/timescale/promscale/pkg/api"
	"github.com/timescale/promscale/pkg/log"
	multi_tenancy "github.com/timescale/promscale/pkg/multi-tenancy"
	multi_tenancy_config "github.com/timescale/promscale/pkg/multi-tenancy/config"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/pgmodel"
	"github.com/timescale/promscale/pkg/pgmodel/common/extension"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/util"
	"github.com/timescale/promscale/pkg/version"
)

var (
	appVersion         = pgmodel.VersionInfo{Version: version.Promscale, CommitHash: version.CommitHash}
	migrationLockError = fmt.Errorf("Could not acquire migration lock. Ensure there are no other connectors running and try again.")
)

func CreateClient(cfg *Config, promMetrics *api.Metrics) (*pgclient.Client, error) {
	// The TimescaleDB migration has to happen before other connections
	// are open, also it has to happen as the first command on a connection.
	// Thus we cannot rely on the migration lock here. Instead we assume
	// that upgrading TimescaleDB will not break existing connectors.
	// (upgrading the DB will force-close all existing connections, so we may
	// add a reconnect check that the DB has an appropriate version)
	connStr, err := cfg.PgmodelCfg.GetConnectionStr()
	if err != nil {
		return nil, err
	}
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
		err = SetupDBState(conn, appVersion, lease, extOptions)
		migrationFailedDueToLockError = err == migrationLockError
		if err != nil && err != migrationLockError {
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
	err = pgmodel.CheckDependencies(conn, appVersion, migrationFailedDueToLockError, extOptions)
	if err != nil {
		err = fmt.Errorf("dependency error: %w", err)
		if migrationFailedDueToLockError {
			log.Error("msg", "Unable to run migrations; failed to acquire the lock. If the database is on an incorrect version, ensure there are no other connectors running and try again.", "err", err)
		}
		return nil, err
	}

	if cfg.InstallExtensions {
		// Only check for background workers if TimessaleDB is installed.
		if notOk, err := isBGWLessThanDBs(conn); err != nil {
			return nil, fmt.Errorf("Error checking the number of background workers: %w", err)
		} else if notOk {
			log.Warn("msg", "Maximum background worker setting is too low for the number of databases in your system. "+
				"Please increase your timescaledb.max_background_workers setting. See https://docs.timescale.com/latest/getting-started/configuring#workers for more information.")
		}
	}

	// Election must be done after migration and version-checking: if we're on
	// the wrong version we should not participate in leader-election.
	elector, err = initElector(cfg, promMetrics)

	if err != nil {
		return nil, fmt.Errorf("elector init error: %w", err)
	}

	if elector == nil && !cfg.APICfg.ReadOnly {
		log.Warn(
			"msg",
			"No adapter leader election. Group lock id is not set. "+
				"Possible duplicate write load if running multiple connectors",
		)
	}

	leasingFunction := getSchemaLease
	if !cfg.UseVersionLease {
		leasingFunction = nil
	}

	var multiTenancy = multi_tenancy.NewNoopMultiTenancy()
	if cfg.EnableMultiTenancy {
		// Configuring multi-tenancy in client.
		multiTenancyConfig := &multi_tenancy_config.Config{
			ValidTenants: cfg.ValidTenantsList,
		}
		switch cfg.MultiTenancyType {
		case "plain":
			multiTenancyConfig.AuthType = multi_tenancy_config.Allow
		case "bearer_token":
			multiTenancyConfig.AuthType = multi_tenancy_config.BearerToken
			multiTenancyConfig.BearerToken = cfg.APICfg.Auth.BearerToken
		default:
			return nil, fmt.Errorf("invalid multi-tenancy type: %s", cfg.MultiTenancyType)
		}
		if err := multiTenancyConfig.Validate(); err != nil {
			return nil, fmt.Errorf("multi-tenancy config: %w", err)
		}
		multiTenancy, err = multi_tenancy.NewMultiTenancy(multiTenancyConfig)
		if err != nil {
			return nil, fmt.Errorf("new multi-tenancy: %w", err)
		}
		// This will be used to check authority of tokens before the request is passed down the tree. We can check tokens
		// in respective functions but then problem arises for /query endpoint. Hence, its better to authorize in start itself.
	}
	cfg.APICfg.MultiTenancy = multiTenancy // If multi-tenancy is disabled, the noopMultiTenancy will be used.

	// client has to be initiated after migrate since migrate
	// can change database GUC settings
	client, err := pgclient.NewClient(&cfg.PgmodelCfg, multiTenancy, leasingFunction)
	if err != nil {
		return nil, fmt.Errorf("client creation error: %w", err)
	}

	return client, nil
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

func initElector(cfg *Config, metrics *api.Metrics) (*util.Elector, error) {
	if cfg.HaGroupLockID == 0 {
		return nil, nil
	}
	if cfg.PrometheusTimeout == -1 {
		return nil, fmt.Errorf("Prometheus timeout configuration must be set when using PG advisory lock")
	}

	connStr, err := cfg.PgmodelCfg.GetConnectionStr()
	if err != nil {
		return nil, err
	}
	lock, err := util.NewPgLeaderLock(cfg.HaGroupLockID, connStr, getSchemaLease)
	if err != nil {
		return nil, fmt.Errorf("Error creating advisory lock\nhaGroupLockId: %d\nerr: %s\n", cfg.HaGroupLockID, err)
	}
	log.Info("msg", "Initializing leader election based on PostgreSQL advisory lock")
	scheduledElector := util.NewScheduledElector(lock, cfg.ElectionInterval)
	if cfg.PrometheusTimeout != 0 {
		go func() {
			ticker := time.NewTicker(promLivenessCheck)
			for range ticker.C {
				lastReq := atomic.LoadInt64(&metrics.LastRequestUnixNano)
				scheduledElector.PrometheusLivenessCheck(lastReq, cfg.PrometheusTimeout)
			}
		}()
	}
	return &scheduledElector.Elector, nil
}

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
			return migrationLockError
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
		return fmt.Errorf("Error while trying to migrate DB: %w", err)
	}

	installedPromscaleExtension, err := extension.InstallUpgradePromscaleExtensions(conn, extOptions)
	if err != nil {
		return err
	}

	pgmodel.UpdateTelemetry(conn, appVersion, installedPromscaleExtension)
	return nil
}

func compileAnchoredRegexString(s string) (*regexp.Regexp, error) {
	r, err := regexp.Compile("^(?:" + s + ")$")
	if err != nil {
		return nil, err
	}
	return r, nil
}

// Except for migration, every connection that communicates with the DB must be
// guarded by an instante of the schema-version lease to ensure that no other
// connector can migrate the DB out from under it. We do not bother to release
// said lease; in such and event the connector will be shutdown anyway, and
// connection-death will close the connection.
func getSchemaLease(ctx context.Context, conn *pgx.Conn) error {
	err := util.GetSharedLease(ctx, conn, schema.LockID)
	if err != nil {
		return err
	}
	return pgmodel.CheckSchemaVersion(ctx, conn, appVersion, false)
}
