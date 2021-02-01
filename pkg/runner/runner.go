// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package runner

// Based on the Prometheus remote storage example:
// documentation/examples/remote_storage/remote_storage_adapter/main.go

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	pgx "github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/peterbourgon/ff/v3"
	"github.com/peterbourgon/ff/v3/ffyaml"
	"github.com/timescale/promscale/pkg/api"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/pgmodel"
	"github.com/timescale/promscale/pkg/pgmodel/common/extension"
	"github.com/timescale/promscale/pkg/util"
	"github.com/timescale/promscale/pkg/version"
)

type Config struct {
	ListenAddr                  string
	PgmodelCfg                  pgclient.Config
	LogCfg                      log.Config
	APICfg                      api.Config
	ConfigFile                  string
	TLSCertFile                 string
	TLSKeyFile                  string
	HaGroupLockID               int64
	PrometheusTimeout           time.Duration
	ElectionInterval            time.Duration
	Migrate                     bool
	StopAfterMigrate            bool
	UseVersionLease             bool
	InstallExtensions           bool
	UpgradeExtensions           bool
	UpgradePrereleaseExtensions bool
}

const (
	promLivenessCheck = time.Second
	schemaLockId      = 0x4D829C732AAFCEDE // chosen randomly
)

var (
	elector            *util.Elector
	appVersion         = pgmodel.VersionInfo{Version: version.Version, CommitHash: version.CommitHash}
	migrationLockError = fmt.Errorf("Could not acquire migration lock. Ensure there are no other connectors running and try again.")
	startupError       = fmt.Errorf("startup error")
)

func ParseFlags(cfg *Config, args []string) (*Config, error) {
	var (
		fs             = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
		corsOriginFlag string
		migrateOption  string
	)

	pgclient.ParseFlags(fs, &cfg.PgmodelCfg)
	log.ParseFlags(fs, &cfg.LogCfg)
	api.ParseFlags(fs, &cfg.APICfg)

	fs.StringVar(&cfg.ConfigFile, "config", "config.yml", "YAML configuration file path for Promscale.")
	fs.StringVar(&cfg.ListenAddr, "web-listen-address", ":9201", "Address to listen on for web endpoints.")
	fs.StringVar(&corsOriginFlag, "web-cors-origin", ".*", `Regex for CORS origin. It is fully anchored. Example: 'https?://(domain1|domain2)\.com'`)
	fs.Int64Var(&cfg.HaGroupLockID, "leader-election-pg-advisory-lock-id", 0, "Leader-election based high-availability. It is based on PostgreSQL advisory lock and requires a unique advisory lock ID per high-availability group. Only a single connector in each high-availability group will write data at one time. A value of 0 disables leader election.")
	fs.DurationVar(&cfg.PrometheusTimeout, "leader-election-pg-advisory-lock-prometheus-timeout", -1, "Prometheus timeout duration for leader-election high-availability. The connector will resign if the associated Prometheus instance does not respond within the given timeout. This value should be a low multiple of the Prometheus scrape interval, big enough to prevent random flips.")
	fs.DurationVar(&cfg.ElectionInterval, "leader-election-scheduled-interval", 5*time.Second, "Interval at which scheduled election runs. This is used to select a leader and confirm that we still holding the advisory lock.")
	fs.StringVar(&migrateOption, "migrate", "true", "Update the Prometheus SQL schema to the latest version. Valid options are: [true, false, only].")
	fs.BoolVar(&cfg.UseVersionLease, "use-schema-version-lease", true, "Use schema version lease to prevent race conditions during migration.")
	fs.BoolVar(&cfg.InstallExtensions, "install-extensions", true, "Install TimescaleDB, Promscale extension.")
	fs.BoolVar(&cfg.UpgradeExtensions, "upgrade-extensions", true, "Upgrades TimescaleDB, Promscale extensions.")
	fs.BoolVar(&cfg.UpgradePrereleaseExtensions, "upgrade-prerelease-extensions", false, "Upgrades to pre-release TimescaleDB, Promscale extensions.")
	fs.StringVar(&cfg.TLSCertFile, "tls-cert-file", "", "TLS Certificate file for web server, leave blank to disable TLS.")
	fs.StringVar(&cfg.TLSKeyFile, "tls-key-file", "", "TLS Key file for web server, leave blank to disable TLS.")

	util.ParseEnv("PROMSCALE", fs)
	// Deprecated: TS_PROM is the old prefix which is deprecated and in here
	// for legacy compatibility. Will be removed in the future. PROMSCALE prefix
	// takes precedence and will be used if the same variable with both prefixes
	// exist.
	util.ParseEnv("TS_PROM", fs)

	if err := ff.Parse(fs, args,
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ffyaml.Parser),
		ff.WithAllowMissingConfigFile(true),
	); err != nil {
		return nil, fmt.Errorf("configuration error: %w", err)
	}

	// Checking if TLS files are not both set or both empty.
	if (cfg.TLSCertFile != "") != (cfg.TLSKeyFile != "") {
		return nil, fmt.Errorf("both TLS Ceriticate File and TLS Key File need to be provided for a valid TLS configuration")
	}

	corsOriginRegex, err := compileAnchoredRegexString(corsOriginFlag)
	if err != nil {
		return nil, fmt.Errorf("could not compile CORS regex string %v: %w", corsOriginFlag, err)
	}
	cfg.APICfg.AllowedOrigin = corsOriginRegex

	if err := api.Validate(&cfg.APICfg); err != nil {
		return nil, fmt.Errorf("error validating API configuration: %w", err)
	}

	cfg.StopAfterMigrate = false
	if strings.EqualFold(migrateOption, "true") {
		cfg.Migrate = true
	} else if strings.EqualFold(migrateOption, "false") {
		cfg.Migrate = false
	} else if strings.EqualFold(migrateOption, "only") {
		cfg.Migrate = true
		cfg.StopAfterMigrate = true
	} else {
		return nil, fmt.Errorf("Invalid option for migrate: %v. Valid options are [true, false, only]", migrateOption)
	}

	if cfg.APICfg.ReadOnly {
		flagset := make(map[string]bool)
		fs.Visit(func(f *flag.Flag) { flagset[f.Name] = true })
		if flagset["migrate"] || flagset["use-schema-version-lease"] {
			return nil, fmt.Errorf("Migration flags not supported in read-only mode")
		}
		if flagset["install-timescaledb"] {
			return nil, fmt.Errorf("Cannot install or update TimescaleDB extension in read-only mode")
		}
		if flagset["leader-election-pg-advisory-lock-id"] {
			return nil, fmt.Errorf("Invalid option for HA group lock ID, cannot enable HA mode and read-only mode")
		}
		cfg.Migrate = false
		cfg.StopAfterMigrate = false
		cfg.UseVersionLease = false
		cfg.InstallExtensions = false
		cfg.UpgradeExtensions = false
	}

	cfg.PgmodelCfg.UsesHA = cfg.HaGroupLockID != 0
	return cfg, nil
}

func Run(cfg *Config) error {
	log.Info("msg", "Version:"+version.Version+"; Commit Hash: "+version.CommitHash)
	log.Info("config", util.MaskPassword(fmt.Sprintf("%+v", cfg)))

	if cfg.APICfg.ReadOnly {
		log.Info("msg", "Migrations disabled for read-only mode")
	}

	promMetrics := api.InitMetrics(cfg.PgmodelCfg.ReportInterval)

	client, err := CreateClient(cfg, promMetrics)
	if err != nil {
		log.Error("msg", "aborting startup due to error", "err", util.MaskPassword(err.Error()))
		return startupError
	}

	if client == nil {
		return nil
	}

	defer client.Close()

	router, err := api.GenerateRouter(&cfg.APICfg, promMetrics, client, elector)
	if err != nil {
		log.Error("msg", "aborting startup due to error", "err", fmt.Sprintf("generate router: %s", err.Error()))
		return fmt.Errorf("generate router: %w", err)
	}

	log.Info("msg", "Starting up...")
	log.Info("msg", "Listening", "addr", cfg.ListenAddr)

	mux := http.NewServeMux()
	mux.Handle("/", router)

	if cfg.TLSCertFile != "" {
		err = http.ListenAndServeTLS(cfg.ListenAddr, cfg.TLSCertFile, cfg.TLSKeyFile, mux)
	} else {
		err = http.ListenAndServe(cfg.ListenAddr, mux)
	}

	if err != nil {
		log.Error("msg", "Listen failure", "err", err)
		return startupError
	}

	return nil
}

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
	schemaVersionLease, err := util.NewPgAdvisoryLock(schemaLockId, connStr)
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
	// client has to be initiated after migrate since migrate
	// can change database GUC settings
	client, err := pgclient.NewClient(&cfg.PgmodelCfg, leasingFunction)
	if err != nil {
		return nil, fmt.Errorf("client creation error: %w", err)
	}

	return client, nil
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
	err := util.GetSharedLease(ctx, conn, schemaLockId)
	if err != nil {
		return err
	}
	return pgmodel.CheckSchemaVersion(ctx, conn, appVersion, false)
}
