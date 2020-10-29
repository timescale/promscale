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
	pprof "net/http/pprof"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	pgx "github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jamiealquiza/envy"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/timescale/promscale/pkg/api"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/pgmodel"
	"github.com/timescale/promscale/pkg/util"
	"github.com/timescale/promscale/pkg/version"
)

type Config struct {
	ListenAddr         string
	TelemetryPath      string
	PgmodelCfg         pgclient.Config
	LogCfg             log.Config
	HaGroupLockID      int64
	PrometheusTimeout  time.Duration
	ElectionInterval   time.Duration
	Migrate            bool
	StopAfterMigrate   bool
	UseVersionLease    bool
	CorsOrigin         *regexp.Regexp
	InstallTimescaleDB bool
	ReadOnly           bool
	AdminAPIEnabled    bool
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
	pgclient.ParseFlags(&cfg.PgmodelCfg)
	log.ParseFlags(&cfg.LogCfg)

	flag.StringVar(&cfg.ListenAddr, "web-listen-address", ":9201", "Address to listen on for web endpoints.")
	flag.StringVar(&cfg.TelemetryPath, "web-telemetry-path", "/metrics", "Web endpoint for exposing Promscale's Prometheus metrics.")

	var corsOriginFlag string
	var migrateOption string
	flag.StringVar(&corsOriginFlag, "web-cors-origin", ".*", `Regex for CORS origin. It is fully anchored. Example: 'https?://(domain1|domain2)\.com'`)
	flag.Int64Var(&cfg.HaGroupLockID, "leader-election-pg-advisory-lock-id", 0, "Leader-election based high-availability. It is based on PostgreSQL advisory lock and requires a unique advisory lock ID per high-availability group. Only a single connector in each high-availability group will write data at one time. A value of 0 disables leader election.")
	flag.DurationVar(&cfg.PrometheusTimeout, "leader-election-pg-advisory-lock-prometheus-timeout", -1, "Prometheus timeout duration for leader-election high-availability. The connector will resign if the associated Prometheus instance does not respond within the given timeout. This value should be a low multiple of the Prometheus scrape interval, big enough to prevent random flips.")
	flag.DurationVar(&cfg.ElectionInterval, "leader-election-scheduled-interval", 5*time.Second, "Interval at which scheduled election runs. This is used to select a leader and confirm that we still holding the advisory lock.")
	flag.StringVar(&migrateOption, "migrate", "true", "Update the Prometheus SQL schema to the latest version. Valid options are: [true, false, only].")
	flag.BoolVar(&cfg.UseVersionLease, "use-schema-version-lease", true, "Use schema version lease to prevent race conditions during migration.")
	flag.BoolVar(&cfg.InstallTimescaleDB, "install-timescaledb", true, "Install or update TimescaleDB extension.")
	flag.BoolVar(&cfg.ReadOnly, "read-only", false, "Read-only mode for the connector. Operations related to writing or updating the database are disallowed. It is used when pointing the connector to a TimescaleDB read replica.")
	flag.BoolVar(&cfg.AdminAPIEnabled, "web-enable-admin-api", false, "Allow operations via API that are for advanced users. Currently, these operations are limited to deletion of series.")
	envy.Parse("TS_PROM")
	// Ignore errors; CommandLine is set for ExitOnError.
	_ = flag.CommandLine.Parse(args)

	corsOriginRegex, err := compileAnchoredRegexString(corsOriginFlag)
	if err != nil {
		err = fmt.Errorf("could not compile CORS regex string %v: %w", corsOriginFlag, err)
		return nil, err
	}
	cfg.CorsOrigin = corsOriginRegex

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

	if cfg.ReadOnly {
		flagset := make(map[string]bool)
		flag.Visit(func(f *flag.Flag) { flagset[f.Name] = true })
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
		cfg.InstallTimescaleDB = false
	}
	return cfg, nil
}

func Run(cfg *Config) error {
	log.Info("msg", "Version:"+version.Version+"; Commit Hash: "+version.CommitHash)
	log.Info("config", util.MaskPassword(fmt.Sprintf("%+v", cfg)))

	if cfg.ReadOnly {
		log.Info("msg", "Migrations disabled for read-only mode")
	}

	promMetrics := api.InitMetrics()

	client, err := CreateClient(cfg, promMetrics)
	if err != nil {
		log.Error("msg", "aborting startup due to error", "err", util.MaskPassword(err.Error()))
		return startupError
	}

	if client == nil {
		return nil
	}

	defer client.Close()

	apiConf := &api.Config{
		AllowedOrigin:   cfg.CorsOrigin,
		ReadOnly:        cfg.ReadOnly,
		AdminAPIEnabled: cfg.AdminAPIEnabled,
	}
	router := api.GenerateRouter(apiConf, promMetrics, client, elector)

	log.Info("msg", "Starting up...")
	log.Info("msg", "Listening", "addr", cfg.ListenAddr)

	mux := http.NewServeMux()
	mux.Handle("/", router)
	mux.Handle(cfg.TelemetryPath, promhttp.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	err = http.ListenAndServe(cfg.ListenAddr, mux)

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
	if cfg.InstallTimescaleDB {
		err := pgmodel.MigrateTimescaleDBExtension(cfg.PgmodelCfg.GetConnectionStr())
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
	schemaVersionLease, err := util.NewPgAdvisoryLock(schemaLockId, cfg.PgmodelCfg.GetConnectionStr())
	if err != nil {
		log.Error("msg", "error creating schema version lease", "err", err)
		return nil, startupError
	}
	// after the client has started it's in charge of maintaining the leases
	defer schemaVersionLease.Close()

	migration_success := true
	if cfg.Migrate {
		conn, err := schemaVersionLease.Conn()
		if err != nil {
			return nil, fmt.Errorf("migration error: %w", err)
		}
		lease := schemaVersionLease
		if !cfg.UseVersionLease {
			lease = nil
		}
		err = migrate(conn, appVersion, lease)
		migration_success = err != nil
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
	err = pgmodel.CheckDependencies(conn, appVersion)
	if err != nil {
		err = fmt.Errorf("dependency error: %w", err)
		if !migration_success {
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

	if elector == nil && !cfg.ReadOnly {
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

	lock, err := util.NewPgLeaderLock(cfg.HaGroupLockID, cfg.PgmodelCfg.GetConnectionStr(), getSchemaLease)
	if err != nil {
		return nil, fmt.Errorf("Error creating advisory lock\nhaGroupLockId: %d\nerr: %s\n", cfg.HaGroupLockID, err)
	}
	scheduledElector := util.NewScheduledElector(lock, cfg.ElectionInterval)
	log.Info("msg", "Initialized leader election based on PostgreSQL advisory lock")
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

func migrate(conn *pgx.Conn, appVersion pgmodel.VersionInfo, leaseLock *util.PgAdvisoryLock) error {
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

	err := pgmodel.Migrate(conn, appVersion)

	if err != nil {
		return fmt.Errorf("Error while trying to migrate DB: %w", err)
	}

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
	return pgmodel.CheckSchemaVersion(ctx, conn, appVersion)
}
