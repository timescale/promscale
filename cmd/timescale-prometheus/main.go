// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package main

// Based on the Prometheus remote storage example:
// documentation/examples/remote_storage/remote_storage_adapter/main.go

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	pprof "net/http/pprof"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	pgx "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jamiealquiza/envy"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/timescale/timescale-prometheus/pkg/api"
	"github.com/timescale/timescale-prometheus/pkg/log"
	"github.com/timescale/timescale-prometheus/pkg/pgclient"
	"github.com/timescale/timescale-prometheus/pkg/pgmodel"
	"github.com/timescale/timescale-prometheus/pkg/util"
	"github.com/timescale/timescale-prometheus/pkg/version"
)

type config struct {
	listenAddr        string
	telemetryPath     string
	pgmodelCfg        pgclient.Config
	logLevel          string
	haGroupLockID     int64
	restElection      bool
	prometheusTimeout time.Duration
	electionInterval  time.Duration
	migrate           bool
	stopAfterMigrate  bool
	useVersionLease   bool
	corsOrigin        *regexp.Regexp
}

const (
	promLivenessCheck = time.Second
	schemaLockId      = 0x4D829C732AAFCEDE // chosen randomly
)

var (
	elector            *util.Elector
	appVersion         = pgmodel.VersionInfo{Version: version.Version, CommitHash: version.CommitHash}
	migrationLockError = fmt.Errorf("Could not acquire migration lock. Ensure there are no other connectors running and try again.")
)

func main() {
	cfg, err := parseFlags()
	if err != nil {
		fmt.Println("Version: ", version.Version, "Commit Hash: ", version.CommitHash)
		fmt.Println("Fatal error: cannot parse flags ", err)
		os.Exit(1)
	}
	err = log.Init(cfg.logLevel)
	if err != nil {
		fmt.Println("Version: ", version.Version, "Commit Hash: ", version.CommitHash)
		fmt.Println("Fatal error: cannot start logger", err)
		os.Exit(1)
	}
	log.Info("msg", "Version:"+version.Version+"; Commit Hash: "+version.CommitHash)
	log.Info("config", util.MaskPassword(fmt.Sprintf("%+v", cfg)))

	promMetrics := api.InitMetrics()

	client, err := createClient(cfg, promMetrics)
	if err != nil {
		log.Error("msg", "aborting startup due to error", "err", util.MaskPassword(err.Error()))
		os.Exit(1)
	}

	if client == nil {
		os.Exit(0)
	}

	defer client.Close()

	apiConf := &api.Config{AllowedOrigin: cfg.corsOrigin}
	router := api.GenerateRouter(apiConf, promMetrics, client, elector)

	log.Info("msg", "Starting up...")
	log.Info("msg", "Listening", "addr", cfg.listenAddr)

	mux := http.NewServeMux()
	mux.Handle("/", router)
	mux.Handle(cfg.telemetryPath, promhttp.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	err = http.ListenAndServe(cfg.listenAddr, mux)

	if err != nil {
		log.Error("msg", "Listen failure", "err", err)
		os.Exit(1)
	}
}

func parseFlags() (*config, error) {

	cfg := &config{}

	pgclient.ParseFlags(&cfg.pgmodelCfg)

	flag.StringVar(&cfg.listenAddr, "web-listen-address", ":9201", "Address to listen on for web endpoints.")
	flag.StringVar(&cfg.telemetryPath, "web-telemetry-path", "/metrics", "Address to listen on for web endpoints.")

	var corsOriginFlag string
	var migrateOption string
	flag.StringVar(&corsOriginFlag, "web-cors-origin", ".*", `Regex for CORS origin. It is fully anchored. Example: 'https?://(domain1|domain2)\.com'`)
	flag.StringVar(&cfg.logLevel, "log-level", "debug", "The log level to use [ \"error\", \"warn\", \"info\", \"debug\" ].")
	flag.Int64Var(&cfg.haGroupLockID, "leader-election-pg-advisory-lock-id", 0, "Unique advisory lock id per adapter high-availability group. Set it if you want to use leader election implementation based on PostgreSQL advisory lock.")
	flag.DurationVar(&cfg.prometheusTimeout, "leader-election-pg-advisory-lock-prometheus-timeout", -1, "Adapter will resign if there are no requests from Prometheus within a given timeout (0 means no timeout). "+
		"Note: make sure that only one Prometheus instance talks to the adapter. Timeout value should be co-related with Prometheus scrape interval but add enough `slack` to prevent random flips.")
	flag.BoolVar(&cfg.restElection, "leader-election-rest", false, "Enable REST interface for the leader election")
	flag.DurationVar(&cfg.electionInterval, "scheduled-election-interval", 5*time.Second, "Interval at which scheduled election runs. This is used to select a leader and confirm that we still holding the advisory lock.")
	flag.StringVar(&migrateOption, "migrate", "true", "Update the Prometheus SQL to the latest version. Valid options are: [true, false, only]")
	flag.BoolVar(&cfg.useVersionLease, "use-schema-version-lease", true, "Prevent race conditions during migration")
	envy.Parse("TS_PROM")
	flag.Parse()

	corsOriginRegex, err := compileAnchoredRegexString(corsOriginFlag)
	if err != nil {
		err = fmt.Errorf("could not compile CORS regex string %v: %w", corsOriginFlag, err)
		return nil, err
	}
	cfg.corsOrigin = corsOriginRegex

	cfg.stopAfterMigrate = false
	if strings.EqualFold(migrateOption, "true") {
		cfg.migrate = true
	} else if strings.EqualFold(migrateOption, "false") {
		cfg.migrate = false
	} else if strings.EqualFold(migrateOption, "only") {
		cfg.migrate = true
		cfg.stopAfterMigrate = true
	} else {
		return nil, fmt.Errorf("Invalid option for migrate: %v. Valid options are [true, false, only]", migrateOption)
	}
	return cfg, nil
}

func createClient(cfg *config, promMetrics *api.Metrics) (*pgclient.Client, error) {
	var schemaVersionLease *util.PgAdvisoryLock
	if cfg.useVersionLease {
		// migration lock logic
		// we don't want to upgrade the schema version while we still have connectors
		// attached who think the schema is at the old version. To prevent this, as
		// best we can, each normal connection attempts to grab a (shared) advisory
		// lock on schemaLockId, and we attempt to grab an exclusive lock on it
		// before running migrate. This implies that migration must be run when no
		// other connector is running.
		schemaVersionLease, err := util.NewPgAdvisoryLock(schemaLockId, cfg.pgmodelCfg.GetConnectionStr())
		if err != nil {
			log.Error("msg", "error creating schema version lease", "err", err)
			os.Exit(1)
		}
		// after the client has started it's in charge of maintaining the leases
		defer schemaVersionLease.Close()
	}

	migration_success := true
	if cfg.migrate {
		err := migrate(&cfg.pgmodelCfg, appVersion, schemaVersionLease)
		migration_success = err != nil
		if err != nil && err != migrationLockError {
			return nil, fmt.Errorf("migration error: %w", err)
		}

		if cfg.stopAfterMigrate {
			if err != nil {
				return nil, err
			}
			log.Info("msg", "Migration successful, exiting")
			return nil, nil
		}
	} else {
		log.Info("msg", "Skipping migration")
	}

	if schemaVersionLease != nil {
		// Grab a lease to protect version checking and client creation. This
		// lease will be released when the schemaVersionLease is closed.
		locked, err := schemaVersionLease.GetSharedAdvisoryLock()
		if err != nil {
			return nil, fmt.Errorf("could not acquire schema version lease due to: %w", err)
		}
		if !locked {
			return nil, fmt.Errorf("could not acquire schema version lease. is a migration in progress?")
		}
	}

	// Check the database is on the correct version.
	// This must be done even if we attempted a migrate; if we failed to acquire
	// the lock we'll start up as-if we were never supposed to migrate in the
	// first place. This is also needed as checkDependencies populates our
	// extension metadata
	err := checkDependencies(&cfg.pgmodelCfg, appVersion)
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

	if elector == nil {
		log.Warn(
			"msg",
			"No adapter leader election. Group lock id is not set. "+
				"Possible duplicate write load if running multiple connectors",
		)
	}

	leasingFunction := getSchemaLease
	if !cfg.useVersionLease {
		leasingFunction = nil
	}
	// client has to be initiated after migrate since migrate
	// can change database GUC settings
	client, err := pgclient.NewClient(&cfg.pgmodelCfg, leasingFunction)
	if err != nil {
		return nil, fmt.Errorf("client creation error: %w", err)
	}

	return client, nil
}

func initElector(cfg *config, metrics *api.Metrics) (*util.Elector, error) {
	if cfg.restElection && cfg.haGroupLockID != 0 {
		return nil, fmt.Errorf("Use either REST or PgAdvisoryLock for the leader election")
	}
	if cfg.restElection {
		return util.NewElector(util.NewRestElection()), nil
	}
	if cfg.haGroupLockID == 0 {
		return nil, nil
	}
	if cfg.prometheusTimeout == -1 {
		return nil, fmt.Errorf("Prometheus timeout configuration must be set when using PG advisory lock")
	}

	lock, err := util.NewPgLeaderLock(cfg.haGroupLockID, cfg.pgmodelCfg.GetConnectionStr(), getSchemaLease)
	if err != nil {
		return nil, fmt.Errorf("Error creating advisory lock\nhaGroupLockId: %d\nerr: %s\n", cfg.haGroupLockID, err)
	}
	scheduledElector := util.NewScheduledElector(lock, cfg.electionInterval)
	log.Info("msg", "Initialized leader election based on PostgreSQL advisory lock")
	if cfg.prometheusTimeout != 0 {
		go func() {
			ticker := time.NewTicker(promLivenessCheck)
			for range ticker.C {
				lastReq := atomic.LoadInt64(&metrics.LastRequestUnixNano)
				scheduledElector.PrometheusLivenessCheck(lastReq, cfg.prometheusTimeout)
			}
		}()
	}
	return &scheduledElector.Elector, nil
}

func migrate(cfg *pgclient.Config, appVersion pgmodel.VersionInfo, leaseLock util.AdvisoryLock) error {
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
	}

	db, err := pgxpool.Connect(context.Background(), cfg.GetConnectionStr())
	if err != nil {
		return fmt.Errorf("Error while trying to open DB connection: %w", err)
	}
	defer db.Close()

	err = pgmodel.Migrate(db, appVersion)

	if err != nil {
		return fmt.Errorf("Error while trying to migrate DB: %w", err)
	}

	return nil
}

func checkDependencies(cfg *pgclient.Config, appVersion pgmodel.VersionInfo) error {
	db, err := pgxpool.Connect(context.Background(), cfg.GetConnectionStr())
	if err != nil {
		return fmt.Errorf("Error while trying to open DB connection: %w", err)
	}
	defer db.Close()

	return pgmodel.CheckDependencies(db, appVersion)
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
