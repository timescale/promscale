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
	haGroupLockID     int
	restElection      bool
	prometheusTimeout time.Duration
	electionInterval  time.Duration
	migrate           bool
	stopAfterMigrate  bool
	corsOrigin        *regexp.Regexp
}

const (
	promLivenessCheck = time.Second
)

var (
	elector *util.Elector
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
	elector, err = initElector(cfg, promMetrics)

	if err != nil {
		errStr := fmt.Sprintf("Aborting startup because of elector init error: %s", util.MaskPassword(err.Error()))
		log.Error("msg", errStr)
		os.Exit(1)
	}

	if elector == nil {
		log.Warn(
			"msg",
			"No adapter leader election. Group lock id is not set. "+
				"Possible duplicate write load if running adapter in high-availability mode",
		)
	}

	appVersion := pgmodel.VersionInfo{Version: version.Version, CommitHash: version.CommitHash}

	// migrate has to happen after elector started
	if cfg.migrate {
		err = migrate(&cfg.pgmodelCfg, appVersion, promMetrics)

		if err != nil {
			log.Error("msg", fmt.Sprintf("Aborting startup because of migration error: %s", util.MaskPassword(err.Error())))
			os.Exit(1)
		}
		if cfg.stopAfterMigrate {
			log.Info("msg", "Migration successful, exiting")
			os.Exit(0)
		}
	} else {
		log.Info("msg", "Skipping migration")
	}

	if err := checkDependencies(&cfg.pgmodelCfg, appVersion); err != nil {
		log.Error("msg", fmt.Sprintf("Aborting startup because of dependency error: %s", util.MaskPassword(err.Error())))
		os.Exit(1)
	}

	// client has to be initiated after migrate since migrate
	// can change database GUC settings
	client, err := pgclient.NewClient(&cfg.pgmodelCfg)
	if err != nil {
		log.Error(util.MaskPassword(err.Error()))
		os.Exit(1)
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
	flag.IntVar(&cfg.haGroupLockID, "leader-election-pg-advisory-lock-id", 0, "Unique advisory lock id per adapter high-availability group. Set it if you want to use leader election implementation based on PostgreSQL advisory lock.")
	flag.DurationVar(&cfg.prometheusTimeout, "leader-election-pg-advisory-lock-prometheus-timeout", -1, "Adapter will resign if there are no requests from Prometheus within a given timeout (0 means no timeout). "+
		"Note: make sure that only one Prometheus instance talks to the adapter. Timeout value should be co-related with Prometheus scrape interval but add enough `slack` to prevent random flips.")
	flag.BoolVar(&cfg.restElection, "leader-election-rest", false, "Enable REST interface for the leader election")
	flag.DurationVar(&cfg.electionInterval, "scheduled-election-interval", 5*time.Second, "Interval at which scheduled election runs. This is used to select a leader and confirm that we still holding the advisory lock.")
	flag.StringVar(&migrateOption, "migrate", "true", "Update the Prometheus SQL to the latest version. Valid options are: [true, false, only]")
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
	lock, err := util.NewPgAdvisoryLock(cfg.haGroupLockID, cfg.pgmodelCfg.GetConnectionStr())
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

func migrate(cfg *pgclient.Config, appVersion pgmodel.VersionInfo, metrics *api.Metrics) error {
	shouldWrite, err := isWriter()
	if err != nil {
		metrics.LeaderGauge.Set(0)
		return fmt.Errorf("isWriter check failed: %w", err)
	}
	if !shouldWrite {
		metrics.LeaderGauge.Set(0)
		log.Debug("msg", fmt.Sprintf("Election id %v: Instance is not a leader. Won't update", elector.ID()))
		return nil
	}

	metrics.LeaderGauge.Set(1)
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

func isWriter() (bool, error) {
	if elector != nil {
		shouldWrite, err := elector.IsLeader()
		return shouldWrite, err
	}
	return true, nil
}

func compileAnchoredRegexString(s string) (*regexp.Regexp, error) {
	r, err := regexp.Compile("^(?:" + s + ")$")
	if err != nil {
		return nil, err
	}
	return r, nil
}
