// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package main

// Based on the Prometheus remote storage example:
// documentation/examples/remote_storage/remote_storage_adapter/main.go

import (
	"database/sql"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync/atomic"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"

	"github.com/timescale/timescale-prometheus/pkg/api"
	"github.com/timescale/timescale-prometheus/pkg/log"
	"github.com/timescale/timescale-prometheus/pkg/pgclient"
	"github.com/timescale/timescale-prometheus/pkg/pgmodel"
	"github.com/timescale/timescale-prometheus/pkg/util"

	"github.com/jamiealquiza/envy"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/timescale/timescale-prometheus/pkg/query"
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
}

const (
	tickInterval      = time.Second
	promLivenessCheck = time.Second
	promNamespace     = "ts_prom"
)

var (
	leaderGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: promNamespace,
			Name:      "current_leader",
			Help:      "Shows current election leader status",
		},
	)
	receivedSamples = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: promNamespace,
			Name:      "received_samples_total",
			Help:      "Total number of received samples.",
		},
	)
	receivedQueries = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: promNamespace,
			Name:      "received_queries_total",
			Help:      "Total number of received queries.",
		},
	)
	sentSamples = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: promNamespace,
			Name:      "sent_samples_total",
			Help:      "Total number of processed samples sent to remote storage.",
		},
	)
	failedSamples = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: promNamespace,
			Name:      "failed_samples_total",
			Help:      "Total number of processed samples which failed on send to remote storage.",
		},
	)
	failedQueries = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: promNamespace,
			Name:      "failed_queries_total",
			Help:      "Total number of queries which failed on send to remote storage.",
		},
	)
	sentBatchDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: promNamespace,
			Name:      "sent_batch_duration_seconds",
			Help:      "Duration of sample batch send calls to the remote storage.",
			Buckets:   prometheus.DefBuckets,
		},
	)
	queryBatchDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: promNamespace,
			Name:      "query_batch_duration_seconds",
			Help:      "Duration of query batch read calls to the remote storage.",
			Buckets:   prometheus.DefBuckets,
		},
	)
	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: promNamespace,
			Name:      "http_request_duration_ms",
			Help:      "Duration of HTTP request in milliseconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"path"},
	)
	ReadQueryLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: promNamespace,
			Name:      "query_latency_ms",
			Help:      "Query latency in milliseconds",
			Buckets:   []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
		},
		[]string{"query"},
	)
	writeThroughput     = util.NewThroughputCalc(tickInterval)
	elector             *util.Elector
	lastRequestUnixNano = time.Now().UnixNano()
)

func init() {
	prometheus.MustRegister(leaderGauge)
	prometheus.MustRegister(receivedSamples)
	prometheus.MustRegister(receivedQueries)
	prometheus.MustRegister(sentSamples)
	prometheus.MustRegister(failedSamples)
	prometheus.MustRegister(failedQueries)
	prometheus.MustRegister(sentBatchDuration)
	prometheus.MustRegister(queryBatchDuration)
	prometheus.MustRegister(httpRequestDuration)
	prometheus.MustRegister(ReadQueryLatency)
	writeThroughput.Start()
}

func main() {
	cfg := parseFlags()
	err := log.Init(cfg.logLevel)
	if err != nil {
		fmt.Println("Version: ", Version, "Commit Hash: ", CommitHash)
		fmt.Println("Fatal error: cannot start logger", err)
		os.Exit(1)
	}
	log.Info("msg", "Version:"+Version+"; Commit Hash: "+CommitHash)
	log.Info("config", util.MaskPassword(fmt.Sprintf("%+v", cfg)))
	http.Handle(cfg.telemetryPath, promhttp.Handler())

	elector, err = initElector(cfg)

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

	// migrate has to happen after elector started
	if cfg.migrate {
		err = migrate(&cfg.pgmodelCfg)

		if err != nil {
			log.Error("msg", fmt.Sprintf("Aborting startup because of migration error: %s", util.MaskPassword(err.Error())))
			os.Exit(1)
		}
	}

	// client has to be initiated after migrate since migrate
	// can change database GUC settings
	client, err := pgclient.NewClient(&cfg.pgmodelCfg, ReadQueryLatency)
	if err != nil {
		log.Error(util.MaskPassword(err.Error()))
		os.Exit(1)
	}
	defer client.Close()
	queryable := client.GetQueryable()
	queryEngine := query.NewEngine(log.GetLogger(), time.Minute)
	queryHandler := timeHandler(httpRequestDuration, "query", api.Query(queryEngine, queryable))
	queryRangeHandler := timeHandler(httpRequestDuration, "query_range", api.QueryRange(queryEngine, queryable))
	promMetrics := api.Metrics{
		LeaderGauge:         leaderGauge,
		ReceivedSamples:     receivedSamples,
		FailedSamples:       failedSamples,
		SentSamples:         sentSamples,
		SentBatchDuration:   sentBatchDuration,
		WriteThroughput:     writeThroughput,
		LastRequestUnixNano: lastRequestUnixNano,
		QueryBatchDuration:  queryBatchDuration,
		FailedQueries:       failedQueries,
		ReceivedQueries:     receivedQueries,
	}
	writeHandler := timeHandler(httpRequestDuration, "write", api.Write(client, elector, &promMetrics))
	http.Handle("/write", writeHandler)
	http.Handle("/read", timeHandler(httpRequestDuration, "read", api.Read(client, &promMetrics)))
	http.Handle("/query", queryHandler)
	http.Handle("/query_range", queryRangeHandler)
	http.Handle("/api/v1/query", queryHandler)
	http.Handle("/api/v1/query_range", queryRangeHandler)
	http.Handle("/healthz", api.Health(client))

	log.Info("msg", "Starting up...")
	log.Info("msg", "Listening", "addr", cfg.listenAddr)

	err = http.ListenAndServe(cfg.listenAddr, nil)

	if err != nil {
		log.Error("msg", "Listen failure", "err", err)
		os.Exit(1)
	}
}

func parseFlags() *config {

	cfg := &config{}

	pgclient.ParseFlags(&cfg.pgmodelCfg)

	flag.StringVar(&cfg.listenAddr, "web-listen-address", ":9201", "Address to listen on for web endpoints.")
	flag.StringVar(&cfg.telemetryPath, "web-telemetry-path", "/metrics", "Address to listen on for web endpoints.")
	flag.StringVar(&cfg.logLevel, "log-level", "debug", "The log level to use [ \"error\", \"warn\", \"info\", \"debug\" ].")
	flag.IntVar(&cfg.haGroupLockID, "leader-election-pg-advisory-lock-id", 0, "Unique advisory lock id per adapter high-availability group. Set it if you want to use leader election implementation based on PostgreSQL advisory lock.")
	flag.DurationVar(&cfg.prometheusTimeout, "leader-election-pg-advisory-lock-prometheus-timeout", -1, "Adapter will resign if there are no requests from Prometheus within a given timeout (0 means no timeout). "+
		"Note: make sure that only one Prometheus instance talks to the adapter. Timeout value should be co-related with Prometheus scrape interval but add enough `slack` to prevent random flips.")
	flag.BoolVar(&cfg.restElection, "leader-election-rest", false, "Enable REST interface for the leader election")
	flag.DurationVar(&cfg.electionInterval, "scheduled-election-interval", 5*time.Second, "Interval at which scheduled election runs. This is used to select a leader and confirm that we still holding the advisory lock.")
	flag.BoolVar(&cfg.migrate, "migrate", true, "Update the Prometheus SQL to the latest version")
	envy.Parse("TS_PROM")
	flag.Parse()

	return cfg
}

func initElector(cfg *config) (*util.Elector, error) {
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
				lastReq := atomic.LoadInt64(&lastRequestUnixNano)
				scheduledElector.PrometheusLivenessCheck(lastReq, cfg.prometheusTimeout)
			}
		}()
	}
	return &scheduledElector.Elector, nil
}

func migrate(cfg *pgclient.Config) error {
	shouldWrite, err := isWriter()
	if err != nil {
		leaderGauge.Set(0)
		return fmt.Errorf("isWriter check failed: %w", err)
	}
	if !shouldWrite {
		leaderGauge.Set(0)
		log.Debug("msg", fmt.Sprintf("Election id %v: Instance is not a leader. Won't update", elector.ID()))
		return nil
	}

	leaderGauge.Set(1)
	dbStd, err := sql.Open("pgx", cfg.GetConnectionStr())
	if err != nil {
		return fmt.Errorf("Error while trying to open DB connection: %w", err)
	}
	defer func() {
		err := dbStd.Close()
		if err != nil {
			log.Error("msg", "Error while trying to close DB connection: %s", err)
		}
	}()

	err = pgmodel.Migrate(dbStd, pgmodel.VersionInfo{Version: Version, CommitHash: CommitHash})

	if err != nil {
		return fmt.Errorf("Error while trying to migrate DB: %w", err)
	}

	return nil
}

func isWriter() (bool, error) {
	if elector != nil {
		shouldWrite, err := elector.IsLeader()
		return shouldWrite, err
	}
	return true, nil
}

// timeHandler uses Prometheus histogram to track request time
func timeHandler(histogramVec prometheus.ObserverVec, path string, handler http.Handler) http.Handler {
	f := func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		handler.ServeHTTP(w, r)
		elapsedMs := time.Since(start).Milliseconds()
		histogramVec.WithLabelValues(path).Observe(float64(elapsedMs))
	}
	return http.HandlerFunc(f)
}
