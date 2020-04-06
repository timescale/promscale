// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package main

// Based on the Prometheus remote storage example:
// documentation/examples/remote_storage/remote_storage_adapter/main.go

import (
	"database/sql"
	"flag"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync/atomic"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"

	"github.com/timescale/timescale-prometheus/pkg/log"
	"github.com/timescale/timescale-prometheus/pkg/pgclient"
	"github.com/timescale/timescale-prometheus/pkg/pgmodel"
	"github.com/timescale/timescale-prometheus/pkg/util"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/jamiealquiza/envy"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/prompb"

	"fmt"
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
)

var (
	receivedSamples = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "received_samples_total",
			Help: "Total number of received samples.",
		},
	)
	receivedQueries = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "received_queries_total",
			Help: "Total number of received queries.",
		},
	)
	sentSamples = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "sent_samples_total",
			Help: "Total number of processed samples sent to remote storage.",
		},
	)
	failedSamples = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "failed_samples_total",
			Help: "Total number of processed samples which failed on send to remote storage.",
		},
	)
	failedQueries = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "failed_queries_total",
			Help: "Total number of queries which failed on send to remote storage.",
		},
	)
	sentBatchDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "sent_batch_duration_seconds",
			Help:    "Duration of sample batch send calls to the remote storage.",
			Buckets: prometheus.DefBuckets,
		},
	)
	queryBatchDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "query_batch_duration_seconds",
			Help:    "Duration of query batch read calls to the remote storage.",
			Buckets: prometheus.DefBuckets,
		},
	)
	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_ms",
			Help:    "Duration of HTTP request in milliseconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"path"},
	)
	writeThroughput     = util.NewThroughputCalc(tickInterval)
	elector             *util.Elector
	lastRequestUnixNano = time.Now().UnixNano()
)

func init() {
	prometheus.MustRegister(receivedSamples)
	prometheus.MustRegister(sentSamples)
	prometheus.MustRegister(failedSamples)
	prometheus.MustRegister(sentBatchDuration)
	prometheus.MustRegister(httpRequestDuration)
	writeThroughput.Start()
}

func main() {
	cfg := parseFlags()
	err := log.Init(cfg.logLevel)
	if err != nil {
		fmt.Println("Fatal error: cannot start logger", err)
		os.Exit(1)
	}
	log.Info("config", fmt.Sprintf("%+v", cfg))

	http.Handle(cfg.telemetryPath, promhttp.Handler())

	dbIsReadyProvider := pgclient.OnDbReady(&cfg.pgmodelCfg)
	// if database is not ready eventually -> exit
	dbIsReadyProvider.AddCallback(exitIfNoDatabaseCallback())
	// adding initElector as a callback
	// makes sure elector is ready before migration
	dbIsReadyProvider.AddCallback(initElectorCallback(cfg))

	if cfg.migrate {
		// makes sure migration is done after elector is created
		dbIsReadyProvider.AddCallback(createMigrateCallback(&cfg.pgmodelCfg))
	}
	// will complete after dbIsReadyProvider is done and all it's callbacks
	// are also done (makes sure elector and migration are complete)
	clientProvider := pgclient.NewClientProvider(&cfg.pgmodelCfg, dbIsReadyProvider)
	elector = initElector(cfg)

	http.Handle("/write", timeHandler("write", write(clientProvider)))
	http.Handle("/read", timeHandler("read", read(clientProvider)))
	http.Handle("/healthz", health(clientProvider))

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

func exitIfNoDatabaseCallback() util.CallbackOnEventualResult {
	return func(isDbReady interface{}) {
		if isDbReady == nil {
			log.Error("msg", "database is not ready, failed to connect after several attempts")
			os.Exit(1)
			return
		}
	}
}

func initElectorCallback(cfg *config) util.CallbackOnEventualResult {
	return func(interface{}) {
		initElector(cfg)
	}
}

func initElector(cfg *config) *util.Elector {
	if cfg.restElection && cfg.haGroupLockID != 0 {
		log.Error("msg", "Use either REST or PgAdvisoryLock for the leader election")
		os.Exit(1)
	}
	if cfg.restElection {
		return util.NewElector(util.NewRestElection())
	}
	if cfg.haGroupLockID == 0 {
		log.Warn("msg", "No adapter leader election. Group lock id is not set. Possible duplicate write load if running adapter in high-availability mode")
		return nil
	}
	if cfg.prometheusTimeout == -1 {
		log.Error("msg", "Prometheus timeout configuration must be set when using PG advisory lock")
		os.Exit(1)
	}
	lock, err := util.NewPgAdvisoryLock(cfg.haGroupLockID, cfg.pgmodelCfg.GetConnectionStr())
	if err != nil {
		log.Error("msg", "Error creating advisory lock", "haGroupLockId", cfg.haGroupLockID, "err", err)
		os.Exit(1)
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
	return &scheduledElector.Elector
}

func createMigrateCallback(cfg *pgclient.Config) util.CallbackOnEventualResult {
	return func(isDbReady interface{}) {
		migrate(cfg)
	}
}

func migrate(cfg *pgclient.Config) {
	shouldWrite, err := isWriter()
	if err != nil {
		log.Error("msg", "IsLeader check failed", "err", err)
		return
	}
	if !shouldWrite {
		log.Debug("msg", fmt.Sprintf("Election id %v: Instance is not a leader. Won't update", elector.ID()))
		return
	}

	dbStd, err := sql.Open("pgx", cfg.GetConnectionStr())
	if err != nil {
		log.Error("msg", err)
		return
	}
	defer func() {
		err := dbStd.Close()
		if err != nil {
			log.Error("msg", err)
			return
		}
	}()

	err = pgmodel.Migrate(dbStd)

	if err != nil {
		log.Error("msg", err)
		return
	}
}

func write(writerProvider util.Eventual) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var writer pgmodel.DBInserter
		writerI, done := writerProvider.Get()
		if !done {
			log.Warn("msg", "Write request received while waiting for remote storage; ignoring")
			return
		}

		if done && writerI == nil {
			errStr := "Couldn't connect to remote storage"
			log.Error("msg", errStr)
			http.Error(w, errStr, http.StatusInternalServerError)
			return
		}

		writer = writerI.(pgmodel.DBInserter)

		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Error("msg", "Read error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// TODO fix so we don't implicitly "know" that elector would be set if the
		// writer Eventual is complete
		shouldWrite, err := isWriter()
		if err != nil {
			log.Error("msg", "IsLeader check failed", "err", err)
			return
		}
		if !shouldWrite {
			log.Debug("msg", fmt.Sprintf("Election id %v: Instance is not a leader. Can't write data", elector.ID()))
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			log.Error("msg", "Decode error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			log.Error("msg", "Unmarshal error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		ts := req.GetTimeseries()
		receivedBatchCount := 0

		for _, t := range ts {
			receivedBatchCount = receivedBatchCount + len(t.Samples)
		}

		receivedSamples.Add(float64(receivedBatchCount))
		begin := time.Now()

		numSamples, err := writer.Ingest(req.GetTimeseries())
		if err != nil {
			log.Warn("msg", "Error sending samples to remote storage", "err", err, "num_samples", numSamples)
			failedSamples.Add(float64(receivedBatchCount))
			return
		}

		duration := time.Since(begin).Seconds()

		sentSamples.Add(float64(numSamples))
		sentBatchDuration.Observe(duration)

		writeThroughput.SetCurrent(getCounterValue(sentSamples))

		select {
		case d := <-writeThroughput.Values:
			log.Info("msg", "Samples write throughput", "samples/sec", d)
		default:
		}
	})
}

func isWriter() (bool, error) {
	if elector != nil {
		shouldWrite, err := elector.IsLeader()
		return shouldWrite, err
	}
	return true, nil
}

func getCounterValue(counter prometheus.Counter) float64 {
	dtoMetric := &io_prometheus_client.Metric{}
	if err := counter.Write(dtoMetric); err != nil {
		log.Warn("msg", "Error reading counter value", "err", err, "sentSamples", sentSamples)
	}
	return dtoMetric.GetCounter().GetValue()
}

func read(readerEventually util.Eventual) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Error("msg", "Read error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			log.Error("msg", "Decode error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			log.Error("msg", "Unmarshal error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		queryCount := float64(len(req.Queries))
		receivedQueries.Add(queryCount)
		begin := time.Now()

		var reader pgmodel.Reader
		readerI, done := readerEventually.Get()
		if !done {
			errStr := "Read request received while waiting for remote storage; ignoring"
			log.Warn("msg", errStr)
			http.Error(w, errStr, http.StatusInternalServerError)
			failedQueries.Add(queryCount)
			return
		}

		if done && readerI == nil {
			errStr := "Couldn't connect to remote storage"
			log.Error("msg", errStr)
			http.Error(w, errStr, http.StatusInternalServerError)
			failedQueries.Add(queryCount)
		}

		reader = readerI.(pgmodel.Reader)
		var resp *prompb.ReadResponse
		resp, err = reader.Read(&req)
		if err != nil {
			log.Warn("msg", "Error executing query", "query", req, "storage", "TimescaleDB", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			failedQueries.Add(queryCount)
			return
		}

		duration := time.Since(begin).Seconds()
		queryBatchDuration.Observe(duration)

		data, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}

func health(hcEventually util.Eventual) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var hc pgmodel.HealthChecker
		hcI, done := hcEventually.Get()
		if !done {
			log.Warn("Healthcheck while waiting for db")
			w.Header().Set("Content-Length", "0")
			return
		}
		if done && hcI == nil {
			errStr := "Healthcheck failed; db is unavailable"
			log.Warn("msg", errStr)
			http.Error(w, errStr, http.StatusInternalServerError)
			return
		}

		err := hc.HealthCheck()
		if err != nil {
			log.Warn("Healthcheck failed", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Length", "0")
	})
}

// timeHandler uses Prometheus histogram to track request time
func timeHandler(path string, handler http.Handler) http.Handler {
	f := func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		handler.ServeHTTP(w, r)
		elapsedMs := time.Since(start).Nanoseconds() / int64(time.Millisecond)
		httpRequestDuration.WithLabelValues(path).Observe(float64(elapsedMs))
	}
	return http.HandlerFunc(f)
}
