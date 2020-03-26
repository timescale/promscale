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
	log.Init(cfg.logLevel)
	log.Info("config", fmt.Sprintf("%+v", cfg))

	http.Handle(cfg.telemetryPath, promhttp.Handler())

	client, err := pgclient.NewClient(&cfg.pgmodelCfg)
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
	defer client.Close()

	elector = initElector(cfg)

	if cfg.migrate {
		migrate(&cfg.pgmodelCfg)
	}

	http.Handle("/write", timeHandler("write", write(client)))
	http.Handle("/read", timeHandler("read", read(client)))
	http.Handle("/healthz", health(client))

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
		log.Error(err)
		return
	}
	defer func() {
		err := dbStd.Close()
		if err != nil {
			log.Error(err)
			return
		}
	}()

	err = pgmodel.Migrate(dbStd)

	if err != nil {
		log.Error(err)
		return
	}
}

func write(writer pgmodel.DBInserter) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Error("msg", "Read error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

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

func read(reader pgmodel.Reader) http.Handler {
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

		var resp *prompb.ReadResponse
		resp, err = reader.Read(&req)
		if err != nil {
			log.Warn("msg", "Error executing query", "query", req, "storage", "PostgreSQL", "err", err)
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

func health(hc pgmodel.HealthChecker) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
