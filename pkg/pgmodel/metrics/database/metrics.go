package database

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/util"
)

var (
	dbHealthErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Subsystem: "sql_database",
			Name:      "health_check_errors_total",
			Help:      "Total number of database health check errors.",
		},
	)
	upMetric = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name:        "up",
			Help:        "Up represents if the database metrics engine is running or not.",
			ConstLabels: map[string]string{"type": "promscale_sql"},
		},
	)
)

func init() {
	prometheus.MustRegister(dbHealthErrors, upMetric)
}

func InitHealthCheck(conn pgxconn.PgxConn) {
	dbHealthCheck := prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Subsystem: "sql_database",
			Name:      "health_check_total",
			Help:      "Total number of database health checks performed.",
		},
	)
	dbNetworkLatency := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace:   util.PromNamespace,
			Subsystem:   "sql_database",
			Name:        "network_latency_milliseconds",
			Help:        "Network latency between Promscale and Database. A negative value indicates a failed health check.",
			ConstLabels: map[string]string{"type": "promscale_sql"},
		}, func() float64 {
			// This function is called everytime Prometheus scrapes /metrics of Promscale.
			// We use this to an advantage and do the health check as much as is expected by Prometheus.
			val := 0
			start := time.Now()
			err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&val)
			latency := float64(time.Since(start).Milliseconds())
			dbHealthCheck.Add(1) // Increment the dbHealthCheck.
			if err != nil {
				latency = -1
				dbHealthErrors.Inc()
				log.Error("msg", "health check failed", "err", err.Error())
			}
			return latency
		},
	)
	prometheus.MustRegister(dbHealthCheck, dbNetworkLatency)
}

type metricQueryWrap struct {
	metric prometheus.Collector
	query  string
}

var metrics = []metricQueryWrap{
	{
		metric: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "chunks_count",
				Help:      "Total number of chunks in TimescaleDB currently.",
			},
		),
		query: `select count(*)::bigint from _timescaledb_catalog.chunk where dropped=false`,
	},
	{
		metric: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "chunks_created",
				Help:      "Total number of chunks created since creation of database.",
			},
		),
		query: `select count(*)::bigint from _timescaledb_catalog.chunk`,
	},
	{
		metric: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "chunks_compressed_count",
				Help:      "Total number of compressed chunks in TimescaleDB currently.",
			},
		),
		query: `select count(*)::bigint from _timescaledb_catalog.chunk where dropped=false and compressed_chunk_id is not null`,
	}, {
		metric: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "compression_status",
				Help:      "Compression status in TimescaleDB.",
			},
		),
		query: `select (case when (value = 'true') then 1 else 0 end) from _prom_catalog.get_default_value('metric_compression') value`,
	}, {
		metric: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_count",
				Help:      "Number of TimescaleDB background workers.",
			},
		),
		query: `select current_setting('timescaledb.max_background_workers')::BIGINT`,
	}, {
		metric: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job",
				Help:      "Number of Promscale maintenance workers.",
			},
		),
		query: `select count(*) from timescaledb_information.jobs where proc_name = 'execute_maintenance_job'`,
	}, {
		metric: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_failed",
				Help:      "Number of Promscale maintenance workers.",
			},
		),
		query: `select count(stats.last_run_status)
	from timescaledb_information.job_stats stats
inner join
	timescaledb_information.jobs jobs
		on jobs.job_id = stats.job_id
	where jobs.proc_name = 'execute_maintenance_job' and stats.last_run_status = 'Failed'`,
	}, {
		metric: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_start_timestamp_seconds",
				Help:      "Timestamp in unix seconds for last successful execution of Promscale maintenance job.",
			},
		),
		query: `SELECT extract(
	epoch FROM (SELECT COALESCE(
		(SELECT last_run_started_at AS job_running_since
			FROM   timescaledb_information.job_stats WHERE  last_run_started_at > last_successful_finish
				AND last_run_status = 'Success'
		),
		CURRENT_TIMESTAMP
	)))::BIGINT`,
	}, {
		metric: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "metric_count",
				Help:      "Total number of metrics in the database.",
			},
		),
		query: `select count(*)::bigint from _prom_catalog.metric`,
	},
}

// GetMetric returns the first metric whose Name matches the supplied name.
func GetMetric(name string) (prometheus.Metric, error) {
	for _, m := range metrics {
		metric := getMetric(m.metric)
		str, err := util.ExtractMetricDesc(metric)
		if err != nil {
			return nil, fmt.Errorf("extract metric string")
		}
		if strings.Contains(str, name) {
			return metric, nil
		}
	}
	return nil, nil
}

func getMetric(c prometheus.Collector) prometheus.Metric {
	switch n := c.(type) {
	case prometheus.Gauge:
		return n
	case prometheus.Counter:
		return n
	default:
		panic(fmt.Sprintf("invalid type: %T", n))
	}
}
