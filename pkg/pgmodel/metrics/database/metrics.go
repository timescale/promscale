package database

import "github.com/prometheus/client_golang/prometheus"

const namespace = "promscale_sql"

type metricQueryWrap struct {
	metric      prometheus.Collector
	query       string
	previousVal int64
	isCounter   bool
}

var metrics = []metricQueryWrap{
	{
		metric: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "database",
				Name:      "chunks_count",
				Help:      "Total number of chunks in TimescaleDB currently.",
			},
		),
		query: `select count(*)::bigint from _timescaledb_catalog.chunk where dropped=false`,
	},
	{
		metric: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: "database",
				Name:      "chunks_created_total",
				Help:      "Total number of chunks created since creation of database.",
			},
		),
		query:     `select count(*)::bigint from _timescaledb_catalog.chunk`,
		isCounter: true,
	},
	{
		metric: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "database",
				Name:      "chunks_compressed_count",
				Help:      "Total number of compressed chunks in TimescaleDB currently.",
			},
		),
		query: `select count(*)::bigint from _timescaledb_catalog.chunk where dropped=false and compressed_chunk_id is not null`,
	}, {
		metric: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "database",
				Name:      "compression_status",
				Help:      "Compression status in TimescaleDB.",
			},
		),
		query: `select (case when (value = 'true') then 1 else 0 end) from _prom_catalog.default where key = 'metric_compression'`,
	}, {
		metric: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "database",
				Name:      "worker_count",
				Help:      "Number of TimescaleDB background workers.",
			},
		),
		query: `select current_setting('timescaledb.max_background_workers')::BIGINT`,
	}, {
		metric: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "database",
				Name:      "worker_maintenance_job",
				Help:      "Number of Promscale maintenance workers.",
			},
		),
		query: `select count(*) from timescaledb_information.jobs where proc_name = 'execute_maintenance_job'`,
	}, {
		metric: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "database",
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
				Namespace: namespace,
				Subsystem: "database",
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
				Namespace: namespace,
				Subsystem: "database",
				Name:      "chunks_bytes",
				Help:      "Total bytes of all chunks in 'prom_data' & '_ps_trace' schema.",
			},
		),
		query: `select sum(total_bytes)::bigint from _timescaledb_internal.hypertable_chunk_local_size where hypertable_schema = 'prom_data' or hypertable_schema = '_ps_trace'`,
	}, {
		metric: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "database",
				Name:      "chunks_compressed_bytes",
				Help:      "Total bytes of all compressed chunks in 'prom_data' & '_ps_trace' schema.",
			},
		),
		query: `select sum(compressed_total_size)::bigint from _timescaledb_internal.hypertable_chunk_local_size where hypertable_schema = 'prom_data' or hypertable_schema = '_ps_trace'`,
	},
}
