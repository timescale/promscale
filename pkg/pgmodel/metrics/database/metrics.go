package database

import (
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
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
	dbNetworkLatency = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace:   util.PromNamespace,
			Subsystem:   "sql_database",
			Name:        "network_latency_milliseconds",
			Help:        "Network latency between Promscale and Database. A negative value indicates a failed health check.",
			ConstLabels: map[string]string{"type": "promscale_sql"},
		},
	)
)

func init() {
	prometheus.MustRegister(dbHealthErrors, upMetric, dbNetworkLatency)
}

type metricQueryWrap struct {
	// Multiple metrics could be retrieved via single query
	// In that case they should appear in the same order as
	// corresponding the columns in the query's result.
	metrics       []prometheus.Collector
	query         string
	isHealthCheck bool // if set only metrics[0] is used
}

func gauges(opts ...prometheus.GaugeOpts) []prometheus.Collector {
	res := make([]prometheus.Collector, 0, len(opts))
	for _, opt := range opts {
		res = append(res, prometheus.NewGauge(opt))
	}
	return res
}
func counters(opts ...prometheus.CounterOpts) []prometheus.Collector {
	res := make([]prometheus.Collector, 0, len(opts))
	for _, opt := range opts {
		res = append(res, prometheus.NewCounter(opt))
	}
	return res
}
func histograms(opts ...prometheus.HistogramOpts) []prometheus.Collector {
	res := make([]prometheus.Collector, 0, len(opts))
	for _, opt := range opts {
		res = append(res, prometheus.NewHistogram(opt))
	}
	return res
}

var metrics = []metricQueryWrap{
	{
		metrics: counters(
			prometheus.CounterOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "health_check_total",
				Help:      "Total number of database health checks performed.",
			},
		),
		query:         "SELECT 1",
		isHealthCheck: true,
	},
	{
		metrics: gauges(
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "chunks_count",
				Help:      "Total number of chunks in TimescaleDB currently.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "chunks_compressed_count",
				Help:      "Total number of compressed chunks in TimescaleDB currently.",
			},
		),
		// Compressed_chunk_id is null for both yet to be compressed and already compressed chunks.
		query: `SELECT
				count(*) FILTER (WHERE dropped=false AND compressed_chunk_id IS NULL)::BIGINT AS chunks_count,
				count(*) FILTER (WHERE dropped=false AND compressed_chunk_id IS NOT NULL)::BIGINT AS chunks_compressed_count
			FROM _timescaledb_catalog.chunk`,
	}, {
		metrics: gauges(
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "chunks_metrics_expired_count",
				Help:      "The number of metrics chunks soon to be removed by maintenance jobs.",
			},
		),
		query: `WITH conf AS MATERIALIZED (SELECT _prom_catalog.get_default_retention_period() AS def_retention)
		SELECT count(*)::BIGINT
		FROM _timescaledb_catalog.dimension_slice ds
			 INNER JOIN _timescaledb_catalog.dimension d ON (d.id = ds.dimension_id)
			 INNER JOIN _timescaledb_catalog.hypertable h ON (h.id = d.hypertable_id)
			 INNER JOIN _prom_catalog.metric m ON (m.table_name = h.table_name AND m.table_schema = h.schema_name)
			 JOIN conf ON TRUE
		WHERE ds.range_start < _timescaledb_internal.time_to_internal(now() - coalesce(m.retention_period, conf.def_retention))
		  AND ds.range_end < _timescaledb_internal.time_to_internal(now() - coalesce(m.retention_period, conf.def_retention))`,
	}, {
		metrics: gauges(
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "chunks_metrics_uncompressed_count",
				Help:      "The number of metrics chunks soon to be compressed by maintenance jobs.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "chunks_metrics_delayed_compression_count",
				Help:      "The number of metrics chunks not-compressed due to a set delay.",
			},
		),
		query: `WITH chunk_candidates AS MATERIALIZED (
				SELECT chcons.dimension_slice_id, h.table_name, h.schema_name
				FROM _timescaledb_catalog.chunk_constraint chcons
					INNER JOIN _timescaledb_catalog.chunk c ON c.id = chcons.chunk_id
					INNER JOIN _timescaledb_catalog.hypertable h ON h.id = c.hypertable_id
				WHERE c.dropped IS FALSE
				AND h.compression_state = 1 -- compression_enabled = TRUE
				AND (c.status & 1) != 1 -- only check for uncompressed chunks
			)
			SELECT
				count(*) FILTER(WHERE m.delay_compression_until IS NULL OR m.delay_compression_until < now())::BIGINT AS uncompressed,
				count(*) FILTER(WHERE m.delay_compression_until IS NOT NULL AND m.delay_compression_until >= now())::BIGINT AS delayed_compression
			FROM chunk_candidates cc
				INNER JOIN _timescaledb_catalog.dimension_slice ds ON ds.id = cc.dimension_slice_id
				INNER JOIN _prom_catalog.metric m ON (m.table_name = cc.table_name AND m.table_schema = cc.schema_name)
			WHERE NOT m.is_view
			AND ds.range_start <= _timescaledb_internal.time_to_internal(now() - interval '1 hour')
			AND ds.range_end <= _timescaledb_internal.time_to_internal(now() - interval '1 hour')`,
	}, {
		metrics: gauges(
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "chunks_traces_expired_count",
				Help:      "The number of traces chunks soon to be removed by maintenance jobs.",
			},
		),
		query: `WITH conf AS MATERIALIZED (SELECT coalesce(ps_trace.get_trace_retention_period(), interval '0 day') AS def_retention)
		SELECT count(*)::BIGINT
		FROM _timescaledb_catalog.dimension_slice ds
			 INNER JOIN _timescaledb_catalog.dimension d ON (d.id = ds.dimension_id)
			 INNER JOIN _timescaledb_catalog.hypertable h ON (h.id = d.hypertable_id)
			 JOIN conf ON TRUE
		WHERE ds.range_start < _timescaledb_internal.time_to_internal(now() - conf.def_retention)
		  AND ds.range_end < _timescaledb_internal.time_to_internal(now() - conf.def_retention)
		  AND h.schema_name = '_ps_trace'`,
	}, {
		metrics: gauges(
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "chunks_traces_uncompressed_count",
				Help:      "The number of traces chunks soon to be compressed by maintenance jobs.",
			},
		),
		query: `WITH chunk_candidates AS MATERIALIZED (
				SELECT chcons.dimension_slice_id
				FROM _timescaledb_catalog.chunk_constraint chcons
					INNER JOIN _timescaledb_catalog.chunk c ON c.id = chcons.chunk_id
					INNER JOIN _timescaledb_catalog.hypertable h ON h.id = c.hypertable_id
				WHERE c.dropped IS FALSE
				AND h.schema_name = '_ps_trace'
				AND h.compression_state = 1 -- compression_enabled = TRUE
				AND (c.status & 1) != 1 -- only check for uncompressed chunks
			)
			SELECT count(*)::BIGINT
			FROM chunk_candidates cc
				INNER JOIN _timescaledb_catalog.dimension_slice ds ON ds.id = cc.dimension_slice_id
			WHERE ds.range_start <= _timescaledb_internal.time_to_internal(now() - interval '1 hour')
			AND ds.range_end <= _timescaledb_internal.time_to_internal(now() - interval '1 hour')`,
	}, {
		metrics: gauges(
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_locks_access_exclusive",
				Help:      "Number of AccessExclusiveLock locks held by Promscale maintenance workers.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_locks_access_share",
				Help:      "Number of AccessShareLock locks held by Promscale maintenance workers.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_locks_exclusive",
				Help:      "Number of ExclusiveLock locks held by Promscale maintenance workers.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_locks_row_exclusive",
				Help:      "Number of RowExclusiveLock locks held by Promscale maintenance workers.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_locks_row_share",
				Help:      "Number of RowShareLock locks held by Promscale maintenance workers.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_locks_share",
				Help:      "Number of ShareLock locks held by Promscale maintenance workers.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_locks_share_update_exclusive",
				Help:      "Number of ShareRowExclusiveLock locks held by Promscale maintenance workers.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_locks_share_row_exclusive",
				Help:      "Number of ShareUpdateExclusiveLock locks held by Promscale maintenance workers.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_locks",
				Help:      "Number of locks held by Promscale maintenance workers.",
			},
		),
		query: `SELECT
				COUNT(*) FILTER (WHERE l.mode = 'AccessExclusiveLock') :: BIGINT AS AccessExclusiveLock,
				COUNT(*) FILTER (WHERE l.mode = 'AccessShareLock') :: BIGINT AS AccessShareLock,
				COUNT(*) FILTER (WHERE l.mode = 'ExclusiveLock') :: BIGINT AS ExclusiveLock,
				COUNT(*) FILTER (WHERE l.mode = 'RowExclusiveLock') :: BIGINT AS RowExclusiveLock,
				COUNT(*) FILTER (WHERE l.mode = 'RowShareLock') :: BIGINT AS RowShareLock,
				COUNT(*) FILTER (WHERE l.mode = 'ShareLock') :: BIGINT AS ShareLock,
				COUNT(*) FILTER (WHERE l.mode = 'ShareUpdateExclusiveLock') :: BIGINT AS ShareUpdateExclusiveLock,
				COUNT(*) FILTER (WHERE l.mode = 'ShareRowExclusiveLock') :: BIGINT AS ShareRowExclusiveLock,
				--
				COUNT(*) :: BIGINT AS total
			FROM pg_stat_activity sa
			JOIN pg_locks l ON l.pid = sa.pid
			WHERE sa.application_name LIKE 'promscale maintenance%'
			AND sa.state <> 'idle'`,
	}, {
		metrics: gauges(
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_long_running_buffer_pin",
				Help:      "Number of Promscale maintenance workers executing long running queries, waiting on BufferPin events.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_long_running_io",
				Help:      "Number of Promscale maintenance workers executing long running queries, waiting on IO events.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_long_running_ipc",
				Help:      "Number of Promscale maintenance workers executing long running queries, waiting on IPC events.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_long_running_lock",
				Help:      "Number of Promscale maintenance workers executing long running queries, waiting on Lock events.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_long_running_lwlock",
				Help:      "Number of Promscale maintenance workers executing long running queries, waiting on LWLock events.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_long_running_timeout",
				Help:      "Number of Promscale maintenance workers executing long running queries, waiting on Timeout events.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_long_running_compression",
				Help:      "Number of Promscale maintenance workers executing long running queries, originating from compression jobs.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_long_running_retention_metric",
				Help:      "Number of Promscale maintenance workers executing long running queries, originating from metric retention jobs.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_long_running_retention_tracing",
				Help:      "Number of Promscale maintenance workers executing long running queries, originating from tracing retention jobs.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_long_running",
				Help:      "Number of Promscale maintenance workers executing long running queries.",
			},
		),
		query: `SELECT
				-- by wait event type; not exhaustive
				COUNT(*) FILTER (WHERE sa.wait_event_type = 'BufferPin') :: BIGINT AS buffer_pin_cnt,
				COUNT(*) FILTER (WHERE sa.wait_event_type = 'IO') :: BIGINT AS io_cnt,
				COUNT(*) FILTER (WHERE sa.wait_event_type = 'IPC') :: BIGINT AS ipc_cnt,
				COUNT(*) FILTER (WHERE sa.wait_event_type = 'Lock') :: BIGINT AS lock_cnt,
				COUNT(*) FILTER (WHERE sa.wait_event_type = 'LWLock') :: BIGINT AS lwlock_cnt,
				COUNT(*) FILTER (WHERE sa.wait_event_type = 'Timeout') :: BIGINT AS timeout_cnt,
				-- by workload type; exhaustive (the sum should equal the total)
				COUNT(*) FILTER (WHERE sa.application_name LIKE '%compression%') :: BIGINT AS compression_cnt,
				COUNT(*) FILTER (WHERE sa.application_name LIKE '%retention%' AND sa.application_name LIKE '%metric%') :: BIGINT AS metric_retention_cnt,
				COUNT(*) FILTER (WHERE sa.application_name LIKE '%retention%' AND sa.application_name LIKE '%tracing%') :: BIGINT AS tracing_retention_cnt,
				--
				COUNT(*) :: BIGINT AS total
			FROM pg_stat_activity sa
			WHERE sa.application_name LIKE 'promscale maintenance%'
			AND (now() - coalesce(sa.query_start, sa.xact_start)) > INTERVAL '10 seconds'
			AND sa.state <> 'idle'`,
	}, {
		metrics: gauges(
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_long_running_longest_seconds",
				Help:      "The duration in seconds of a longest running query, originating from a Promscale maintenance worker.",
			},
		),
		query: `SELECT coalesce(extract(EPOCH FROM MAX(now() - coalesce(sa.query_start, sa.xact_start))) :: BIGINT, 0)
			FROM pg_stat_activity sa WHERE sa.application_name LIKE 'promscale maintenance%'
			AND sa.state <> 'idle'`,
	}, {
		metrics: gauges(
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "compression_status",
				Help:      "Compression status in TimescaleDB.",
			},
		),
		query: `select (case when (value = 'true') then 1 else 0 end) from _prom_catalog.get_default_value('metric_compression') value`,
	}, {
		metrics: gauges(
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_count",
				Help:      "Number of TimescaleDB background workers.",
			},
		),
		query: `select current_setting('timescaledb.max_background_workers')::BIGINT`,
	}, {
		metrics: gauges(
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job",
				Help:      "Number of Promscale maintenance workers.",
			},
		),
		query: `select count(*) from timescaledb_information.jobs where proc_name = 'execute_maintenance_job'`,
	}, {
		metrics: gauges(
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_failed",
				Help:      "Number of Promscale maintenance jobs that failed.",
			},
		),
		query: `select count(stats.last_run_status)
			from timescaledb_information.job_stats stats
			inner join
			timescaledb_information.jobs jobs
				on jobs.job_id = stats.job_id
			where jobs.proc_name = 'execute_maintenance_job' and stats.last_run_status = 'Failed'`,
	}, {
		metrics: append(histograms(
			prometheus.HistogramOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Buckets:   prometheus.ExponentialBucketsRange(1, 86400.0, 15), // up to a day
				Name:      "worker_maintenance_job_metrics_compression_last_duration_seconds",
				Help:      "The duration of the most recently completed metrics compression job.",
			},
			prometheus.HistogramOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Buckets:   prometheus.ExponentialBucketsRange(1, 86400.0, 15), // up to a day
				Name:      "worker_maintenance_job_metrics_retention_last_duration_seconds",
				Help:      "The duration of the most recently completed metrics retention job.",
			},
			prometheus.HistogramOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Buckets:   prometheus.ExponentialBucketsRange(1, 86400.0, 15), // up to a day
				Name:      "worker_maintenance_job_traces_retention_last_duration_seconds",
				Help:      "The duration of the most recently completed traces retention job.",
			},
			prometheus.HistogramOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Buckets:   prometheus.ExponentialBucketsRange(1, 86400.0, 15), // up to a day
				Name:      "worker_maintenance_job_traces_compression_last_duration_seconds",
				Help:      "The duration of the most recently completed traces compression job.",
			},
		), gauges(
			// They need to be guagues, because the source is already a sum
			// and our DB metric colleciton system uses Add on counters.
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_metrics_compression_failures_count",
				Help:      "The number of failed metrics compression jobs.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_metrics_compression_total_runs_count",
				Help:      "The total number of completed metrics compression jobs.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_metrics_retention_failures_count",
				Help:      "The number of failed metrics retention jobs.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_metrics_retention_total_runs_count",
				Help:      "The total number of completed metrics retention jobs.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_traces_retention_failures_count",
				Help:      "The number of failed traces retention jobs.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_traces_retention_total_runs_count",
				Help:      "The total number of completed traces retention jobs.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_traces_compression_failures_count",
				Help:      "The number of failed traces compression jobs.",
			},
			prometheus.GaugeOpts{
				Namespace: util.PromNamespace,
				Subsystem: "sql_database",
				Name:      "worker_maintenance_job_traces_compression_total_runs_count",
				Help:      "The total number of completed traces compression jobs.",
			},
		)...),
		query: `WITH maintenance_jobs_stats AS (
			SELECT
				coalesce(config ->> 'signal', 'traces') AS signal_type,
				coalesce(config ->> 'type', 'compression') AS job_type,
				MAX(js.last_run_duration) AS last_duration,
				SUM(js.total_failures) AS failures_count,
				SUM(js.total_runs) AS total_runs_count
			FROM timescaledb_information.job_stats js
			JOIN timescaledb_information.jobs j ON j.job_id = js.job_id
			WHERE proc_schema = '_prom_catalog' OR proc_schema = '_ps_trace'
			GROUP BY 1, 2
			)
			SELECT
				coalesce(extract(EPOCH FROM MAX(last_duration)
						FILTER ( WHERE signal_type = 'metrics' AND job_type = 'compression' )), 0)::BIGINT AS metrics_compression_last_duration,
				coalesce(extract(EPOCH FROM MAX(last_duration)
						FILTER ( WHERE signal_type = 'metrics' AND job_type = 'retention' )), 0)::BIGINT   AS metrics_retention_last_duration,
				coalesce(extract(EPOCH FROM MAX(last_duration)
						FILTER ( WHERE signal_type = 'traces'  AND job_type = 'retention' )), 0)::BIGINT   AS traces_retention_last_duration,
				coalesce(extract(EPOCH FROM MAX(last_duration)
						FILTER ( WHERE signal_type = 'traces'  AND job_type = 'compression' )), 0)::BIGINT AS traces_compression_last_duration,
				coalesce(MAX(failures_count)
				FILTER ( WHERE signal_type = 'metrics' AND job_type = 'compression' ), 0)::BIGINT                  AS metrics_compression_failures_count,
				coalesce(MAX(total_runs_count)
				FILTER ( WHERE signal_type = 'metrics' AND job_type = 'compression' ), 0)::BIGINT                  AS metrics_compression_total_runs_count,
				coalesce(MAX(failures_count)
				FILTER ( WHERE signal_type = 'metrics' AND job_type = 'retention' ), 0)::BIGINT                    AS metrics_retention_failures_count,
				coalesce(MAX(total_runs_count)
				FILTER ( WHERE signal_type = 'metrics' AND job_type = 'retention' ), 0)::BIGINT                    AS metrics_retention_total_runs_count,
				coalesce(MAX(failures_count)
				FILTER ( WHERE signal_type = 'traces' AND job_type = 'retention' ), 0)::BIGINT                     AS traces_retention_failures_count,
				coalesce(MAX(total_runs_count)
				FILTER ( WHERE signal_type = 'traces' AND job_type = 'retention' ), 0)::BIGINT                     AS traces_retention_total_runs_count,
				coalesce(MAX(failures_count)
				FILTER ( WHERE signal_type = 'traces' AND job_type = 'compression' ), 0)::BIGINT                   AS traces_compression_failures_count,
				coalesce(MAX(total_runs_count)
				FILTER ( WHERE signal_type = 'traces' AND job_type = 'compression' ), 0)::BIGINT                   AS traces_compression_total_runs_count
			FROM maintenance_jobs_stats;`,
	}, {
		metrics: gauges(
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
		metrics: gauges(
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

// GetMetric returns the metric whose Description best matches the supplied name.
func GetMetric(name string) (prometheus.Metric, error) {
	var candidate prometheus.Metric = nil
	candidateDescLen := 0
	for _, ms := range metrics {
		for _, m := range ms.metrics {
			metric := getMetric(m)
			str, err := util.ExtractMetricDesc(metric)
			if err != nil {
				return nil, fmt.Errorf("extract metric string")
			}
			if strings.Contains(str, name) && (len(str) < candidateDescLen || candidate == nil) {
				candidate = metric
				candidateDescLen = len(str)
			}
		}
	}
	return candidate, nil
}

func getMetric(c prometheus.Collector) prometheus.Metric {
	switch n := c.(type) {
	case prometheus.Gauge:
		return n
	case prometheus.Counter:
		return n
	case prometheus.Histogram:
		return n
	default:
		panic(fmt.Sprintf("invalid type: %T", n))
	}
}
