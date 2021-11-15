// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package telemetry

// Whats the difference b/t stats and telemetry?
//
// Stats are reported by all Promscales and written into the promscale_instance_connection table.
//
// Telemetry is the aggregates that housekeeper performs, the final processing before writing
// into the _timescaledb_catalog.metadata table.

type telemetryType uint8

const (
	isInt telemetryType = iota
	isString
)

var telemetries = []telemetry{
	telemetrySQL{
		stat: "promscale_ingested_samples_total",
		sql:  "SELECT sum(telemetry_ingested_samples) FROM _ps_catalog.promscale_instance_information",
		typ:  isInt,
	}, telemetrySQL{
		stat: "promscale_instance_count",
		sql:  "SELECT count(*) FROM _ps_catalog.promscale_instance_information",
		typ:  isInt,
	}, telemetrySQL{
		stat: "promscale_metrics_count",
		sql:  "SELECT count(*) FROM _prom_catalog.metric",
		typ:  isInt,
	}, telemetrySQL{
		stat: "promscale_metrics_bytes_total",
		sql:  "SELECT sum(hypertable_size(format('prom_data.%I', table_name))) FROM _prom_catalog.metric",
		typ:  isInt,
	}, telemetrySQL{
		stat: "promscale_series_count_approx",
		sql:  "SELECT approximate_row_count('_prom_catalog.series')",
		typ:  isInt,
	}, telemetrySQL{
		stat: "promscale_traces_count",
		sql:  "SELECT count(trace_id) FROM (SELECT trace_id FROM _ps_trace.span GROUP BY 1) a",
		typ:  isInt,
	}, telemetrySQL{
		stat: "promscale_spans_count",
		sql:  "SELECT approximate_row_count('_ps_trace.span')",
		typ:  isInt,
	}, telemetrySQL{
		stat: "promscale_spans_bytes_total",
		sql:  "SELECT hypertable_size('_ps_trace.span')",
		typ:  isInt,
	}, telemetrySQL{
		stat: "promscale_multi_tenancy_tenant_count",
		sql:  "SELECT count(*) FROM _prom_catalog.label WHERE key = '__tenant__'",
		typ:  isInt,
	}, telemetrySQL{
		stat: "promscale_node_count",
		sql:  "SELECT count(*) FROM timescaledb_information.data_nodes",
		typ:  isInt,
	}, telemetrySQL{
		stat: "promscale_ha_cluster_count",
		sql:  "SELECT count(*) FROM _prom_catalog.label_key WHERE key = '__cluster__'",
		typ:  isInt,
	}, telemetrySQL{
		stat: "promscale_downsampling_count",
		sql:  "SELECT count(*) FROM _prom_catalog.metric WHERE is_view IS true",
		typ:  isInt,
	}, telemetrySQL{
		stat: "promscale_exemplar_metrics_count",
		sql:  "SELECT count(*) FROM _prom_catalog.exemplar",
		typ:  isInt,
	}, telemetrySQL{
		stat: "promscale_metric_metadata_count",
		sql:  "SELECT count(*) FROM _prom_catalog.metadata",
		typ:  isInt,
	}, telemetrySQL{
		stat: "promscale_default_metric_retention",
		sql:  "SELECT value FROM _prom_catalog.default WHERE key = 'retention_period'",
		typ:  isString,
	}, telemetrySQL{
		stat: "promscale_default_chunk_interval",
		sql:  "SELECT value FROM _prom_catalog.default WHERE key = 'chunk_interval'",
		typ:  isString,
	},
	telemetrySQL{
		stat: "promscale_promql_executed_queries",
		sql:  "SELECT sum(telemetry_queries_executed) FROM _ps_catalog.promscale_instance_information",
		typ:  isInt,
	},
	telemetrySQL{
		stat: "promscale_promql_timed_out_queries",
		sql:  "select sum(telemetry_queries_timed_out) from _ps_catalog.promscale_instance_information",
		typ:  isInt,
	},
	telemetrySQL{
		stat: "promscale_promql_failed_queries",
		sql:  "select sum(telemetry_queries_failed) from _ps_catalog.promscale_instance_information",
		typ:  isInt,
	},
}
