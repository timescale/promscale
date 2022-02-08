# Usage Telemetry

Promscale collects anonymous usage data to help us better understand how the
community uses our products and as a result make our product better for our
users. Your privacy is the most important thing to us so we don't collect any 
personally identifiable information.

You can see at all times the data collected by Promscale by running the following
query in the database:

`SELECT key, value FROM _timescaledb_catalog.metadata WHERE key like 'promscale%' order by key;`

The query will return something similar to this:

|                             key                             |    value     |
|-------------------------------------------------------------|--------------|
| promscale_arch                                              | amd64        |
| promscale_build_platform                                    |              |
| promscale_commit_hash                                       |              |
| promscale_connector_instance_total                          | 1            |
| promscale_db_node_count                                     | 0            |
| promscale_ingested_samples_total                            | 0            |
| promscale_metrics_bytes_total                               | 0            |
| promscale_metrics_default_chunk_interval                    | 08:00:00     |
| promscale_metrics_default_retention                         | 90 days      |
| promscale_metrics_exemplar_total                            | 0            |
| promscale_metrics_ha_cluster_count                          | 0            |
| promscale_metrics_metadata_total                            | 0            |
| promscale_metrics_multi_tenancy_tenant_count                | 0            |
| promscale_metrics_queries_success_total                     | 0            |
| promscale_metrics_queries_failed_total                      | 0            |
| promscale_metrics_queries_timedout_total                    | 0            |
| promscale_metrics_registered_views                          | 0            |
| promscale_metrics_series_total_approx                       | 0            |
| promscale_metrics_total                                     | 0            |
| promscale_os                                                | linux        |
| promscale_os_id                                             | ubuntu       |
| promscale_os_machine                                        | x86_64       |
| promscale_os_node_name                                      | ubuntu-focal |
| promscale_os_sys_name                                       | Linux        |
| promscale_os_version                                        | 20.04        |
| promscale_packager                                          | unknown      |
| promscale_promql_query_execution_time_p50                   | 0.0000       |
| promscale_promql_query_execution_time_p90                   | 0.0000       |
| promscale_promql_query_execution_time_p95                   | 0.0000       |
| promscale_promql_query_execution_time_p99                   | 0.0000       |
| promscale_promql_query_remote_read_batch_execution_time_p50 | 0.0000       |
| promscale_promql_query_remote_read_batch_execution_time_p90 | 0.0000       |
| promscale_promql_query_remote_read_batch_execution_time_p95 | 0.0000       |
| promscale_promql_query_remote_read_batch_execution_time_p99 | 0.0000       |
| promscale_promql_telemetry_evaluation_duration_seconds      | 1.027        |
| promscale_trace_dependency_requests_executed_total          | 0            |
| promscale_trace_query_execution_time_p50                    | 0.0000       |
| promscale_trace_query_execution_time_p90                    | 0.0000       |
| promscale_trace_query_execution_time_p95                    | 0.0000       |
| promscale_trace_query_execution_time_p99                    | 0.0000       |
| promscale_trace_query_requests_executed_total               | 0            |
| promscale_traces_spans_bytes_total                          | 65536        |
| promscale_traces_spans_total_approx                         | 0            |
| promscale_traces_total_approx                               | 0            |
| promscale_version                                           | 0.8.0        |


This data together with usage data collected by
[TimescaleDB](https://docs.timescale.com/timescaledb/latest/how-to-guides/configuration/telemetry/) 
is sent to our servers every 24 hours.

## Disable Telemetry

Promscale usage telemetry is collected through TimescaleDB's telemetry feature.

You can [exclude specific items](https://docs.timescale.com/timescaledb/latest/how-to-guides/configuration/telemetry/#change-what-is-included-the-telemetry-report) you are not confortable sharing
 or [completely disable the feature](https://docs.timescale.com/timescaledb/latest/how-to-guides/configuration/telemetry/#disable-telemetry).
