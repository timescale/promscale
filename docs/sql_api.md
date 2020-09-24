# Function API Reference

This page is a reference for the functions available in Promscale.

A description of the usage of these functions, and examples can be found in
[our description of the sql schema](sql_schema.md)

<!--
SQL To generate

SELECT
  p.proname as "Name",
  pg_catalog.pg_get_function_arguments(p.oid) as "Argument data types",
  pg_catalog.pg_get_function_result(p.oid) as "Result data type",
  p.proname || ' ' || pg_catalog.obj_description(p.oid, 'pg_proc') || '.' as "Description"
FROM pg_catalog.pg_proc p
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
     LEFT JOIN pg_catalog.pg_language l ON l.oid = p.prolang
WHERE n.nspname OPERATOR(pg_catalog.~) '^(prom)$' COLLATE pg_catalog.default
ORDER BY 1, 2, 4;
-->

 Name | Arguments | Return type | Description
 --- | --- | --- | ---
 execute_maintenance           |                                                          |                  | Execute maintenance tasks like dropping data according to retention policy. This procedure should be run regularly in a cron job.
 eq                            | labels label_array, json_labels jsonb                    | boolean          | eq returns true if the labels and jsonb are equal, ignoring the metric name.
 eq                            | labels1 label_array, labels2 label_array                 | boolean          | eq returns true if two label arrays are equal, ignoring the metric name.
 eq                            | labels1 label_array, matchers matcher_positive           | boolean          | eq returns true if the label array and matchers are equal, there should not be a matcher for the metric name.
 is_normal_nan                 | value double precision                                   | boolean          | is_normal_nan returns true if the value is a NaN.
 is_stale_marker               | value double precision                                   | boolean          | is_stale_marker returns true if the value is a Prometheus stale marker.
 jsonb                         | labels label_array                                       | jsonb            | jsonb converts a labels array to a JSONB object.
 key_value_array               | labels label_array, OUT keys text[], OUT vals text[]     | record           | key_value_array converts a labels array to two arrays: one for keys and another for values.
 matcher                       | labels jsonb                                             | matcher_positive | matcher returns a matcher for the JSONB, __name__ is ignored. The matcher can be used to match against a label array using @> or ? operators.
 reset_metric_chunk_interval   | metric_name text                                         | boolean          | reset_metric_chunk_interval resets the chunk interval for a specific metric to using the default.
 reset_metric_retention_period | metric_name text                                         | boolean          | reset_metric_retention_period resets the retention period for a specific metric to using the default.
 set_default_chunk_interval    | chunk_interval interval                                  | boolean          | set_default_chunk_interval set the chunk interval for any metrics (existing and new) without an explicit override.
 set_default_retention_period  | retention_period interval                                | boolean          | set_default_retention_period set the retention period for any metrics (existing and new) without an explicit override.
 set_metric_chunk_interval     | metric_name text, chunk_interval interval                | boolean          | set_metric_chunk_interval set a chunk interval for a specific metric (this overrides the default).
 set_metric_retention_period   | metric_name text, new_retention_period interval          | boolean          | set_metric_retention_period set a retention period for a specific metric (this overrides the default).
 val                           | label_id integer                                         | text             | val returns the label value from a label id.