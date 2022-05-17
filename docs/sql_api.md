# Function API Reference

The content in this page has been moved to https://docs.timescale.com/promscale/latest/sql-api/

<!--
SQL To generate

\pset border 1
\pset format aligned
\pset linestyle ascii
\o funcs.txt
SELECT
  n.nspname as "Schema",
  p.proname as "Name",
  pg_catalog.pg_get_function_arguments(p.oid) as "Argument data types",
  pg_catalog.pg_get_function_result(p.oid) as "Result data type",
  p.proname || ' ' || pg_catalog.obj_description(p.oid, 'pg_proc') || '.' as "Description"
FROM pg_catalog.pg_proc p
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
     LEFT JOIN pg_catalog.pg_language l ON l.oid = p.prolang
WHERE n.nspname like 'prom_%' or n.nspname = 'ps_trace'
ORDER BY 1, 2, 3;
-->
