DO $$
BEGIN
  --This fixes previous updates to 0.6 that were only partially applied. See issue #755
  --Often this isn't needed and so will error out
  ALTER TABLE _prom_catalog.metric
  DROP COLUMN table_schema,
  DROP COLUMN series_table,
  DROP COLUMN is_view,
  ADD CONSTRAINT  "metric_metric_name_table_name_key" UNIQUE(metric_name) INCLUDE (table_name),
  ADD CONSTRAINT  "metric_table_name_key" UNIQUE(table_name);
EXCEPTION WHEN others THEN --ignore
    NULL;
END
$$;

ALTER TABLE _prom_catalog.metric
    ADD COLUMN table_schema name NOT NULL DEFAULT 'prom_data',
    ADD COLUMN series_table name, -- series_table stores the name of the table used to store the series data for this metric.
    ADD COLUMN is_view BOOLEAN NOT NULL DEFAULT false,
    DROP CONSTRAINT metric_metric_name_table_name_key,
    DROP CONSTRAINT metric_table_name_key,
    ADD CONSTRAINT metric_metric_name_table_schema_table_name_key UNIQUE (metric_name, table_schema) INCLUDE (table_name),
    ADD CONSTRAINT metric_table_schema_table_name_key UNIQUE(table_schema, table_name);

UPDATE _prom_catalog.metric SET series_table = table_name WHERE 1 = 1;
ALTER TABLE _prom_catalog.metric ALTER COLUMN series_table SET NOT NULL;

DROP FUNCTION IF EXISTS _prom_catalog.get_metric_table_name_if_exists(TEXT);
DROP FUNCTION IF EXISTS _prom_catalog.get_confirmed_unused_series( TEXT, BIGINT[], TIMESTAMPTZ);
DROP FUNCTION IF EXISTS _prom_catalog.mark_unused_series(TEXT, TIMESTAMPTZ, TIMESTAMPTZ);
DROP FUNCTION IF EXISTS _prom_catalog.delete_expired_series(TEXT, TIMESTAMPTZ, BIGINT, TIMESTAMPTZ);
DROP FUNCTION IF EXISTS _prom_catalog.drop_metric_chunk_data(TEXT, TIMESTAMPTZ);
DROP PROCEDURE IF EXISTS _prom_catalog.drop_metric_chunks(TEXT, TIMESTAMPTZ, TIMESTAMPTZ, BOOLEAN);
