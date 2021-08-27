BEGIN; -- need to run in a single transaction to set series_table column defaults to table_name
ALTER TABLE SCHEMA_CATALOG.metric
    ADD COLUMN table_schema name NOT NULL DEFAULT 'SCHEMA_DATA',
    ADD COLUMN series_table name, -- series_table stores the name of the table used to store the series data for this metric.
    ADD COLUMN is_view BOOLEAN NOT NULL DEFAULT false,
    DROP CONSTRAINT metric_metric_name_table_name_key,
    DROP CONSTRAINT metric_table_name_key,
    ADD CONSTRAINT metric_metric_name_table_schema_table_name_key UNIQUE (metric_name, table_schema) INCLUDE (table_name),
    ADD CONSTRAINT metric_table_schema_table_name_key UNIQUE(table_schema, table_name);

UPDATE SCHEMA_CATALOG.metric SET series_table = table_name;
ALTER TABLE SCHEMA_CATALOG.metric ALTER COLUMN series_table SET NOT NULL;
COMMIT;

-- table for storing continuous aggregate information for metric views that are based on them
CREATE TABLE SCHEMA_CATALOG.metric_view_cagg (
    view_schema NAME,
    view_name NAME,
    cagg_schema NAME,
    cagg_name NAME,
    PRIMARY KEY (view_schema, view_name)
);
GRANT SELECT ON TABLE SCHEMA_CATALOG.metric_view_cagg TO prom_reader;

DROP FUNCTION IF EXISTS SCHEMA_CATALOG.get_metric_table_name_if_exists(TEXT);
DROP FUNCTION IF EXISTS SCHEMA_CATALOG.get_confirmed_unused_series( TEXT, BIGINT[], TIMESTAMPTZ); 
DROP FUNCTION IF EXISTS SCHEMA_CATALOG.mark_unused_series(TEXT, TIMESTAMPTZ, TIMESTAMPTZ);
DROP FUNCTION IF EXISTS SCHEMA_CATALOG.delete_expired_series(TEXT, TIMESTAMPTZ, BIGINT, TIMESTAMPTZ);
DROP FUNCTION IF EXISTS SCHEMA_CATALOG.drop_metric_chunk_data(TEXT, TIMESTAMPTZ);
DROP FUNCTION IF EXISTS SCHEMA_CATALOG.drop_metric_chunks(TEXT, TIMESTAMPTZ, TIMESTAMPTZ, BOOLEAN);
