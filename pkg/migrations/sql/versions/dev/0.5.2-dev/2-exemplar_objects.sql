-- delete the get_new_label_pos so that base.sql creates the new one.
DROP FUNCTION IF EXISTS SCHEMA_CATALOG.get_new_pos_for_key(text, text[]);

CREATE DOMAIN SCHEMA_PROM.label_value_array AS TEXT[];

CREATE SCHEMA IF NOT EXISTS SCHEMA_DATA_EXEMPLAR;
GRANT USAGE ON SCHEMA SCHEMA_DATA_EXEMPLAR TO prom_reader;
GRANT ALL ON SCHEMA SCHEMA_DATA_EXEMPLAR TO prom_writer;

CREATE TABLE IF NOT EXISTS SCHEMA_CATALOG.exemplar_label_key_position (
  metric_name TEXT NOT NULL,
  key         TEXT NOT NULL,
  pos         INTEGER NOT NULL,
  PRIMARY KEY (metric_name, key) INCLUDE (pos)
);
GRANT SELECT ON TABLE SCHEMA_CATALOG.exemplar_label_key_position TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_CATALOG.exemplar_label_key_position TO prom_writer;

CREATE TABLE IF NOT EXISTS SCHEMA_CATALOG.exemplar (
   id          SERIAL PRIMARY KEY,
   metric_name TEXT NOT NULL,
   table_name  TEXT NOT NULL,
   UNIQUE (metric_name) INCLUDE (table_name, id)
);
GRANT SELECT ON TABLE SCHEMA_CATALOG.exemplar TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_CATALOG.exemplar TO prom_writer;

GRANT USAGE, SELECT ON SEQUENCE exemplar_id_seq TO prom_writer;

INSERT INTO public.prom_installation_info(key, value) VALUES
    ('exemplar data schema',  'SCHEMA_DATA_EXEMPLAR');

------------------ op.@> -----------------
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_value_contains(labels SCHEMA_PROM.label_value_array, label_value TEXT)
RETURNS BOOLEAN
AS $func$
    SELECT labels @> ARRAY[label_value]::TEXT[]
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_PROM.@> (
    LEFTARG = SCHEMA_PROM.label_value_array,
    RIGHTARG = TEXT,
    FUNCTION = SCHEMA_CATALOG.label_value_contains
);

CALL SCHEMA_CATALOG.execute_everywhere('create_schemas', $ee$ DO $$ BEGIN
    CREATE SCHEMA IF NOT EXISTS SCHEMA_CATALOG; -- catalog tables + internal functions
    GRANT USAGE ON SCHEMA SCHEMA_CATALOG TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_PROM; -- public functions
    GRANT USAGE ON SCHEMA SCHEMA_PROM TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_EXT; -- optimized versions of functions created by the extension
    GRANT USAGE ON SCHEMA SCHEMA_EXT TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_SERIES; -- series views
    GRANT USAGE ON SCHEMA SCHEMA_SERIES TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_METRIC; -- metric views
    GRANT USAGE ON SCHEMA SCHEMA_METRIC TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_DATA;
    GRANT USAGE ON SCHEMA SCHEMA_DATA TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_DATA_SERIES;
    GRANT USAGE ON SCHEMA SCHEMA_DATA_SERIES TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_INFO;
    GRANT USAGE ON SCHEMA SCHEMA_INFO TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_DATA_EXEMPLAR;
    GRANT USAGE ON SCHEMA SCHEMA_DATA_EXEMPLAR TO prom_reader;
    GRANT ALL ON SCHEMA SCHEMA_DATA_EXEMPLAR TO prom_writer;
END $$ $ee$);