-- delete the get_new_label_pos so that base.sql creates the new one.
DROP FUNCTION IF EXISTS _prom_catalog.get_new_pos_for_key(text, text[]);

CREATE DOMAIN prom_api.label_value_array AS TEXT[];

CREATE SCHEMA IF NOT EXISTS prom_data_exemplar;
GRANT USAGE ON SCHEMA prom_data_exemplar TO prom_reader;
GRANT ALL ON SCHEMA prom_data_exemplar TO prom_writer;

CREATE TABLE IF NOT EXISTS _prom_catalog.exemplar_label_key_position (
  metric_name TEXT NOT NULL,
  key         TEXT NOT NULL,
  pos         INTEGER NOT NULL,
  PRIMARY KEY (metric_name, key) INCLUDE (pos)
);
GRANT SELECT ON TABLE _prom_catalog.exemplar_label_key_position TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _prom_catalog.exemplar_label_key_position TO prom_writer;

CREATE TABLE IF NOT EXISTS _prom_catalog.exemplar (
   id          SERIAL PRIMARY KEY,
   metric_name TEXT NOT NULL,
   table_name  TEXT NOT NULL,
   UNIQUE (metric_name) INCLUDE (table_name, id)
);
GRANT SELECT ON TABLE _prom_catalog.exemplar TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _prom_catalog.exemplar TO prom_writer;

GRANT USAGE, SELECT ON SEQUENCE exemplar_id_seq TO prom_writer;

INSERT INTO public.prom_installation_info(key, value) VALUES
    ('exemplar data schema',  'prom_data_exemplar');

------------------ op.@> -----------------
CREATE OR REPLACE FUNCTION _prom_catalog.label_value_contains(labels prom_api.label_value_array, label_value TEXT)
RETURNS BOOLEAN
AS $func$
    SELECT labels @> ARRAY[label_value]::TEXT[]
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR prom_api.@> (
    LEFTARG = prom_api.label_value_array,
    RIGHTARG = TEXT,
    FUNCTION = _prom_catalog.label_value_contains
);

CALL _prom_catalog.execute_everywhere('create_schemas', $ee$ DO $$ BEGIN
    CREATE SCHEMA IF NOT EXISTS _prom_catalog; -- catalog tables + internal functions
    GRANT USAGE ON SCHEMA _prom_catalog TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_api; -- public functions
    GRANT USAGE ON SCHEMA prom_api TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS _prom_ext; -- optimized versions of functions created by the extension
    GRANT USAGE ON SCHEMA _prom_ext TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_series; -- series views
    GRANT USAGE ON SCHEMA prom_series TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_metric; -- metric views
    GRANT USAGE ON SCHEMA prom_metric TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_data;
    GRANT USAGE ON SCHEMA prom_data TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_data_series;
    GRANT USAGE ON SCHEMA prom_data_series TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_info;
    GRANT USAGE ON SCHEMA prom_info TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_data_exemplar;
    GRANT USAGE ON SCHEMA prom_data_exemplar TO prom_reader;
    GRANT ALL ON SCHEMA prom_data_exemplar TO prom_writer;
END $$ $ee$);