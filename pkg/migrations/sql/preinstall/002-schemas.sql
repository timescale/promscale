CALL _prom_catalog.execute_everywhere('create_schemas', $ee$ DO $$ BEGIN
    CREATE SCHEMA IF NOT EXISTS _prom_catalog; -- this will be limited to metric and probably renamed in future
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

    CREATE SCHEMA IF NOT EXISTS ps_tag;
    GRANT USAGE ON SCHEMA ps_tag TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS _ps_trace;
    GRANT USAGE ON SCHEMA _ps_trace TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS ps_trace;
    GRANT USAGE ON SCHEMA ps_trace TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS _ps_catalog;
    GRANT USAGE ON SCHEMA _ps_catalog TO prom_reader;
END $$ $ee$);

-- the promscale extension contains optimized version of some
-- of our functions and operators. To ensure the correct version of the are
-- used, _prom_ext must be before all of our other schemas in the search path
DO $$
DECLARE
   new_path text;
BEGIN
   new_path := current_setting('search_path') || format(',%L,%L,%L,%L,%L,%L', 'ps_tag', '_prom_ext', 'prom_api', 'prom_metric', '_prom_catalog', 'ps_trace');
   execute format('ALTER DATABASE %I SET search_path = %s', current_database(), new_path);
   execute format('SET search_path = %s', new_path);
END
$$;
