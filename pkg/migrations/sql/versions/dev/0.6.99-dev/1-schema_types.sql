CREATE SCHEMA IF NOT EXISTS ps_tag;
GRANT USAGE ON SCHEMA ps_tag TO prom_reader;

CREATE SCHEMA IF NOT EXISTS _ps_trace;
GRANT USAGE ON SCHEMA _ps_trace TO prom_reader;

CREATE SCHEMA IF NOT EXISTS ps_trace;
GRANT USAGE ON SCHEMA ps_trace TO prom_reader;

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

    CREATE SCHEMA IF NOT EXISTS ps_tag;
    GRANT USAGE ON SCHEMA ps_tag TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS _ps_trace;
    GRANT USAGE ON SCHEMA _ps_trace TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS ps_trace;
    GRANT USAGE ON SCHEMA ps_trace TO prom_reader;
END $$ $ee$);

CALL _prom_catalog.execute_everywhere('tracing_types', $ee$ DO $$ BEGIN

    CREATE DOMAIN ps_trace.tag_k text NOT NULL CHECK (value != '');
    GRANT USAGE ON DOMAIN ps_trace.tag_k TO prom_reader;

    CREATE DOMAIN ps_trace.tag_v jsonb NOT NULL;
    GRANT USAGE ON DOMAIN ps_trace.tag_v TO prom_reader;

    CREATE DOMAIN ps_trace.tag_map jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(value) = 'object');
    GRANT USAGE ON DOMAIN ps_trace.tag_map TO prom_reader;

    CREATE DOMAIN ps_trace.tag_type smallint NOT NULL; --bitmap, may contain several types
    GRANT USAGE ON DOMAIN ps_trace.tag_type TO prom_reader;

    CREATE TYPE ps_trace.span_kind AS ENUM
    (
        'SPAN_KIND_UNSPECIFIED',
        'SPAN_KIND_INTERNAL',
        'SPAN_KIND_SERVER',
        'SPAN_KIND_CLIENT',
        'SPAN_KIND_PRODUCER',
        'SPAN_KIND_CONSUMER'
    );
    GRANT USAGE ON TYPE ps_trace.span_kind TO prom_reader;

    CREATE TYPE ps_trace.status_code AS ENUM
    (
        'STATUS_CODE_UNSET',
        'STATUS_CODE_OK',
        'STATUS_CODE_ERROR'
    );
    GRANT USAGE ON TYPE ps_trace.status_code TO prom_reader;
END $$ $ee$);

UPDATE _prom_catalog.remote_commands SET seq = seq+2 WHERE seq >= 8;
UPDATE _prom_catalog.remote_commands SET seq = 9 WHERE key='tracing_types';

DO $$
DECLARE
   new_path text;
BEGIN
   new_path := current_setting('search_path') || format(',%L,%L,%L,%L,%L,%L', 'ps_tag', '_prom_ext', 'prom_api', 'prom_metric', '_prom_catalog', 'ps_trace');
   execute format('ALTER DATABASE %I SET search_path = %s', current_database(), new_path);
   execute format('SET search_path = %s', new_path);
END
$$;

INSERT INTO public.prom_installation_info(key, value) VALUES
    ('tagging schema',          'ps_tag'),
    ('tracing schema',          'ps_trace'),
    ('tracing schema private',  '_ps_trace')
ON CONFLICT (key) DO NOTHING;
