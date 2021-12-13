CALL SCHEMA_CATALOG.execute_everywhere('tracing_types', $ee$ DO $$ BEGIN

    CREATE DOMAIN SCHEMA_TRACING_PUBLIC.trace_id uuid NOT NULL CHECK (value != '00000000-0000-0000-0000-000000000000');
    GRANT USAGE ON DOMAIN SCHEMA_TRACING_PUBLIC.trace_id TO prom_reader;

    CREATE DOMAIN SCHEMA_TRACING_PUBLIC.tag_k text NOT NULL CHECK (value != '');
    GRANT USAGE ON DOMAIN SCHEMA_TRACING_PUBLIC.tag_k TO prom_reader;

    CREATE DOMAIN SCHEMA_TRACING_PUBLIC.tag_v jsonb NOT NULL;
    GRANT USAGE ON DOMAIN SCHEMA_TRACING_PUBLIC.tag_v TO prom_reader;

    CREATE DOMAIN SCHEMA_TRACING_PUBLIC.tag_map jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(value) = 'object');
    GRANT USAGE ON DOMAIN SCHEMA_TRACING_PUBLIC.tag_map TO prom_reader;

    CREATE DOMAIN SCHEMA_TRACING_PUBLIC.tag_type smallint NOT NULL; --bitmap, may contain several types
    GRANT USAGE ON DOMAIN SCHEMA_TRACING_PUBLIC.tag_type TO prom_reader;

    CREATE TYPE SCHEMA_TRACING_PUBLIC.span_kind AS ENUM
    (
        'SPAN_KIND_UNSPECIFIED',
        'SPAN_KIND_INTERNAL',
        'SPAN_KIND_SERVER',
        'SPAN_KIND_CLIENT',
        'SPAN_KIND_PRODUCER',
        'SPAN_KIND_CONSUMER'
    );
    GRANT USAGE ON TYPE SCHEMA_TRACING_PUBLIC.span_kind TO prom_reader;

    CREATE TYPE SCHEMA_TRACING_PUBLIC.status_code AS ENUM
    (
        'STATUS_CODE_UNSET',
        'STATUS_CODE_OK',
        'STATUS_CODE_ERROR'
    );
    GRANT USAGE ON TYPE SCHEMA_TRACING_PUBLIC.status_code TO prom_reader;
END $$ $ee$);

INSERT INTO public.prom_installation_info(key, value) VALUES
    ('tagging schema',          'SCHEMA_TAG'),
    ('tracing schema',          'SCHEMA_TRACING_PUBLIC'),
    ('tracing schema private',  'SCHEMA_TRACING')
ON CONFLICT (key) DO NOTHING;

CREATE TABLE SCHEMA_TRACING.tag_key
(
    id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tag_type SCHEMA_TRACING_PUBLIC.tag_type NOT NULL,
    key SCHEMA_TRACING_PUBLIC.tag_k NOT NULL
);
CREATE UNIQUE INDEX ON SCHEMA_TRACING.tag_key (key) INCLUDE (id, tag_type);
GRANT SELECT ON TABLE SCHEMA_TRACING.tag_key TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_TRACING.tag_key TO prom_writer;
GRANT USAGE ON SEQUENCE SCHEMA_TRACING.tag_key_id_seq TO prom_writer;

CREATE TABLE SCHEMA_TRACING.tag
(
    id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
    tag_type SCHEMA_TRACING_PUBLIC.tag_type NOT NULL,
    key_id bigint NOT NULL,
    key SCHEMA_TRACING_PUBLIC.tag_k NOT NULL REFERENCES SCHEMA_TRACING.tag_key (key) ON DELETE CASCADE,
    value SCHEMA_TRACING_PUBLIC.tag_v NOT NULL,
    UNIQUE (key, value) INCLUDE (id, key_id)
)
PARTITION BY HASH (key);
GRANT SELECT ON TABLE SCHEMA_TRACING.tag TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_TRACING.tag TO prom_writer;
GRANT USAGE ON SEQUENCE SCHEMA_TRACING.tag_id_seq TO prom_writer;

-- create the partitions of the tag table
DO $block$
DECLARE
    _i bigint;
    _max bigint = 64;
BEGIN
    FOR _i IN 1.._max
    LOOP
        EXECUTE format($sql$
            CREATE TABLE SCHEMA_TRACING.tag_%s PARTITION OF SCHEMA_TRACING.tag FOR VALUES WITH (MODULUS %s, REMAINDER %s)
            $sql$, _i, _max, _i - 1);
        EXECUTE format($sql$
            ALTER TABLE SCHEMA_TRACING.tag_%s ADD PRIMARY KEY (id)
            $sql$, _i);
        EXECUTE format($sql$
            GRANT SELECT ON TABLE SCHEMA_TRACING.tag_%s TO prom_reader
            $sql$, _i);
        EXECUTE format($sql$
            GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_TRACING.tag_%s TO prom_writer
            $sql$, _i);
    END LOOP;
END
$block$
;

CREATE TABLE IF NOT EXISTS SCHEMA_TRACING.operation
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    service_name_id bigint not null, -- references id column of tag table for the service.name tag value
    span_kind SCHEMA_TRACING_PUBLIC.span_kind not null,
    span_name text NOT NULL CHECK (span_name != ''),
    UNIQUE (service_name_id, span_name, span_kind)
);
GRANT SELECT ON TABLE SCHEMA_TRACING.operation TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_TRACING.operation TO prom_writer;
GRANT USAGE ON SEQUENCE SCHEMA_TRACING.operation_id_seq TO prom_writer;

CREATE TABLE IF NOT EXISTS SCHEMA_TRACING.schema_url
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url text NOT NULL CHECK (url != '') UNIQUE
);
GRANT SELECT ON TABLE SCHEMA_TRACING.schema_url TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_TRACING.schema_url TO prom_writer;
GRANT USAGE ON SEQUENCE SCHEMA_TRACING.schema_url_id_seq TO prom_writer;

CREATE TABLE IF NOT EXISTS SCHEMA_TRACING.instrumentation_lib
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL,
    version text NOT NULL,
    schema_url_id BIGINT REFERENCES SCHEMA_TRACING.schema_url(id),
    UNIQUE(name, version, schema_url_id)
);
GRANT SELECT ON TABLE SCHEMA_TRACING.instrumentation_lib TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_TRACING.instrumentation_lib TO prom_writer;
GRANT USAGE ON SEQUENCE SCHEMA_TRACING.instrumentation_lib_id_seq TO prom_writer;

CREATE TABLE IF NOT EXISTS SCHEMA_TRACING.span
(
    trace_id SCHEMA_TRACING_PUBLIC.trace_id NOT NULL,
    span_id bigint NOT NULL CHECK (span_id != 0),
    parent_span_id bigint NULL CHECK (parent_span_id != 0),
    operation_id bigint NOT NULL,
    start_time timestamptz NOT NULL,
    end_time timestamptz NOT NULL,
    duration_ms double precision NOT NULL GENERATED ALWAYS AS ( extract(epoch from (end_time - start_time)) * 1000.0 ) STORED,
    trace_state text CHECK (trace_state != ''),
    span_tags SCHEMA_TRACING_PUBLIC.tag_map NOT NULL,
    dropped_tags_count int NOT NULL default 0,
    event_time tstzrange default NULL,
    dropped_events_count int NOT NULL default 0,
    dropped_link_count int NOT NULL default 0,
    status_code SCHEMA_TRACING_PUBLIC.status_code NOT NULL,
    status_message text,
    instrumentation_lib_id bigint,
    resource_tags SCHEMA_TRACING_PUBLIC.tag_map NOT NULL,
    resource_dropped_tags_count int NOT NULL default 0,
    resource_schema_url_id BIGINT,
    PRIMARY KEY (span_id, trace_id, start_time),
    CHECK (start_time <= end_time)
);
CREATE INDEX ON SCHEMA_TRACING.span USING BTREE (trace_id, parent_span_id) INCLUDE (span_id); -- used for recursive CTEs for trace tree queries
CREATE INDEX ON SCHEMA_TRACING.span USING GIN (span_tags jsonb_path_ops); -- supports tag filters. faster ingest than json_ops
CREATE INDEX ON SCHEMA_TRACING.span USING BTREE (operation_id); -- supports filters/joins to operation table
--CREATE INDEX ON SCHEMA_TRACING.span USING GIN (jsonb_object_keys(span_tags) array_ops); -- possible way to index key exists
CREATE INDEX ON SCHEMA_TRACING.span USING GIN (resource_tags jsonb_path_ops); -- supports tag filters. faster ingest than json_ops
GRANT SELECT ON TABLE SCHEMA_TRACING.span TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_TRACING.span TO prom_writer;

CREATE TABLE IF NOT EXISTS SCHEMA_TRACING.event
(
    time timestamptz NOT NULL,
    trace_id SCHEMA_TRACING_PUBLIC.trace_id NOT NULL,
    span_id bigint NOT NULL CHECK (span_id != 0),
    event_nbr int NOT NULL DEFAULT 0,
    name text NOT NULL,
    tags SCHEMA_TRACING_PUBLIC.tag_map NOT NULL,
    dropped_tags_count int NOT NULL DEFAULT 0
);
CREATE INDEX ON SCHEMA_TRACING.event USING GIN (tags jsonb_path_ops);
CREATE INDEX ON SCHEMA_TRACING.event USING BTREE (trace_id, span_id);
GRANT SELECT ON TABLE SCHEMA_TRACING.event TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_TRACING.event TO prom_writer;

CREATE TABLE IF NOT EXISTS SCHEMA_TRACING.link
(
    trace_id SCHEMA_TRACING_PUBLIC.trace_id NOT NULL,
    span_id bigint NOT NULL CHECK (span_id != 0),
    span_start_time timestamptz NOT NULL,
    linked_trace_id SCHEMA_TRACING_PUBLIC.trace_id NOT NULL,
    linked_span_id bigint NOT NULL CHECK (linked_span_id != 0),
    link_nbr int NOT NULL DEFAULT 0,
    trace_state text CHECK (trace_state != ''),
    tags SCHEMA_TRACING_PUBLIC.tag_map NOT NULL,
    dropped_tags_count int NOT NULL DEFAULT 0
);
CREATE INDEX ON SCHEMA_TRACING.link USING BTREE (trace_id, span_id);
CREATE INDEX ON SCHEMA_TRACING.link USING GIN (tags jsonb_path_ops);
GRANT SELECT ON TABLE SCHEMA_TRACING.link TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_TRACING.link TO prom_writer;

/*
    If "vanilla" postgres is installed, do nothing.
    If timescaledb is installed, turn on compression for tracing tables.
    If timescaledb is installed and multinode is set up,
    turn span, event, and link into distributed hypertables.
    If timescaledb is installed but multinode is NOT set up,
    turn span, event, and link into regular hypertables.
*/
DO $block$
DECLARE
    _is_timescaledb_installed boolean = false;
    _is_timescaledb_oss boolean = true;
    _timescaledb_version_text text;
    _timescaledb_major_version int;
    _timescaledb_minor_version int;
    _is_compression_available boolean = false;
    _is_multinode boolean = false;
    _saved_search_path text;
BEGIN
    /*
        These functions do not exist until the
        idempotent scripts are executed, so we have
        to deal with it "manually"
        SCHEMA_CATALOG.get_timescale_major_version()
        SCHEMA_CATALOG.is_timescaledb_oss()
        SCHEMA_CATALOG.is_timescaledb_installed()
        SCHEMA_CATALOG.is_multinode()
        SCHEMA_CATALOG.get_default_chunk_interval()
        SCHEMA_CATALOG.get_staggered_chunk_interval(...)
    */
    SELECT count(*) > 0
    INTO STRICT _is_timescaledb_installed
    FROM pg_extension
    WHERE extname='timescaledb';

    IF _is_timescaledb_installed THEN
        SELECT extversion INTO STRICT _timescaledb_version_text
        FROM pg_catalog.pg_extension
        WHERE extname='timescaledb'
        LIMIT 1;

        _timescaledb_major_version = split_part(_timescaledb_version_text, '.', 1)::INT;
        _timescaledb_minor_version = split_part(_timescaledb_version_text, '.', 2)::INT;

        _is_compression_available = CASE
            WHEN _timescaledb_major_version >= 2 THEN true
            WHEN _timescaledb_major_version = 1 and _timescaledb_minor_version >= 5 THEN true
            ELSE false
        END;

        IF _timescaledb_major_version >= 2 THEN
            _is_timescaledb_oss = (current_setting('timescaledb.license') = 'apache');
        ELSE
            _is_timescaledb_oss = (SELECT edition = 'apache' FROM timescaledb_information.license);
        END IF;

        IF _timescaledb_major_version >= 2 THEN
            SELECT count(*) > 0
            INTO STRICT _is_multinode
            FROM timescaledb_information.data_nodes;
        END IF;
    END IF;

    IF _is_timescaledb_installed THEN
        IF _is_multinode THEN
            --need to clear the search path while creating distributed
            --hypertables because otherwise the datanodes don't find
            --the right column types since type names are not schema
            --qualified if in search path.
            _saved_search_path := current_setting('search_path');
            SET search_path = pg_temp;
            PERFORM SCHEMA_TIMESCALE.create_distributed_hypertable(
                'SCHEMA_TRACING.span'::regclass,
                'start_time'::name,
                partitioning_column=>'trace_id'::name,
                number_partitions=>1::int,
                chunk_time_interval=>'07:57:57.345608'::interval,
                create_default_indexes=>false
            );
            PERFORM SCHEMA_TIMESCALE.create_distributed_hypertable(
                'SCHEMA_TRACING.event'::regclass,
                'time'::name,
                partitioning_column=>'trace_id'::name,
                number_partitions=>1::int,
                chunk_time_interval=>'07:59:53.649542'::interval,
                create_default_indexes=>false
            );
            PERFORM SCHEMA_TIMESCALE.create_distributed_hypertable(
                'SCHEMA_TRACING.link'::regclass,
                'span_start_time'::name,
                partitioning_column=>'trace_id'::name,
                number_partitions=>1::int,
                chunk_time_interval=>'07:59:48.644258'::interval,
                create_default_indexes=>false
            );
            execute format('SET search_path = %s', _saved_search_path);
        ELSE -- not multinode
            PERFORM SCHEMA_TIMESCALE.create_hypertable(
                'SCHEMA_TRACING.span'::regclass,
                'start_time'::name,
                partitioning_column=>'trace_id'::name,
                number_partitions=>1::int,
                chunk_time_interval=>'07:57:57.345608'::interval,
                create_default_indexes=>false
            );
            PERFORM SCHEMA_TIMESCALE.create_hypertable(
                'SCHEMA_TRACING.event'::regclass,
                'time'::name,
                partitioning_column=>'trace_id'::name,
                number_partitions=>1::int,
                chunk_time_interval=>'07:59:53.649542'::interval,
                create_default_indexes=>false
            );
            PERFORM SCHEMA_TIMESCALE.create_hypertable(
                'SCHEMA_TRACING.link'::regclass,
                'span_start_time'::name,
                partitioning_column=>'trace_id'::name,
                number_partitions=>1::int,
                chunk_time_interval=>'07:59:48.644258'::interval,
                create_default_indexes=>false
            );
        END IF;

        IF (NOT _is_timescaledb_oss) AND _is_compression_available THEN
            -- turn on compression
            ALTER TABLE SCHEMA_TRACING.span SET (timescaledb.compress, timescaledb.compress_segmentby='trace_id,span_id');
            ALTER TABLE SCHEMA_TRACING.event SET (timescaledb.compress, timescaledb.compress_segmentby='trace_id,span_id');
            ALTER TABLE SCHEMA_TRACING.link SET (timescaledb.compress, timescaledb.compress_segmentby='trace_id,span_id');

            IF _timescaledb_major_version < 2 THEN
                BEGIN
                    PERFORM SCHEMA_TIMESCALE.add_compression_policy('SCHEMA_TRACING.span', INTERVAL '1 hour');
                    PERFORM SCHEMA_TIMESCALE.add_compression_policy('SCHEMA_TRACING.event', INTERVAL '1 hour');
                    PERFORM SCHEMA_TIMESCALE.add_compression_policy('SCHEMA_TRACING.link', INTERVAL '1 hour');
                EXCEPTION
                    WHEN undefined_function THEN
                        RAISE NOTICE 'add_compression_policy does not exist';
                END;
            END IF;
        END IF;
    END IF;
END;
$block$
;
