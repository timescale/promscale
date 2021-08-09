
CREATE SCHEMA IF NOT EXISTS SCHEMA_TRACING;
GRANT USAGE ON SCHEMA SCHEMA_TRACING TO prom_reader;

CREATE SCHEMA IF NOT EXISTS SCHEMA_TRACING_PUBLIC;
GRANT USAGE ON SCHEMA SCHEMA_TRACING_PUBLIC TO prom_reader;

CALL SCHEMA_CATALOG.execute_everywhere('tracing_types', $ee$ DO $$ BEGIN
    CREATE DOMAIN SCHEMA_TRACING_PUBLIC.trace_id uuid NOT NULL CHECK (value != '00000000-0000-0000-0000-000000000000');
    GRANT USAGE ON DOMAIN SCHEMA_TRACING_PUBLIC.trace_id TO prom_reader;

    CREATE DOMAIN SCHEMA_TRACING_PUBLIC.tag_k text NOT NULL CHECK (value != '');
    GRANT USAGE ON DOMAIN SCHEMA_TRACING_PUBLIC.tag_k TO prom_reader;

    CREATE DOMAIN SCHEMA_TRACING_PUBLIC.tag_v jsonb NOT NULL;
    GRANT USAGE ON DOMAIN SCHEMA_TRACING_PUBLIC.tag_v TO prom_reader;

    CREATE DOMAIN SCHEMA_TRACING_PUBLIC.tag_map jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(value) = 'object');
    GRANT USAGE ON DOMAIN SCHEMA_TRACING_PUBLIC.tag_map TO prom_reader;

    CREATE DOMAIN SCHEMA_TRACING_PUBLIC.tag_matchers SCHEMA_TRACING_PUBLIC.tag_map[] NOT NULL;
    GRANT USAGE ON DOMAIN SCHEMA_TRACING_PUBLIC.tag_matchers TO prom_reader;

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

CREATE TABLE IF NOT EXISTS SCHEMA_TRACING.span_name
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL CHECK (name != '') UNIQUE
);
GRANT SELECT ON TABLE SCHEMA_TRACING.span_name TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_TRACING.span_name TO prom_writer;
GRANT USAGE ON SEQUENCE SCHEMA_TRACING.span_name_id_seq TO prom_writer;

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
    span_id bigint NOT NULL,
    parent_span_id bigint NULL,
    name_id bigint NOT NULL,
    start_time timestamptz NOT NULL,
    end_time timestamptz NOT NULL,
    trace_state text CHECK (trace_state != ''),
    span_kind SCHEMA_TRACING_PUBLIC.span_kind,
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
CREATE INDEX ON SCHEMA_TRACING.span USING BTREE (name_id); -- supports filters/joins to span_name table
--CREATE INDEX ON SCHEMA_TRACING.span USING GIN (jsonb_object_keys(span_tags) array_ops); -- possible way to index key exists
CREATE INDEX ON SCHEMA_TRACING.span USING GIN (resource_tags jsonb_path_ops); -- supports tag filters. faster ingest than json_ops
GRANT SELECT ON TABLE SCHEMA_TRACING.span TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_TRACING.span TO prom_writer;

CREATE TABLE IF NOT EXISTS SCHEMA_TRACING.event
(
    time timestamptz NOT NULL,
    trace_id SCHEMA_TRACING_PUBLIC.trace_id NOT NULL,
    span_id bigint NOT NULL,
    event_nbr int NOT NULL DEFAULT 0,
    name text NOT NULL CHECK (name != ''),
    tags SCHEMA_TRACING_PUBLIC.tag_map NOT NULL,
    dropped_tags_count int NOT NULL DEFAULT 0
);
CREATE INDEX ON SCHEMA_TRACING.event USING GIN (tags jsonb_path_ops);
CREATE INDEX ON SCHEMA_TRACING.event USING BTREE (span_id, time) INCLUDE (trace_id);
GRANT SELECT ON TABLE SCHEMA_TRACING.event TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_TRACING.event TO prom_writer;

CREATE TABLE IF NOT EXISTS SCHEMA_TRACING.link
(
    trace_id SCHEMA_TRACING_PUBLIC.trace_id NOT NULL,
    span_id bigint NOT NULL,
    span_start_time timestamptz NOT NULL,
    linked_trace_id SCHEMA_TRACING_PUBLIC.trace_id NOT NULL,
    linked_span_id bigint NOT NULL,
    link_nbr int NOT NULL DEFAULT 0,
    trace_state text CHECK (trace_state != ''),
    tags SCHEMA_TRACING_PUBLIC.tag_map NOT NULL,
    dropped_tags_count int NOT NULL DEFAULT 0
);
CREATE INDEX ON SCHEMA_TRACING.link USING BTREE (span_id, span_start_time) INCLUDE (trace_id);
CREATE INDEX ON SCHEMA_TRACING.link USING GIN (tags jsonb_path_ops);
GRANT SELECT ON TABLE SCHEMA_TRACING.link TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_TRACING.link TO prom_writer;

/*
    If "vanilla" postgres is installed, do nothing.
    If timescaledb is installed and multinode is set up,
    turn span, event, and link into distributed hypertables.
    If timescaledb is installed but multinode is NOT set up,
    turn span, event, and link into regular hypertables.
*/
DO $block$
DECLARE
    _is_timescaledb_installed boolean = false;
    _timescaledb_major_version int;
    _is_multinode boolean = false;
    _saved_search_path text;
BEGIN
    /*
        These functions do not exist until the
        idempotent scripts are executed, so we have
        to deal with it "manually"
        SCHEMA_CATALOG.get_timescale_major_version()
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
        SELECT split_part(extversion, '.', 1)::INT
        INTO STRICT _timescaledb_major_version
        FROM pg_catalog.pg_extension
        WHERE extname='timescaledb'
        LIMIT 1;
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
        ELSE
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
    END IF;
END;
$block$
;

