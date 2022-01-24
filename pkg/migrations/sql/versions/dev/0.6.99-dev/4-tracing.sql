CREATE TABLE _ps_trace.tag_key
(
    id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tag_type ps_trace.tag_type NOT NULL,
    key ps_trace.tag_k NOT NULL
);
CREATE UNIQUE INDEX ON _ps_trace.tag_key (key) INCLUDE (id, tag_type);
GRANT SELECT ON TABLE _ps_trace.tag_key TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _ps_trace.tag_key TO prom_writer;
GRANT USAGE ON SEQUENCE _ps_trace.tag_key_id_seq TO prom_writer;

CREATE TABLE _ps_trace.tag
(
    id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
    tag_type ps_trace.tag_type NOT NULL,
    key_id bigint NOT NULL,
    key ps_trace.tag_k NOT NULL REFERENCES _ps_trace.tag_key (key) ON DELETE CASCADE,
    value ps_trace.tag_v NOT NULL,
    UNIQUE (key, value) INCLUDE (id, key_id)
)
PARTITION BY HASH (key);
GRANT SELECT ON TABLE _ps_trace.tag TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _ps_trace.tag TO prom_writer;
GRANT USAGE ON SEQUENCE _ps_trace.tag_id_seq TO prom_writer;

-- create the partitions of the tag table
DO $block$
DECLARE
    _i bigint;
    _max bigint = 64;
BEGIN
    FOR _i IN 1.._max
    LOOP
        EXECUTE format($sql$
            CREATE TABLE _ps_trace.tag_%s PARTITION OF _ps_trace.tag FOR VALUES WITH (MODULUS %s, REMAINDER %s)
            $sql$, _i, _max, _i - 1);
        EXECUTE format($sql$
            ALTER TABLE _ps_trace.tag_%s ADD PRIMARY KEY (id)
            $sql$, _i);
        EXECUTE format($sql$
            GRANT SELECT ON TABLE _ps_trace.tag_%s TO prom_reader
            $sql$, _i);
        EXECUTE format($sql$
            GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _ps_trace.tag_%s TO prom_writer
            $sql$, _i);
    END LOOP;
END
$block$
;

CREATE TABLE IF NOT EXISTS _ps_trace.operation
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    service_name_id bigint not null, -- references id column of tag table for the service.name tag value
    span_kind ps_trace.span_kind not null,
    span_name text NOT NULL CHECK (span_name != ''),
    UNIQUE (service_name_id, span_name, span_kind)
);
GRANT SELECT ON TABLE _ps_trace.operation TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _ps_trace.operation TO prom_writer;
GRANT USAGE ON SEQUENCE _ps_trace.operation_id_seq TO prom_writer;

CREATE TABLE IF NOT EXISTS _ps_trace.schema_url
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url text NOT NULL CHECK (url != '') UNIQUE
);
GRANT SELECT ON TABLE _ps_trace.schema_url TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _ps_trace.schema_url TO prom_writer;
GRANT USAGE ON SEQUENCE _ps_trace.schema_url_id_seq TO prom_writer;

CREATE TABLE IF NOT EXISTS _ps_trace.instrumentation_lib
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL,
    version text NOT NULL,
    schema_url_id BIGINT REFERENCES _ps_trace.schema_url(id),
    UNIQUE(name, version, schema_url_id)
);
GRANT SELECT ON TABLE _ps_trace.instrumentation_lib TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _ps_trace.instrumentation_lib TO prom_writer;
GRANT USAGE ON SEQUENCE _ps_trace.instrumentation_lib_id_seq TO prom_writer;

CREATE TABLE IF NOT EXISTS _ps_trace.span
(
    trace_id ps_trace.trace_id NOT NULL,
    span_id bigint NOT NULL,
    parent_span_id bigint NULL,
    operation_id bigint NOT NULL,
    start_time timestamptz NOT NULL,
    end_time timestamptz NOT NULL,
    duration_ms double precision NOT NULL GENERATED ALWAYS AS ( extract(epoch from (end_time - start_time)) * 1000.0 ) STORED,
    trace_state text CHECK (trace_state != ''),
    span_tags ps_trace.tag_map NOT NULL,
    dropped_tags_count int NOT NULL default 0,
    event_time tstzrange default NULL,
    dropped_events_count int NOT NULL default 0,
    dropped_link_count int NOT NULL default 0,
    status_code ps_trace.status_code NOT NULL,
    status_message text,
    instrumentation_lib_id bigint,
    resource_tags ps_trace.tag_map NOT NULL,
    resource_dropped_tags_count int NOT NULL default 0,
    resource_schema_url_id BIGINT,
    PRIMARY KEY (span_id, trace_id, start_time),
    CHECK (start_time <= end_time)
);
CREATE INDEX ON _ps_trace.span USING BTREE (trace_id, parent_span_id) INCLUDE (span_id); -- used for recursive CTEs for trace tree queries
CREATE INDEX ON _ps_trace.span USING GIN (span_tags jsonb_path_ops); -- supports tag filters. faster ingest than json_ops
CREATE INDEX ON _ps_trace.span USING BTREE (operation_id); -- supports filters/joins to operation table
--CREATE INDEX ON _ps_trace.span USING GIN (jsonb_object_keys(span_tags) array_ops); -- possible way to index key exists
CREATE INDEX ON _ps_trace.span USING GIN (resource_tags jsonb_path_ops); -- supports tag filters. faster ingest than json_ops
GRANT SELECT ON TABLE _ps_trace.span TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _ps_trace.span TO prom_writer;

CREATE TABLE IF NOT EXISTS _ps_trace.event
(
    time timestamptz NOT NULL,
    trace_id ps_trace.trace_id NOT NULL,
    span_id bigint NOT NULL,
    event_nbr int NOT NULL DEFAULT 0,
    name text NOT NULL CHECK (name != ''),
    tags ps_trace.tag_map NOT NULL,
    dropped_tags_count int NOT NULL DEFAULT 0
);
CREATE INDEX ON _ps_trace.event USING GIN (tags jsonb_path_ops);
CREATE INDEX ON _ps_trace.event USING BTREE (trace_id, span_id);
GRANT SELECT ON TABLE _ps_trace.event TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _ps_trace.event TO prom_writer;

CREATE TABLE IF NOT EXISTS _ps_trace.link
(
    trace_id ps_trace.trace_id NOT NULL,
    span_id bigint NOT NULL,
    span_start_time timestamptz NOT NULL,
    linked_trace_id ps_trace.trace_id NOT NULL,
    linked_span_id bigint NOT NULL,
    link_nbr int NOT NULL DEFAULT 0,
    trace_state text CHECK (trace_state != ''),
    tags ps_trace.tag_map NOT NULL,
    dropped_tags_count int NOT NULL DEFAULT 0
);
CREATE INDEX ON _ps_trace.link USING BTREE (trace_id, span_id);
CREATE INDEX ON _ps_trace.link USING GIN (tags jsonb_path_ops);
GRANT SELECT ON TABLE _ps_trace.link TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _ps_trace.link TO prom_writer;

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
        _prom_catalog.get_timescale_major_version()
        _prom_catalog.is_timescaledb_installed()
        _prom_catalog.is_multinode()
        _prom_catalog.get_default_chunk_interval()
        _prom_catalog.get_staggered_chunk_interval(...)
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
            PERFORM public.create_distributed_hypertable(
                '_ps_trace.span'::regclass,
                'start_time'::name,
                partitioning_column=>'trace_id'::name,
                number_partitions=>1::int,
                chunk_time_interval=>'07:57:57.345608'::interval,
                create_default_indexes=>false
            );
            PERFORM public.create_distributed_hypertable(
                '_ps_trace.event'::regclass,
                'time'::name,
                partitioning_column=>'trace_id'::name,
                number_partitions=>1::int,
                chunk_time_interval=>'07:59:53.649542'::interval,
                create_default_indexes=>false
            );
            PERFORM public.create_distributed_hypertable(
                '_ps_trace.link'::regclass,
                'span_start_time'::name,
                partitioning_column=>'trace_id'::name,
                number_partitions=>1::int,
                chunk_time_interval=>'07:59:48.644258'::interval,
                create_default_indexes=>false
            );
            execute format('SET search_path = %s', _saved_search_path);
        ELSE
            PERFORM public.create_hypertable(
                '_ps_trace.span'::regclass,
                'start_time'::name,
                partitioning_column=>'trace_id'::name,
                number_partitions=>1::int,
                chunk_time_interval=>'07:57:57.345608'::interval,
                create_default_indexes=>false
            );
            PERFORM public.create_hypertable(
                '_ps_trace.event'::regclass,
                'time'::name,
                partitioning_column=>'trace_id'::name,
                number_partitions=>1::int,
                chunk_time_interval=>'07:59:53.649542'::interval,
                create_default_indexes=>false
            );
            PERFORM public.create_hypertable(
                '_ps_trace.link'::regclass,
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
