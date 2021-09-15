
CREATE SCHEMA IF NOT EXISTS _ps_trace;
GRANT USAGE ON SCHEMA _ps_trace TO prom_reader;

CREATE SCHEMA IF NOT EXISTS ps_trace;
GRANT USAGE ON SCHEMA ps_trace TO prom_reader;

CREATE DOMAIN _ps_trace.trace_id uuid NOT NULL CHECK (value != '00000000-0000-0000-0000-000000000000');
GRANT USAGE ON DOMAIN _ps_trace.trace_id TO prom_reader;

CREATE DOMAIN _ps_trace.tag_k text NOT NULL CHECK (value != '');
GRANT USAGE ON DOMAIN _ps_trace.tag_k TO prom_reader;

CREATE DOMAIN _ps_trace.tag_v jsonb NOT NULL;
GRANT USAGE ON DOMAIN _ps_trace.tag_v TO prom_reader;

CREATE DOMAIN _ps_trace.tag_map jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(value) = 'object');
GRANT USAGE ON DOMAIN _ps_trace.tag_map TO prom_reader;

CREATE DOMAIN _ps_trace.tag_maps _ps_trace.tag_map[] NOT NULL;
GRANT USAGE ON DOMAIN _ps_trace.tag_maps TO prom_reader;
/*
CREATE DOMAIN _ps_trace.tag_type smallint NOT NULL;
GRANT USAGE ON DOMAIN _ps_trace.tag_type TO prom_reader;
*/
CREATE TABLE _ps_trace.tag_key
(
    id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
--    tag_type _ps_trace.tag_type NOT NULL,
    key _ps_trace.tag_k NOT NULL
);
CREATE UNIQUE INDEX ON _ps_trace.tag_key (key) INCLUDE (id/*, tag_type*/);
GRANT SELECT ON TABLE _ps_trace.tag_key TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _ps_trace.tag_key TO prom_writer;
GRANT USAGE ON SEQUENCE _ps_trace.tag_key_id_seq TO prom_writer;

CREATE TABLE _ps_trace.tag
(
    id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
    --tag_type _ps_trace.tag_type NOT NULL,
    key_id bigint NOT NULL,
    key _ps_trace.tag_k NOT NULL REFERENCES _ps_trace.tag_key (key) ON DELETE CASCADE,
    value _ps_trace.tag_v NOT NULL,
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

CREATE TYPE _ps_trace.span_kind AS ENUM
(
    'SPAN_KIND_UNSPECIFIED',
    'SPAN_KIND_INTERNAL',
    'SPAN_KIND_SERVER',
    'SPAN_KIND_CLIENT',
    'SPAN_KIND_PRODUCER',
    'SPAN_KIND_CONSUMER'
);
GRANT USAGE ON TYPE _ps_trace.span_kind TO prom_reader;

CREATE TYPE _ps_trace.status_code AS ENUM
(
    'STATUS_CODE_UNSET',
    'STATUS_CODE_OK',
    'STATUS_CODE_ERROR'
);
GRANT USAGE ON TYPE _ps_trace.status_code TO prom_reader;

CREATE TABLE IF NOT EXISTS _ps_trace.span_name
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL CHECK (name != '') UNIQUE
);
GRANT SELECT ON TABLE _ps_trace.span_name TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _ps_trace.span_name TO prom_writer;
GRANT USAGE ON SEQUENCE _ps_trace.span_name_id_seq TO prom_writer;

CREATE TABLE IF NOT EXISTS _ps_trace.schema_url
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url text NOT NULL CHECK (url != '') UNIQUE
);
GRANT SELECT ON TABLE _ps_trace.schema_url TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _ps_trace.schema_url TO prom_writer;
GRANT USAGE ON SEQUENCE _ps_trace.schema_url_id_seq TO prom_writer;

CREATE TABLE IF NOT EXISTS _ps_trace.inst_lib
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL,
    version text NOT NULL,
    schema_url_id BIGINT NOT NULL REFERENCES _ps_trace.schema_url(id),
    UNIQUE(name, version, schema_url_id)
);
GRANT SELECT ON TABLE _ps_trace.inst_lib TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _ps_trace.inst_lib TO prom_writer;
GRANT USAGE ON SEQUENCE _ps_trace.inst_lib_id_seq TO prom_writer;
/*
CREATE TABLE IF NOT EXISTS _ps_trace.trace
(
    id _ps_trace.trace_id NOT NULL PRIMARY KEY,
    root_span_id bigint not null,
    span_count int not null default 0,
    span_time_range tstzrange NOT NULL default tstzrange('infinity', 'infinity', '()'),
    event_time_range tstzrange NOT NULL default tstzrange('infinity', 'infinity', '()') --should this be included?
--    span_tree jsonb
);
CREATE INDEX ON _ps_trace.trace USING GIST (span_time_range) INCLUDE (id);
GRANT SELECT ON TABLE _ps_trace.trace TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _ps_trace.trace TO prom_writer;
*/
CREATE TABLE IF NOT EXISTS _ps_trace.span
(
    trace_id _ps_trace.trace_id NOT NULL,
    span_id bigint NOT NULL,
    trace_state text,
    parent_span_id bigint NULL,
    name_id bigint NOT NULL,
    span_kind _ps_trace.span_kind,
    start_time timestamptz NOT NULL,
    end_time timestamptz NOT NULL,
    span_tags _ps_trace.tag_map,
    dropped_tags_count int NOT NULL default 0,
    event_time tstzrange NOT NULL default tstzrange('infinity', 'infinity', '()'),
    dropped_events_count int NOT NULL default 0,
    dropped_link_count int NOT NULL default 0,
    status_code _ps_trace.status_code,
    status_message text,
    inst_lib_id bigint,
    resource_tags _ps_trace.tag_map,
    resource_dropped_tags_count int NOT NULL default 0,
    resource_schema_url_id BIGINT NOT NULL,
    PRIMARY KEY (span_id, trace_id, start_time),
    CHECK (start_time <= end_time)
);
CREATE INDEX ON _ps_trace.span USING BTREE (trace_id, parent_span_id);
CREATE INDEX ON _ps_trace.span USING GIN (span_tags jsonb_path_ops);
--CREATE INDEX ON _ps_trace.span USING GIN (jsonb_object_keys(span_tags) array_ops); -- possible way to index key exists
CREATE INDEX ON _ps_trace.span USING GIN (resource_tags jsonb_path_ops);
SELECT create_hypertable('_ps_trace.span', 'start_time', partitioning_column=>'trace_id', number_partitions=>1);
GRANT SELECT ON TABLE _ps_trace.span TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _ps_trace.span TO prom_writer;

CREATE TABLE IF NOT EXISTS _ps_trace.event
(
    time timestamptz NOT NULL,
    trace_id _ps_trace.trace_id NOT NULL,
    span_id bigint NOT NULL,
    event_number smallint NOT NULL,
    name text NOT NULL CHECK (name != ''),
    tags _ps_trace.tag_map,
    dropped_tags_count int NOT NULL DEFAULT 0
);
CREATE INDEX ON _ps_trace.event USING GIN (tags jsonb_path_ops);
CREATE INDEX ON _ps_trace.event USING BTREE (span_id, time) INCLUDE (trace_id);
SELECT create_hypertable('_ps_trace.event', 'time', partitioning_column=>'trace_id', number_partitions=>1);
GRANT SELECT ON TABLE _ps_trace.event TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _ps_trace.event TO prom_writer;

CREATE TABLE IF NOT EXISTS _ps_trace.link
(
    trace_id _ps_trace.trace_id NOT NULL,
    span_id bigint NOT NULL,
    span_start_time timestamptz NOT NULL,
    span_name_id BIGINT NOT NULL REFERENCES _ps_trace.span_name (id),
    linked_trace_id _ps_trace.trace_id NOT NULL,
    linked_span_id bigint NOT NULL,
    trace_state text,
    tags _ps_trace.tag_map,
    dropped_tags_count int NOT NULL DEFAULT 0,
    event_number smallint NOT NULL
);
CREATE INDEX ON _ps_trace.link USING BTREE (span_id, span_start_time) INCLUDE (trace_id);
CREATE INDEX ON _ps_trace.link USING GIN (tags jsonb_path_ops);
SELECT create_hypertable('_ps_trace.link', 'span_start_time', partitioning_column=>'trace_id', number_partitions=>1);
GRANT SELECT ON TABLE _ps_trace.link TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _ps_trace.link TO prom_writer;
/*
CREATE OR REPLACE FUNCTION _ps_trace.tag_k(_key text)
RETURNS _ps_trace.tag_k
AS $sql$
    SELECT _key::_ps_trace.tag_k
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION _ps_trace.tag_k(text) TO prom_reader;

CREATE CAST (text as _ps_trace.tag_k) WITH FUNCTION _ps_trace.tag_k(text) AS IMPLICIT;

CREATE OPERATOR _ps_trace.# (
    RIGHTARG = text,
    FUNCTION = _ps_trace.tag_k
);
*/
CREATE OR REPLACE FUNCTION _ps_trace.tag_maps_query(_key _ps_trace.tag_k, _path jsonpath)
RETURNS _ps_trace.tag_maps
AS $sql$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::_ps_trace.tag_maps
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OPERATOR _ps_trace.@? (
    LEFTARG = _ps_trace.tag_k,
    RIGHTARG = jsonpath,
    FUNCTION = _ps_trace.tag_maps_query
);

CREATE OR REPLACE FUNCTION _ps_trace.tag_maps_regex(_key _ps_trace.tag_k, _pattern text)
RETURNS _ps_trace.tag_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT '{}'::_ps_trace.tag_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OPERATOR _ps_trace.==~ (
    LEFTARG = _ps_trace.tag_k,
    RIGHTARG = text,
    FUNCTION = _ps_trace.tag_maps_regex
);

CREATE OR REPLACE FUNCTION _ps_trace.tag_maps_not_regex(_key _ps_trace.tag_k, _pattern text)
RETURNS _ps_trace.tag_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT '{}'::_ps_trace.tag_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OPERATOR _ps_trace.!=~ (
    LEFTARG = _ps_trace.tag_k,
    RIGHTARG = text,
    FUNCTION = _ps_trace.tag_maps_not_regex
);

CREATE OR REPLACE FUNCTION _ps_trace.match(_attr_map _ps_trace.tag_map, _maps _ps_trace.tag_maps)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR _ps_trace.? (
    LEFTARG = _ps_trace.tag_map,
    RIGHTARG = _ps_trace.tag_maps,
    FUNCTION = _ps_trace.match
);

DO $do$
DECLARE
    _tpl1 text =
$sql$
CREATE OR REPLACE FUNCTION _ps_trace.tag_maps_%s_%s(_key _ps_trace.tag_k, _val %s)
RETURNS _ps_trace.tag_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::_ps_trace.tag_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
$sql$;
    _tpl2 text =
$sql$
CREATE OPERATOR _ps_trace.%s (
    LEFTARG = _ps_trace.tag_k,
    RIGHTARG = %s,
    FUNCTION = _ps_trace.tag_maps_%s_%s
);
$sql$;
    _sql record;
BEGIN
    FOR _sql IN
    (
        SELECT
            format(_tpl1, replace(t.type, ' ', '_'), f.name, t.type) as func,
            format(_tpl2, f.op, t.type, replace(t.type, ' ', '_'), f.name) as op
        FROM
        (
            VALUES
            ('text'),
            ('smallint'),
            ('int'),
            ('bigint'),
            ('bool'),
            ('real'),
            ('double precision'),
            ('numeric'),
            ('timestamptz'),
            ('timestamp'),
            ('time'),
            ('date')
        ) t(type)
        CROSS JOIN
        (
            VALUES
            ('equal', '=='),
            ('not_equal', '!==')
        ) f(name, op)
    )
    LOOP
        EXECUTE _sql.func;
        EXECUTE _sql.op;
    END LOOP;
END;
$do$;

DO $do$
DECLARE
    _tpl1 text =
$sql$
CREATE OR REPLACE FUNCTION _ps_trace.tag_maps_%s_%s(_key _ps_trace.tag_k, _val %s)
RETURNS _ps_trace.tag_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::_ps_trace.tag_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
$sql$;
    _tpl2 text =
$sql$
CREATE OPERATOR _ps_trace.%s (
    LEFTARG = _ps_trace.tag_k,
    RIGHTARG = %s,
    FUNCTION = _ps_trace.tag_maps_%s_%s
);
$sql$;
    _sql record;
BEGIN
    FOR _sql IN
    (
        SELECT
            format(_tpl1, replace(t.type, ' ', '_'), f.name, t.type) as func,
            format(_tpl2, f.op, t.type, replace(t.type, ' ', '_'), f.name) as op
        FROM
        (
            VALUES
            ('smallint'        ),
            ('int'             ),
            ('bigint'          ),
            ('bool'            ),
            ('real'            ),
            ('double precision'),
            ('numeric'         ),
            ('timestamptz'     ),
            ('timestamp'       ),
            ('time'            ),
            ('date'            )
        ) t(type)
        CROSS JOIN
        (
            VALUES
            ('less_than', '#<'),
            ('less_than_equal', '#<='),
            ('greater_than', '#>'),
            ('greater_than_equal', '#>=')
        ) f(name, op)
    )
    LOOP
        EXECUTE _sql.func;
        EXECUTE _sql.op;
    END LOOP;
END;
$do$;

