
CREATE SCHEMA IF NOT EXISTS prom_trace;
GRANT USAGE ON SCHEMA prom_trace TO prom_reader;

CREATE DOMAIN prom_trace.trace_id uuid
NOT NULL
CHECK (value != '00000000-0000-0000-0000-000000000000');
GRANT USAGE ON DOMAIN prom_trace.trace_id TO prom_reader;

CREATE DOMAIN prom_trace.attribute_map jsonb
NOT NULL
DEFAULT '{}'::jsonb
CHECK (jsonb_typeof(value) = 'object');
GRANT USAGE ON DOMAIN prom_trace.attribute_map TO prom_reader;

CREATE DOMAIN prom_trace.attribute_maps AS prom_trace.attribute_map[] NOT NULL;
GRANT USAGE ON DOMAIN prom_trace.attribute_maps TO prom_reader;

CREATE DOMAIN prom_trace.attribute_type smallint NOT NULL;
GRANT USAGE ON DOMAIN prom_trace.attribute_type TO prom_reader;

CREATE TABLE prom_trace.attribute_key
(
    id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    attribute_type prom_trace.attribute_type NOT NULL,
    key text NOT NULL
);
CREATE UNIQUE INDEX ON prom_trace.attribute_key (key) INCLUDE (id, attribute_type);
GRANT SELECT ON TABLE prom_trace.attribute_key TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE prom_trace.attribute_key TO prom_writer;
GRANT USAGE ON SEQUENCE prom_trace.attribute_key_id_seq TO prom_writer;

CREATE TABLE prom_trace.attribute
(
    id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
    attribute_type prom_trace.attribute_type NOT NULL,
    key_id bigint NOT NULL,
    key text NOT NULL,
    value jsonb,
    FOREIGN KEY (key) REFERENCES prom_trace.attribute_key (key) ON DELETE CASCADE,
    UNIQUE (key, value) INCLUDE (id, key_id)
)
PARTITION BY HASH (key);
GRANT SELECT ON TABLE prom_trace.attribute TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE prom_trace.attribute TO prom_writer;
GRANT USAGE ON SEQUENCE prom_trace.attribute_id_seq TO prom_writer;

-- create the partitions of the attribute table
DO $block$
DECLARE
    _i bigint;
    _max bigint = 64;
BEGIN
    FOR _i IN 1.._max
    LOOP
        EXECUTE format($sql$
            CREATE TABLE prom_trace.attribute_%s PARTITION OF prom_trace.attribute FOR VALUES WITH (MODULUS %s, REMAINDER %s)
            $sql$, _i, _max, _i - 1);
        EXECUTE format($sql$
            ALTER TABLE prom_trace.attribute_%s ADD PRIMARY KEY (id)
            $sql$, _i);
        EXECUTE format($sql$
            GRANT SELECT ON TABLE prom_trace.attribute_%s TO prom_reader
            $sql$, _i);
        EXECUTE format($sql$
            GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE prom_trace.attribute_%s TO prom_writer
            $sql$, _i);
    END LOOP;
END
$block$
;

CREATE TYPE prom_trace.span_kind AS ENUM
(
    'UNSPECIFIED',
    'INTERNAL',
    'SERVER',
    'CLIENT',
    'PRODUCER',
    'CONSUMER'
);
GRANT USAGE ON TYPE prom_trace.span_kind TO prom_reader;

CREATE TYPE prom_trace.status_code AS ENUM
(
    'UNSET',
    'OK',
    'ERROR'
);
GRANT USAGE ON TYPE prom_trace.status_code TO prom_reader;

CREATE TABLE IF NOT EXISTS prom_trace.span_name
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL CHECK (name != '') UNIQUE
);
GRANT SELECT ON TABLE prom_trace.span_name TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE prom_trace.span_name TO prom_writer;
GRANT USAGE ON SEQUENCE prom_trace.span_name_id_seq TO prom_writer;

CREATE TABLE IF NOT EXISTS prom_trace.schema_url
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url text NOT NULL CHECK (url != '') UNIQUE
);
GRANT SELECT ON TABLE prom_trace.schema_url TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE prom_trace.schema_url TO prom_writer;
GRANT USAGE ON SEQUENCE prom_trace.schema_url_id_seq TO prom_writer;

CREATE TABLE IF NOT EXISTS prom_trace.instrumentation_library
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL,
    version text NOT NULL,
    schema_url_id BIGINT NOT NULL REFERENCES prom_trace.schema_url(id),
    UNIQUE(name, version, schema_url_id)
);
GRANT SELECT ON TABLE prom_trace.instrumentation_library TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE prom_trace.instrumentation_library TO prom_writer;
GRANT USAGE ON SEQUENCE prom_trace.instrumentation_library_id_seq TO prom_writer;

CREATE TABLE IF NOT EXISTS prom_trace.trace
(
    id prom_trace.trace_id NOT NULL PRIMARY KEY,
    root_span_id bigint not null,
    span_count int not null default 0,
    span_time_range tstzrange NOT NULL default tstzrange('infinity', 'infinity', '()'),
    event_time_range tstzrange NOT NULL default tstzrange('infinity', 'infinity', '()') --should this be included?
--    span_tree jsonb
);
CREATE INDEX ON prom_trace.trace USING GIST (span_time_range) INCLUDE (id);
GRANT SELECT ON TABLE prom_trace.trace TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE prom_trace.trace TO prom_writer;

CREATE TABLE IF NOT EXISTS prom_trace.span
(
    trace_id prom_trace.trace_id NOT NULL,
    span_id bigint NOT NULL,
    trace_state text,
    parent_span_id bigint NULL,
    name_id bigint NOT NULL,
    span_kind prom_trace.span_kind,
    start_time timestamptz NOT NULL,
    end_time timestamptz NOT NULL,
    span_attributes prom_trace.attribute_map,
    dropped_attributes_count int NOT NULL default 0,
    event_time tstzrange NOT NULL default tstzrange('infinity', 'infinity', '()'),
    dropped_events_count int NOT NULL default 0,
    dropped_link_count int NOT NULL default 0,
    status_code prom_trace.status_code,
    status_message text,
    instrumentation_library_id bigint,
    resource_attributes prom_trace.attribute_map,
    resource_dropped_attributes_count int NOT NULL default 0,
    resource_schema_url_id BIGINT NOT NULL,
    PRIMARY KEY (span_id, trace_id, start_time),
    CHECK (start_time <= end_time)
);
CREATE INDEX ON prom_trace.span USING BTREE (trace_id, parent_span_id);
CREATE INDEX ON prom_trace.span USING GIN (span_attributes jsonb_path_ops);
CREATE INDEX ON prom_trace.span USING GIN (resource_attributes jsonb_path_ops);
SELECT create_hypertable('prom_trace.span', 'start_time', partitioning_column=>'trace_id', number_partitions=>1);
GRANT SELECT ON TABLE prom_trace.span TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE prom_trace.span TO prom_writer;

CREATE TABLE IF NOT EXISTS prom_trace.event
(
    time timestamptz NOT NULL,
    trace_id prom_trace.trace_id NOT NULL,
    span_id bigint NOT NULL,
    event_number smallint NOT NULL,
    name text NOT NULL CHECK (name != ''),
    attributes prom_trace.attribute_map,
    dropped_attributes_count int NOT NULL DEFAULT 0
);
CREATE INDEX ON prom_trace.event USING GIN (attributes jsonb_path_ops);
CREATE INDEX ON prom_trace.event USING BTREE (span_id, time) INCLUDE (trace_id);
SELECT create_hypertable('prom_trace.event', 'time', partitioning_column=>'trace_id', number_partitions=>1);
GRANT SELECT ON TABLE prom_trace.event TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE prom_trace.event TO prom_writer;

CREATE TABLE IF NOT EXISTS prom_trace.link
(
    trace_id prom_trace.trace_id NOT NULL,
    span_id bigint NOT NULL,
    span_start_time timestamptz NOT NULL,
    span_name_id BIGINT NOT NULL REFERENCES prom_trace.span_name (id),
    linked_trace_id prom_trace.trace_id NOT NULL,
    linked_span_id bigint NOT NULL,
    trace_state text,
    attributes prom_trace.attribute_map,
    dropped_attributes_count int NOT NULL DEFAULT 0,
    event_number smallint NOT NULL
);
CREATE INDEX ON prom_trace.link USING BTREE (span_id, span_start_time) INCLUDE (trace_id);
CREATE INDEX ON prom_trace.link USING GIN (attributes jsonb_path_ops);
SELECT create_hypertable('prom_trace.link', 'span_start_time', partitioning_column=>'trace_id', number_partitions=>1);
GRANT SELECT ON TABLE prom_trace.link TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE prom_trace.link TO prom_writer;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_jsonpath(_key text, _qry jsonpath)
RETURNS prom_trace.attribute_maps
AS $sql$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::prom_trace.attribute_maps
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_text_equal(_key text, _val text)
RETURNS prom_trace.attribute_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::prom_trace.attribute_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_text_not_equal(_key text, _val text)
RETURNS prom_trace.attribute_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::prom_trace.attribute_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_int_equal(_key text, _val int)
RETURNS prom_trace.attribute_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::prom_trace.attribute_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_int_not_equal(_key text, _val int)
RETURNS prom_trace.attribute_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::prom_trace.attribute_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_bigint_equal(_key text, _val bigint)
RETURNS prom_trace.attribute_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::prom_trace.attribute_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_bigint_not_equal(_key text, _val bigint)
RETURNS prom_trace.attribute_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::prom_trace.attribute_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_numeric_equal(_key text, _val numeric)
RETURNS prom_trace.attribute_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::prom_trace.attribute_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_numeric_not_equal(_key text, _val numeric)
RETURNS prom_trace.attribute_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::prom_trace.attribute_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_real_equal(_key text, _val real)
RETURNS prom_trace.attribute_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::prom_trace.attribute_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_real_not_equal(_key text, _val real)
RETURNS prom_trace.attribute_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::prom_trace.attribute_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_double_equal(_key text, _val double precision)
RETURNS prom_trace.attribute_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::prom_trace.attribute_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_double_not_equal(_key text, _val double precision)
RETURNS prom_trace.attribute_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::prom_trace.attribute_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_bool_equal(_key text, _val bool)
RETURNS prom_trace.attribute_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::prom_trace.attribute_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_bool_not_equal(_key text, _val bool)
RETURNS prom_trace.attribute_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::prom_trace.attribute_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_timestamptz_equal(_key text, _val timestamptz)
RETURNS prom_trace.attribute_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::prom_trace.attribute_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_timestamptz_not_equal(_key text, _val timestamptz)
RETURNS prom_trace.attribute_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::prom_trace.attribute_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_time_equal(_key text, _val time)
RETURNS prom_trace.attribute_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::prom_trace.attribute_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_time_not_equal(_key text, _val time)
RETURNS prom_trace.attribute_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::prom_trace.attribute_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_date_equal(_key text, _val date)
RETURNS prom_trace.attribute_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::prom_trace.attribute_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_date_not_equal(_key text, _val date)
RETURNS prom_trace.attribute_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::prom_trace.attribute_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_regex(_key text,  _pattern text)
RETURNS prom_trace.attribute_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT '{}'::prom_trace.attribute_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.attribute_maps_not_regex(_key text,  _pattern text)
RETURNS prom_trace.attribute_maps
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT '{}'::prom_trace.attribute_maps
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION prom_trace.match(_attr_map prom_trace.attribute_map, _matchers prom_trace.attribute_maps)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OPERATOR prom_trace.@? (
    LEFTARG = text,
    RIGHTARG = jsonpath,
    FUNCTION = prom_trace.attribute_maps_jsonpath
);

CREATE OPERATOR prom_trace.== (
    LEFTARG = text,
    RIGHTARG = text,
    FUNCTION = prom_trace.attribute_maps_text_equal
);

CREATE OPERATOR prom_trace.!== (
    LEFTARG = text,
    RIGHTARG = text,
    FUNCTION = prom_trace.attribute_maps_text_not_equal
);

CREATE OPERATOR prom_trace.== (
    LEFTARG = text,
    RIGHTARG = int,
    FUNCTION = prom_trace.attribute_maps_int_equal
);

CREATE OPERATOR prom_trace.!== (
    LEFTARG = text,
    RIGHTARG = int,
    FUNCTION = prom_trace.attribute_maps_int_not_equal
);

CREATE OPERATOR prom_trace.== (
    LEFTARG = text,
    RIGHTARG = bigint,
    FUNCTION = prom_trace.attribute_maps_bigint_equal
);

CREATE OPERATOR prom_trace.!== (
    LEFTARG = text,
    RIGHTARG = bigint,
    FUNCTION = prom_trace.attribute_maps_bigint_not_equal
);

CREATE OPERATOR prom_trace.== (
    LEFTARG = text,
    RIGHTARG = numeric,
    FUNCTION = prom_trace.attribute_maps_numeric_equal
);

CREATE OPERATOR prom_trace.!== (
    LEFTARG = text,
    RIGHTARG = numeric,
    FUNCTION = prom_trace.attribute_maps_numeric_not_equal
);

CREATE OPERATOR prom_trace.== (
    LEFTARG = text,
    RIGHTARG = real,
    FUNCTION = prom_trace.attribute_maps_real_equal
);

CREATE OPERATOR prom_trace.!== (
    LEFTARG = text,
    RIGHTARG = real,
    FUNCTION = prom_trace.attribute_maps_real_not_equal
);

CREATE OPERATOR prom_trace.== (
    LEFTARG = text,
    RIGHTARG = double precision,
    FUNCTION = prom_trace.attribute_maps_double_equal
);

CREATE OPERATOR prom_trace.!== (
    LEFTARG = text,
    RIGHTARG = double precision,
    FUNCTION = prom_trace.attribute_maps_double_not_equal
);

CREATE OPERATOR prom_trace.== (
    LEFTARG = text,
    RIGHTARG = bool,
    FUNCTION = prom_trace.attribute_maps_bool_equal
);

CREATE OPERATOR prom_trace.!== (
    LEFTARG = text,
    RIGHTARG = bool,
    FUNCTION = prom_trace.attribute_maps_bool_not_equal
);

CREATE OPERATOR prom_trace.== (
    LEFTARG = text,
    RIGHTARG = timestamptz,
    FUNCTION = prom_trace.attribute_maps_timestamptz_equal
);

CREATE OPERATOR prom_trace.!== (
    LEFTARG = text,
    RIGHTARG = timestamptz,
    FUNCTION = prom_trace.attribute_maps_timestamptz_not_equal
);

CREATE OPERATOR prom_trace.== (
    LEFTARG = text,
    RIGHTARG = time,
    FUNCTION = prom_trace.attribute_maps_time_equal
);

CREATE OPERATOR prom_trace.!== (
    LEFTARG = text,
    RIGHTARG = time,
    FUNCTION = prom_trace.attribute_maps_time_not_equal
);

CREATE OPERATOR prom_trace.== (
    LEFTARG = text,
    RIGHTARG = date,
    FUNCTION = prom_trace.attribute_maps_date_equal
);

CREATE OPERATOR prom_trace.!== (
    LEFTARG = text,
    RIGHTARG = date,
    FUNCTION = prom_trace.attribute_maps_date_not_equal
);

CREATE OPERATOR prom_trace.==~ (
    LEFTARG = text,
    RIGHTARG = text,
    FUNCTION = prom_trace.attribute_maps_regex
);

CREATE OPERATOR prom_trace.!=~ (
    LEFTARG = text,
    RIGHTARG = text,
    FUNCTION = prom_trace.attribute_maps_not_regex
);

CREATE OPERATOR prom_trace.? (
    LEFTARG = prom_trace.attribute_map,
    RIGHTARG = prom_trace.attribute_maps,
    FUNCTION = prom_trace.match
);

