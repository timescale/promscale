
CREATE SCHEMA IF NOT EXISTS SCHEMA_TRACING;
GRANT USAGE ON SCHEMA SCHEMA_TRACING TO prom_reader;

CREATE DOMAIN SCHEMA_TRACING.trace_id uuid
NOT NULL
CHECK (value != '00000000-0000-0000-0000-000000000000');
GRANT USAGE ON DOMAIN SCHEMA_TRACING.trace_id TO prom_reader;

CREATE DOMAIN SCHEMA_TRACING.attribute_map jsonb
NOT NULL
DEFAULT '{}'::jsonb
CHECK (jsonb_typeof(value) = 'object');
GRANT USAGE ON DOMAIN SCHEMA_TRACING.attribute_map TO prom_reader;

CREATE DOMAIN SCHEMA_TRACING.attribute_matchers AS SCHEMA_TRACING.attribute_map[] NOT NULL;
GRANT USAGE ON DOMAIN SCHEMA_TRACING.attribute_matchers TO prom_reader;

CREATE DOMAIN SCHEMA_TRACING.attribute_type smallint NOT NULL;
GRANT USAGE ON DOMAIN SCHEMA_TRACING.attribute_type TO prom_reader;

CREATE TABLE SCHEMA_TRACING.attribute_key
(
    id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    attribute_type SCHEMA_TRACING.attribute_type NOT NULL,
    key text NOT NULL
);
CREATE UNIQUE INDEX ON SCHEMA_TRACING.attribute_key (key) INCLUDE (id, attribute_type);
GRANT SELECT ON TABLE SCHEMA_TRACING.attribute_key TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_TRACING.attribute_key TO prom_writer;
GRANT USAGE ON SEQUENCE SCHEMA_TRACING.attribute_key_id_seq TO prom_writer;

CREATE TABLE SCHEMA_TRACING.attribute
(
    id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
    attribute_type SCHEMA_TRACING.attribute_type NOT NULL,
    key_id bigint NOT NULL,
    key text NOT NULL,
    value jsonb,
    FOREIGN KEY (key) REFERENCES SCHEMA_TRACING.attribute_key (key) ON DELETE CASCADE,
    UNIQUE (key, value) INCLUDE (attribute_type)
)
PARTITION BY HASH (key);
GRANT SELECT ON TABLE SCHEMA_TRACING.attribute TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_TRACING.attribute TO prom_writer;
GRANT USAGE ON SEQUENCE SCHEMA_TRACING.attribute_id_seq TO prom_writer;

-- create the partitions of the attribute table
DO $block$
DECLARE
    _i bigint;
    _max bigint = 64;
BEGIN
    FOR _i IN 1.._max
    LOOP
        EXECUTE format($sql$
            CREATE TABLE SCHEMA_TRACING.attribute_%s PARTITION OF SCHEMA_TRACING.attribute FOR VALUES WITH (MODULUS %s, REMAINDER %s)
            $sql$, _i, _max, _i - 1);
        EXECUTE format($sql$
            ALTER TABLE SCHEMA_TRACING.attribute_%s ADD PRIMARY KEY (id)
            $sql$, _i);
        EXECUTE format($sql$
            GRANT SELECT ON TABLE SCHEMA_TRACING.attribute_%s TO prom_reader
            $sql$, _i);
        EXECUTE format($sql$
            GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_TRACING.attribute_%s TO prom_writer
            $sql$, _i);
    END LOOP;
END
$block$
;

CREATE TYPE SCHEMA_TRACING.span_kind AS ENUM
(
    'UNSPECIFIED',
    'INTERNAL',
    'SERVER',
    'CLIENT',
    'PRODUCER',
    'CONSUMER'
);
GRANT USAGE ON TYPE SCHEMA_TRACING.span_kind TO prom_reader;

CREATE TYPE SCHEMA_TRACING.status_code AS ENUM
(
    'UNSET',
    'OK',
    'ERROR'
);
GRANT USAGE ON TYPE SCHEMA_TRACING.status_code TO prom_reader;

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

CREATE TABLE IF NOT EXISTS SCHEMA_TRACING.instrumentation_library
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL,
    version text NOT NULL,
    schema_url_id BIGINT NOT NULL REFERENCES SCHEMA_TRACING.schema_url(id),
    UNIQUE(name, version, schema_url_id)
);
GRANT SELECT ON TABLE SCHEMA_TRACING.instrumentation_library TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_TRACING.instrumentation_library TO prom_writer;
GRANT USAGE ON SEQUENCE SCHEMA_TRACING.instrumentation_library_id_seq TO prom_writer;

CREATE TABLE IF NOT EXISTS SCHEMA_TRACING.trace
(
    id SCHEMA_TRACING.trace_id NOT NULL PRIMARY KEY,
    root_span_id bigint not null,
    span_count int not null default 0,
    span_time_range tstzrange NOT NULL default tstzrange('infinity', 'infinity', '()'),
    event_time_range tstzrange NOT NULL default tstzrange('infinity', 'infinity', '()') --should this be included?
--    span_tree jsonb
);
CREATE INDEX ON SCHEMA_TRACING.trace USING GIST (span_time_range) INCLUDE (id);
GRANT SELECT ON TABLE SCHEMA_TRACING.trace TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_TRACING.trace TO prom_writer;

CREATE TABLE IF NOT EXISTS SCHEMA_TRACING.span
(
    trace_id SCHEMA_TRACING.trace_id NOT NULL,
    span_id bigint NOT NULL,
    trace_state text,
    parent_span_id bigint NULL,
    name_id bigint NOT NULL,
    span_kind SCHEMA_TRACING.span_kind,
    start_time timestamptz NOT NULL,
    end_time timestamptz NOT NULL,
    span_attributes SCHEMA_TRACING.attribute_map,
    dropped_attributes_count int NOT NULL default 0,
    event_time tstzrange NOT NULL default tstzrange('infinity', 'infinity', '()'),
    dropped_events_count int NOT NULL default 0,
    dropped_link_count int NOT NULL default 0,
    status_code SCHEMA_TRACING.status_code,
    status_message text,
    instrumentation_library_id bigint,
    resource_attributes SCHEMA_TRACING.attribute_map,
    resource_dropped_attributes_count int NOT NULL default 0,
    resource_schema_url_id BIGINT NOT NULL,
    PRIMARY KEY (span_id, trace_id, start_time),
    CHECK (start_time <= end_time)
);
CREATE INDEX ON SCHEMA_TRACING.span USING BTREE (trace_id, parent_span_id);
CREATE INDEX ON SCHEMA_TRACING.span USING GIN (span_attributes jsonb_path_ops);
CREATE INDEX ON SCHEMA_TRACING.span USING GIN (resource_attributes jsonb_path_ops);
SELECT create_hypertable('SCHEMA_TRACING.span', 'start_time', partitioning_column=>'trace_id', number_partitions=>1);
GRANT SELECT ON TABLE SCHEMA_TRACING.span TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_TRACING.span TO prom_writer;

CREATE TABLE IF NOT EXISTS SCHEMA_TRACING.event
(
    time timestamptz NOT NULL,
    trace_id SCHEMA_TRACING.trace_id NOT NULL,
    span_id bigint NOT NULL,
    event_number smallint NOT NULL,
    name text NOT NULL CHECK (name != ''),
    attributes SCHEMA_TRACING.attribute_map,
    dropped_attributes_count int NOT NULL DEFAULT 0
);
CREATE INDEX ON SCHEMA_TRACING.event USING GIN (attributes jsonb_path_ops);
CREATE INDEX ON SCHEMA_TRACING.event USING BTREE (span_id, time) INCLUDE (trace_id);
SELECT create_hypertable('SCHEMA_TRACING.event', 'time', partitioning_column=>'trace_id', number_partitions=>1);
GRANT SELECT ON TABLE SCHEMA_TRACING.event TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_TRACING.event TO prom_writer;

CREATE TABLE IF NOT EXISTS SCHEMA_TRACING.link
(
    trace_id SCHEMA_TRACING.trace_id NOT NULL,
    span_id bigint NOT NULL,
    span_start_time timestamptz NOT NULL,
    span_name_id BIGINT NOT NULL REFERENCES SCHEMA_TRACING.span_name (id),
    linked_trace_id SCHEMA_TRACING.trace_id NOT NULL,
    linked_span_id bigint NOT NULL,
    trace_state text,
    attributes SCHEMA_TRACING.attribute_map,
    dropped_attributes_count int NOT NULL DEFAULT 0,
    event_number smallint NOT NULL
);
CREATE INDEX ON SCHEMA_TRACING.link USING BTREE (span_id, span_start_time) INCLUDE (trace_id);
CREATE INDEX ON SCHEMA_TRACING.link USING GIN (attributes jsonb_path_ops);
SELECT create_hypertable('SCHEMA_TRACING.link', 'span_start_time', partitioning_column=>'trace_id', number_partitions=>1);
GRANT SELECT ON TABLE SCHEMA_TRACING.link TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_TRACING.link TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.get_attributes_jsonpath(_key text, _qry jsonpath)
RETURNS SCHEMA_TRACING.attribute_matchers
AS $sql$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT '{}'::SCHEMA_TRACING.attribute_matchers
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.get_attributes_text_equal(_key text, _val text)
RETURNS SCHEMA_TRACING.attribute_matchers
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT '{}'::SCHEMA_TRACING.attribute_matchers
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.get_attributes_text_not_equal(_key text, _val text)
RETURNS SCHEMA_TRACING.attribute_matchers
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT '{}'::SCHEMA_TRACING.attribute_matchers
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.get_attributes_int_equal(_key text, _val int)
RETURNS SCHEMA_TRACING.attribute_matchers
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT '{}'::SCHEMA_TRACING.attribute_matchers
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.get_attributes_int_not_equal(_key text, _val int)
RETURNS SCHEMA_TRACING.attribute_matchers
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT '{}'::SCHEMA_TRACING.attribute_matchers
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.get_attributes_bool_equal(_key text, _val boolean)
RETURNS SCHEMA_TRACING.attribute_matchers
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT '{}'::SCHEMA_TRACING.attribute_matchers
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.get_attributes_bool_not_equal(_key text, _val boolean)
RETURNS SCHEMA_TRACING.attribute_matchers
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT '{}'::SCHEMA_TRACING.attribute_matchers
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.get_attributes_regex(_key text,  _pattern text)
RETURNS SCHEMA_TRACING.attribute_matchers
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT '{}'::SCHEMA_TRACING.attribute_matchers
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.get_attributes_not_regex(_key text,  _pattern text)
RETURNS SCHEMA_TRACING.attribute_matchers
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT '{}'::SCHEMA_TRACING.attribute_matchers
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.match_attribute_map(_attr_map SCHEMA_TRACING.attribute_map, _matchers SCHEMA_TRACING.attribute_matchers)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OPERATOR SCHEMA_TRACING.@? (
    LEFTARG = text,
    RIGHTARG = jsonpath,
    FUNCTION = SCHEMA_TRACING.get_attributes_jsonpath
);

CREATE OPERATOR SCHEMA_TRACING.== (
    LEFTARG = text,
    RIGHTARG = text,
    FUNCTION = SCHEMA_TRACING.get_attributes_text_equal
);

CREATE OPERATOR SCHEMA_TRACING.!== (
    LEFTARG = text,
    RIGHTARG = text,
    FUNCTION = SCHEMA_TRACING.get_attributes_text_not_equal
);

CREATE OPERATOR SCHEMA_TRACING.== (
    LEFTARG = text,
    RIGHTARG = int,
    FUNCTION = SCHEMA_TRACING.get_attributes_int_equal
);

CREATE OPERATOR SCHEMA_TRACING.!== (
    LEFTARG = text,
    RIGHTARG = int,
    FUNCTION = SCHEMA_TRACING.get_attributes_int_not_equal
);

CREATE OPERATOR SCHEMA_TRACING.== (
    LEFTARG = text,
    RIGHTARG = boolean,
    FUNCTION = SCHEMA_TRACING.get_attributes_bool_equal
);

CREATE OPERATOR SCHEMA_TRACING.!== (
    LEFTARG = text,
    RIGHTARG = boolean,
    FUNCTION = SCHEMA_TRACING.get_attributes_bool_not_equal
);

CREATE OPERATOR SCHEMA_TRACING.==~ (
    LEFTARG = text,
    RIGHTARG = text,
    FUNCTION = SCHEMA_TRACING.get_attributes_regex
);

CREATE OPERATOR SCHEMA_TRACING.!=~ (
    LEFTARG = text,
    RIGHTARG = text,
    FUNCTION = SCHEMA_TRACING.get_attributes_not_regex
);

CREATE OPERATOR SCHEMA_TRACING.? (
    LEFTARG = SCHEMA_TRACING.attribute_map,
    RIGHTARG = SCHEMA_TRACING.attribute_matchers,
    FUNCTION = SCHEMA_TRACING.match_attribute_map
);
