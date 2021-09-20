
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

CREATE DOMAIN _ps_trace.tag_type smallint NOT NULL;
GRANT USAGE ON DOMAIN _ps_trace.tag_type TO prom_reader;

CREATE TABLE _ps_trace.tag_key
(
    id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tag_type _ps_trace.tag_type NOT NULL,
    key _ps_trace.tag_k NOT NULL
);
CREATE UNIQUE INDEX ON _ps_trace.tag_key (key) INCLUDE (id, tag_type);
GRANT SELECT ON TABLE _ps_trace.tag_key TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _ps_trace.tag_key TO prom_writer;
GRANT USAGE ON SEQUENCE _ps_trace.tag_key_id_seq TO prom_writer;

CREATE TABLE _ps_trace.tag
(
    id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
    tag_type _ps_trace.tag_type NOT NULL,
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

CREATE TABLE IF NOT EXISTS _ps_trace.span
(
    trace_id _ps_trace.trace_id NOT NULL,
    span_id bigint NOT NULL,
    parent_span_id bigint NULL,
    name_id bigint NOT NULL,
    start_time timestamptz NOT NULL,
    end_time timestamptz NOT NULL,
    trace_state text,
    span_kind _ps_trace.span_kind,
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
CREATE INDEX ON _ps_trace.span USING BTREE (name_id);
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

CREATE OR REPLACE FUNCTION _ps_trace.match(_tag_map _ps_trace.tag_map, _maps _ps_trace.tag_maps)
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

CREATE OR REPLACE FUNCTION _ps_trace.span_tag_type()
RETURNS _ps_trace.tag_type
AS $sql$
    SELECT (1<<0)::smallint::_ps_trace.tag_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.span_tag_type() TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.resource_tag_type()
RETURNS _ps_trace.tag_type
AS $sql$
    SELECT (1<<1)::smallint::_ps_trace.tag_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.resource_tag_type() TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.event_tag_type()
RETURNS _ps_trace.tag_type
AS $sql$
    SELECT (1<<2)::smallint::_ps_trace.tag_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.event_tag_type() TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.link_tag_type()
RETURNS _ps_trace.tag_type
AS $sql$
    SELECT (1<<3)::smallint::_ps_trace.tag_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.link_tag_type() TO prom_reader;

INSERT INTO _ps_trace.tag_key (id, key, tag_type)
OVERRIDING SYSTEM VALUE
VALUES
    (1, 'service.name', _ps_trace.resource_tag_type()),
    (2, 'service.namespace', _ps_trace.resource_tag_type()),
    (3, 'service.instance.id', _ps_trace.resource_tag_type()),
    (4, 'service.version', _ps_trace.resource_tag_type()),
    (5, 'telemetry.sdk.name', _ps_trace.resource_tag_type()),
    (6, 'telemetry.sdk.language', _ps_trace.resource_tag_type()),
    (7, 'telemetry.sdk.version', _ps_trace.resource_tag_type()),
    (8, 'telemetry.auto.version', _ps_trace.resource_tag_type()),
    (9, 'container.name', _ps_trace.resource_tag_type()),
    (10, 'container.id', _ps_trace.resource_tag_type()),
    (11, 'container.runtime', _ps_trace.resource_tag_type()),
    (12, 'container.image.name', _ps_trace.resource_tag_type()),
    (13, 'container.image.tag', _ps_trace.resource_tag_type()),
    (14, 'faas.name', _ps_trace.resource_tag_type()),
    (15, 'faas.id', _ps_trace.resource_tag_type()),
    (16, 'faas.version', _ps_trace.resource_tag_type()),
    (17, 'faas.instance', _ps_trace.resource_tag_type()),
    (18, 'faas.max_memory', _ps_trace.resource_tag_type()),
    (19, 'process.pid', _ps_trace.resource_tag_type()),
    (20, 'process.executable.name', _ps_trace.resource_tag_type()),
    (21, 'process.executable.path', _ps_trace.resource_tag_type()),
    (22, 'process.command', _ps_trace.resource_tag_type()),
    (23, 'process.command_line', _ps_trace.resource_tag_type()),
    (24, 'process.command_args', _ps_trace.resource_tag_type()),
    (25, 'process.owner', _ps_trace.resource_tag_type()),
    (26, 'process.runtime.name', _ps_trace.resource_tag_type()),
    (27, 'process.runtime.version', _ps_trace.resource_tag_type()),
    (28, 'process.runtime.description', _ps_trace.resource_tag_type()),
    (29, 'webengine.name', _ps_trace.resource_tag_type()),
    (30, 'webengine.version', _ps_trace.resource_tag_type()),
    (31, 'webengine.description', _ps_trace.resource_tag_type()),
    (32, 'host.id', _ps_trace.resource_tag_type()),
    (33, 'host.name', _ps_trace.resource_tag_type()),
    (34, 'host.type', _ps_trace.resource_tag_type()),
    (35, 'host.arch', _ps_trace.resource_tag_type()),
    (36, 'host.image.name', _ps_trace.resource_tag_type()),
    (37, 'host.image.id', _ps_trace.resource_tag_type()),
    (38, 'host.image.version', _ps_trace.resource_tag_type()),
    (39, 'os.type', _ps_trace.resource_tag_type()),
    (40, 'os.description', _ps_trace.resource_tag_type()),
    (41, 'os.name', _ps_trace.resource_tag_type()),
    (42, 'os.version', _ps_trace.resource_tag_type()),
    (43, 'device.id', _ps_trace.resource_tag_type()),
    (44, 'device.model.identifier', _ps_trace.resource_tag_type()),
    (45, 'device.model.name', _ps_trace.resource_tag_type()),
    (46, 'cloud.provider', _ps_trace.resource_tag_type()),
    (47, 'cloud.account.id', _ps_trace.resource_tag_type()),
    (48, 'cloud.region', _ps_trace.resource_tag_type()),
    (49, 'cloud.availability_zone', _ps_trace.resource_tag_type()),
    (50, 'cloud.platform', _ps_trace.resource_tag_type()),
    (51, 'deployment.environment', _ps_trace.resource_tag_type()),
    (52, 'k8s.cluster', _ps_trace.resource_tag_type()),
    (53, 'k8s.node.name', _ps_trace.resource_tag_type()),
    (54, 'k8s.node.uid', _ps_trace.resource_tag_type()),
    (55, 'k8s.namespace.name', _ps_trace.resource_tag_type()),
    (56, 'k8s.pod.uid', _ps_trace.resource_tag_type()),
    (57, 'k8s.pod.name', _ps_trace.resource_tag_type()),
    (58, 'k8s.container.name', _ps_trace.resource_tag_type()),
    (59, 'k8s.replicaset.uid', _ps_trace.resource_tag_type()),
    (60, 'k8s.replicaset.name', _ps_trace.resource_tag_type()),
    (61, 'k8s.deployment.uid', _ps_trace.resource_tag_type()),
    (62, 'k8s.deployment.name', _ps_trace.resource_tag_type()),
    (63, 'k8s.statefulset.uid', _ps_trace.resource_tag_type()),
    (64, 'k8s.statefulset.name', _ps_trace.resource_tag_type()),
    (65, 'k8s.daemonset.uid', _ps_trace.resource_tag_type()),
    (66, 'k8s.daemonset.name', _ps_trace.resource_tag_type()),
    (67, 'k8s.job.uid', _ps_trace.resource_tag_type()),
    (68, 'k8s.job.name', _ps_trace.resource_tag_type()),
    (69, 'k8s.cronjob.uid', _ps_trace.resource_tag_type()),
    (70, 'k8s.cronjob.name', _ps_trace.resource_tag_type()),
    (71, 'net.transport', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (72, 'net.peer.ip', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (73, 'net.peer.port', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (74, 'net.peer.name', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (75, 'net.host.ip', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (76, 'net.host.port', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (77, 'net.host.name', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (78, 'net.host.connection.type', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (79, 'net.host.connection.subtype', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (80, 'net.host.carrier.name', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (81, 'net.host.carrier.mcc', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (82, 'net.host.carrier.mnc', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (83, 'net.host.carrier.icc', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (84, 'peer.service', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (85, 'enduser.id', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (86, 'enduser.role', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (87, 'enduser.scope', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (88, 'thread.id', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (89, 'thread.name', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (90, 'code.function', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (91, 'code.namespace', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (92, 'code.filepath', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (93, 'code.lineno', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (94, 'http.method', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (95, 'http.url', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (96, 'http.target', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (97, 'http.host', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (98, 'http.scheme', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (99, 'http.status_code', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (100, 'http.flavor', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (101, 'http.user_agent', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (102, 'http.request_content_length', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (103, 'http.request_content_length_uncompressed', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (104, 'http.response_content_length', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (105, 'http.response_content_length_uncompressed', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (106, 'http.server_name', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (107, 'http.route', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (108, 'http.client_ip', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (109, 'db.system', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (110, 'db.connection_string', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (111, 'db.user', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (112, 'db.jdbc.driver_classname', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (113, 'db.mssql.instance_name', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (114, 'db.name', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (115, 'db.statement', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (116, 'db.operation', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (117, 'db.hbase.namespace', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (118, 'db.redis.database_index', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (119, 'db.mongodb.collection', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (120, 'db.sql.table', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (121, 'db.cassandra.keyspace', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (122, 'db.cassandra.page_size', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (123, 'db.cassandra.consistency_level', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (124, 'db.cassandra.table', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (125, 'db.cassandra.idempotence', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (126, 'db.cassandra.speculative_execution_count', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (127, 'db.cassandra.coordinator.id', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (128, 'db.cassandra.coordinator.dc', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (129, 'rpc.system', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (130, 'rpc.service', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (131, 'rpc.method', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (132, 'rpc.grpc.status_code', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (133, 'message.type', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (134, 'message.id', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (135, 'message.compressed_size', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (136, 'message.uncompressed_size', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (137, 'rpc.jsonrpc.version', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (138, 'rpc.jsonrpc.request_id', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (139, 'rpc.jsonrpc.error_code', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (140, 'rpc.jsonrpc.error_message', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (141, 'messaging.system', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (142, 'messaging.destination', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (143, 'messaging.destination_kind', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (144, 'messaging.temp_destination', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (145, 'messaging.protocol', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (146, 'messaging.url', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (147, 'messaging.message_id', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (148, 'messaging.conversation_id', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (149, 'messaging.message_payload_size_bytes', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (150, 'messaging.message_payload_compressed_size_bytes', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (151, 'messaging.operation', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (152, 'messaging.consumer_id', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (153, 'messaging.rabbitmq.routing_key', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (154, 'messaging.kafka.message_key', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (155, 'messaging.kafka.consumer_group', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (156, 'messaging.kafka.client_id', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (157, 'messaging.kafka.partition', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (158, 'messaging.kafka.tombstone', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (159, 'faas.trigger', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (160, 'faas.speculative_execution_count', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (161, 'faas.coldstart', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (162, 'faas.invoked_name', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (163, 'faas.invoked_provider', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (164, 'faas.invoked_region', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (165, 'faas.document.collection', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (166, 'faas.document.operation', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (167, 'faas.document.time', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (168, 'faas.document.name', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (169, 'faas.time', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (170, 'faas.cron', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (171, 'exception.type', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (172, 'exception.message', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (173, 'exception.stacktrace', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type()),
    (174, 'exception.escaped', _ps_trace.span_tag_type() | _ps_trace.event_tag_type() | _ps_trace.link_tag_type())
;
SELECT setval('_ps_trace.tag_key_id_seq', (SELECT max(id) from _ps_trace.tag_key));
