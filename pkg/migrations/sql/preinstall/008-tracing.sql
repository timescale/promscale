
CREATE DOMAIN trace_id uuid
NOT NULL
CHECK (value != '00000000-0000-0000-0000-000000000000')
;

CREATE DOMAIN attribute_map jsonb
NOT NULL
DEFAULT '{}'::jsonb
CHECK (jsonb_typeof(value) = 'object')
;

CREATE DOMAIN attribute_type smallint NOT NULL
;

CREATE TABLE attribute_key
(
    id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    attribute_type attribute_type NOT NULL,
    key text NOT NULL
);
CREATE UNIQUE INDEX ON attribute_key (key) INCLUDE (id, attribute_type);

CREATE TABLE attribute
(
    id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
    attribute_type attribute_type NOT NULL,
    key text NOT NULL,
    value jsonb,
    FOREIGN KEY (key) REFERENCES attribute_key (key) ON DELETE CASCADE,
    UNIQUE (key, value) INCLUDE (id, attribute_type)
)
PARTITION BY HASH (key)
;

DO $block$
DECLARE
    _i bigint;
    _max bigint = 64;
BEGIN
    FOR _i IN 1.._max
    LOOP
        EXECUTE format($sql$
            CREATE TABLE attribute_%s PARTITION OF attribute FOR VALUES WITH (MODULUS %s, REMAINDER %s)
            $sql$, _i, _max, _i - 1);
        EXECUTE format($sql$
            ALTER TABLE attribute_%s ADD PRIMARY KEY (id)
            $sql$, _i);
    END LOOP;
END
$block$
;

CREATE TYPE span_kind AS ENUM
(
    'UNSPECIFIED',
    'INTERNAL',
    'SERVER',
    'CLIENT',
    'PRODUCER',
    'CONSUMER'
);

CREATE TYPE status_code AS ENUM
(
    'UNSET',
    'OK',
    'ERROR'
);

CREATE TABLE IF NOT EXISTS span_name
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL CHECK (name != '') UNIQUE
);

CREATE TABLE IF NOT EXISTS schema_url
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url text NOT NULL CHECK (url != '') UNIQUE
);

CREATE TABLE IF NOT EXISTS instrumentation_library
(
    id bigint NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL,
    version text NOT NULL,
    schema_url_id BIGINT NOT NULL REFERENCES schema_url(id),
    UNIQUE(name, version, schema_url_id)
);

CREATE TABLE IF NOT EXISTS trace
(
    id trace_id NOT NULL PRIMARY KEY,
    span_time_range tstzrange NOT NULL,
    event_time_range tstzrange NOT NULL default tstzrange('infinity', 'infinity', '()') --should this be included?
    --graph representation? --
);

CREATE TABLE IF NOT EXISTS span
(
    trace_id trace_id NOT NULL REFERENCES trace(id),
    span_id bigint NOT NULL,
    trace_state text,
    parent_span_id bigint NULL,
    name_id bigint NOT NULL REFERENCES span_name (id),
    span_kind span_kind,
    start_time timestamptz NOT NULL,
    end_time timestamptz NOT NULL,
    span_attributes attribute_map,
    dropped_attributes_count int NOT NULL default 0,
    event_time tstzrange NOT NULL default tstzrange('infinity', 'infinity', '()'),
    dropped_events_count int NOT NULL default 0,
    dropped_link_count int NOT NULL default 0,
    status_code status_code,
    status_message text,
    instrumentation_library_id bigint REFERENCES instrumentation_library (id),
    resource_attributes attribute_map,
    resource_dropped_attributes_count int NOT NULL default 0,
    resource_schema_url_id BIGINT NOT NULL REFERENCES schema_url(id),
    PRIMARY KEY (span_id, trace_id),
    CHECK (start_time <= end_time)
);
CREATE INDEX ON span USING GIST (tstzrange(start_time, end_time, '[]'));
CREATE INDEX ON span USING GIN (span_attributes jsonb_path_ops);
CREATE INDEX ON span USING GIN (resource_attributes jsonb_path_ops);

CREATE TABLE IF NOT EXISTS event
(
    time timestamptz NOT NULL,
    trace_id trace_id NOT NULL,
    span_id bigint NOT NULL,
    event_number smallint NOT NULL,
    name text NOT NULL CHECK (name != ''),
    attributes attribute_map,
    dropped_attributes_count int NOT NULL DEFAULT 0,
    FOREIGN KEY (span_id, trace_id) REFERENCES span (span_id, trace_id) ON DELETE CASCADE
);
CREATE INDEX ON event USING GIN (attributes jsonb_path_ops);
CREATE INDEX ON event USING BTREE (span_id, time);

CREATE TABLE IF NOT EXISTS link
(
    trace_id trace_id NOT NULL,
    span_id bigint NOT NULL,
    span_start_time timestamptz NOT NULL,
    span_name_id BIGINT NOT NULL REFERENCES span_name (id),
    linked_trace_id trace_id NOT NULL,
    linked_span_id bigint NOT NULL,
    trace_state text,
    attributes attribute_map,
    dropped_attributes_count int NOT NULL DEFAULT 0,
    event_number smallint NOT NULL,
    FOREIGN KEY (span_id, trace_id) REFERENCES span (span_id, trace_id) ON DELETE CASCADE
);
CREATE INDEX ON link USING GIN (attributes jsonb_path_ops);
