--NOTES
--This code assumes that table names can only be 63 chars long

DO $$
    BEGIN
        CREATE ROLE prom_reader;
    EXCEPTION WHEN duplicate_object THEN
        RAISE NOTICE 'role prom_reader already exists, skipping create';
        RETURN;
    END
$$;
DO $$
    BEGIN
        CREATE ROLE prom_writer;
    EXCEPTION WHEN duplicate_object THEN
        RAISE NOTICE 'role prom_writer already exists, skipping create';
        RETURN;
    END
$$;
GRANT prom_reader TO prom_writer;

CREATE SCHEMA IF NOT EXISTS SCHEMA_CATALOG; -- catalog tables + internal functions
GRANT USAGE ON SCHEMA SCHEMA_CATALOG TO prom_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_CATALOG TO prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_CATALOG GRANT SELECT ON TABLES TO prom_reader;
GRANT USAGE ON SCHEMA SCHEMA_CATALOG TO prom_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SCHEMA_CATALOG TO prom_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_CATALOG GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO prom_writer;

CREATE SCHEMA IF NOT EXISTS SCHEMA_PROM; -- public functions
GRANT USAGE ON SCHEMA SCHEMA_PROM TO prom_reader;

CREATE SCHEMA IF NOT EXISTS SCHEMA_EXT; -- optimized versions of functions created by the extension
GRANT USAGE ON SCHEMA SCHEMA_EXT TO prom_reader;

CREATE SCHEMA IF NOT EXISTS SCHEMA_SERIES; -- series views
GRANT USAGE ON SCHEMA SCHEMA_SERIES TO prom_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_SERIES TO prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_SERIES GRANT SELECT ON TABLES TO prom_reader;

CREATE SCHEMA IF NOT EXISTS SCHEMA_METRIC; -- metric views
GRANT USAGE ON SCHEMA SCHEMA_METRIC TO prom_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_METRIC TO prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_METRIC GRANT SELECT ON TABLES TO prom_reader;

CREATE SCHEMA IF NOT EXISTS SCHEMA_DATA;
GRANT USAGE ON SCHEMA SCHEMA_DATA TO prom_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_DATA TO prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA GRANT SELECT ON TABLES TO prom_reader;
GRANT USAGE ON SCHEMA SCHEMA_DATA TO prom_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SCHEMA_DATA TO prom_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO prom_writer;

CREATE SCHEMA IF NOT EXISTS SCHEMA_DATA_SERIES;
GRANT USAGE ON SCHEMA SCHEMA_DATA_SERIES TO prom_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_DATA_SERIES TO prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA_SERIES GRANT SELECT ON TABLES TO prom_reader;
GRANT USAGE ON SCHEMA SCHEMA_DATA_SERIES TO prom_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SCHEMA_DATA_SERIES TO prom_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA_SERIES GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO prom_writer;


CREATE SCHEMA IF NOT EXISTS SCHEMA_INFO;
GRANT USAGE ON SCHEMA SCHEMA_INFO TO prom_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_INFO TO prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_INFO GRANT SELECT ON TABLES TO prom_reader;

CREATE DOMAIN SCHEMA_PROM.label_array AS int[] NOT NULL;

-- the timescale_prometheus_extra extension contains optimized version of some
-- of our functions and operators. To ensure the correct version of the are
-- used, SCHEMA_EXT must be before all of our other schemas in the search path
DO $$
DECLARE
   new_path text;
BEGIN
   new_path := current_setting('search_path') || format(',%L,%L,%L,%L', 'SCHEMA_EXT', 'SCHEMA_PROM', 'SCHEMA_METRIC', 'SCHEMA_CATALOG');
   execute format('ALTER DATABASE %I SET search_path = %s', current_database(), new_path);
   execute format('SET search_path = %s', new_path);
END
$$;


-----------------------
-- Table definitions --
-----------------------

CREATE TABLE public.prom_installation_info (
    key TEXT PRIMARY KEY,
    value TEXT
);

INSERT INTO public.prom_installation_info(key, value) VALUES
    ('catalog schema',        'SCHEMA_CATALOG'),
    ('prometheus API schema', 'SCHEMA_PROM'),
    ('extension schema',      'SCHEMA_EXT'),
    ('series schema',         'SCHEMA_SERIES'),
    ('metric schema',         'SCHEMA_METRIC'),
    ('data schema',           'SCHEMA_DATA'),
    ('information schema',    'SCHEMA_INFO');


CREATE TABLE SCHEMA_CATALOG.series (
    id bigint NOT NULL,
    metric_id int NOT NULL,
    labels SCHEMA_PROM.label_array NOT NULL --labels are globally unique because of how partitions are defined
) PARTITION BY LIST(metric_id);
CREATE INDEX series_labels_id ON SCHEMA_CATALOG.series USING GIN (labels);
CREATE SEQUENCE SCHEMA_CATALOG.series_id;

CREATE TABLE SCHEMA_CATALOG.label (
    id serial CHECK (id > 0),
    key TEXT,
    value text,
    PRIMARY KEY (id) INCLUDE (key, value),
    UNIQUE (key, value) INCLUDE (id)
);

--This table creates a unique mapping
--between label keys and their column names across metrics.
--This is done for usability of column name, especially for
-- long keys that get cut off.
CREATE TABLE SCHEMA_CATALOG.label_key(
    id SERIAL,
    key TEXT,
    value_column_name NAME,
    id_column_name NAME,
    PRIMARY KEY (id),
    UNIQUE(key)
);

CREATE TABLE SCHEMA_CATALOG.label_key_position (
    metric_name text, --references metric.metric_name NOT metric.id for performance reasons
    key TEXT, --NOT label_key.id for performance reasons.
    pos int,
    UNIQUE (metric_name, key) INCLUDE (pos)
);

CREATE TABLE SCHEMA_CATALOG.metric (
    id SERIAL PRIMARY KEY,
    metric_name text NOT NULL,
    table_name name NOT NULL,
    creation_completed BOOLEAN NOT NULL DEFAULT false,
    default_chunk_interval BOOLEAN NOT NULL DEFAULT true,
    retention_period INTERVAL DEFAULT NULL, --NULL to use the default retention_period
    UNIQUE (metric_name) INCLUDE (table_name),
    UNIQUE(table_name)
);

CREATE TABLE SCHEMA_CATALOG.default (
    key TEXT PRIMARY KEY,
    value TEXT
);

INSERT INTO SCHEMA_CATALOG.default(key,value) VALUES
('chunk_interval', (INTERVAL '8 hours')::text),
('retention_period', (90 * INTERVAL '1 day')::text);



----------------------------------
-- Label selectors and matchers --
----------------------------------
-- This section contains a few functions that we're going to create not in the
-- idempotent section, but here. This is due to the fact that it is harder to
-- deal with idempotent operator creation etc. than other operations, and
-- because these functions are mostly relatively simple in their logic and less
-- likely to change. they are here as conveniences to the user for the most part
-- and do not contain signifcant logic of their own.

CREATE DOMAIN SCHEMA_PROM.matcher_positive AS int[] NOT NULL;
CREATE DOMAIN SCHEMA_PROM.matcher_negative AS int[] NOT NULL;
CREATE DOMAIN SCHEMA_PROM.label_key AS TEXT NOT NULL;
CREATE DOMAIN SCHEMA_PROM.pattern AS TEXT NOT NULL;

--wrapper around jsonb_each_text to give a better row_estimate
--for labels (10 not 100)
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_jsonb_each_text(js jsonb, OUT key text, OUT value text)
 RETURNS SETOF record
 LANGUAGE SQL
 IMMUTABLE PARALLEL SAFE STRICT ROWS 10
AS $function$ SELECT (jsonb_each_text(js)).* $function$;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.label_jsonb_each_text(jsonb) to prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.count_jsonb_keys(j jsonb)
RETURNS INT
AS $func$
    SELECT count(*)::int from (SELECT jsonb_object_keys(j)) v;
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.count_jsonb_keys(jsonb) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.matcher(labels jsonb)
RETURNS SCHEMA_PROM.matcher_positive
AS $func$
    SELECT ARRAY(
           SELECT coalesce(l.id, -1) -- -1 indicates no such label
           FROM label_jsonb_each_text(labels-'__name__') e
           LEFT JOIN SCHEMA_CATALOG.label l
               ON (l.key = e.key AND l.value = e.value)
        )::SCHEMA_PROM.matcher_positive
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
COMMENT ON FUNCTION SCHEMA_PROM.matcher(jsonb)
IS 'returns a matcher for the JSONB, __name__ is ignored. The matcher can be used to match against a label array using @> or ? operators';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.matcher(jsonb) TO prom_reader;


---------------- eq functions ------------------

CREATE OR REPLACE FUNCTION SCHEMA_PROM.eq(labels1 SCHEMA_PROM.label_array, labels2 SCHEMA_PROM.label_array)
RETURNS BOOLEAN
AS $func$
    --assumes labels have metric name in position 1 and have no duplicate entries
    SELECT array_length(labels1, 1) = array_length(labels2, 1) AND labels1 @> labels2[2:]
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
COMMENT ON FUNCTION SCHEMA_PROM.eq(SCHEMA_PROM.label_array, SCHEMA_PROM.label_array)
IS 'returns true if two label arrays are equal, ignoring the metric name';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.eq(SCHEMA_PROM.label_array, SCHEMA_PROM.label_array) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.eq(labels1 SCHEMA_PROM.label_array, matchers SCHEMA_PROM.matcher_positive)
RETURNS BOOLEAN
AS $func$
    --assumes no duplicate entries
     SELECT array_length(labels1, 1) = (array_length(matchers, 1) + 1)
            AND labels1 @> matchers
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
COMMENT ON FUNCTION SCHEMA_PROM.eq(SCHEMA_PROM.label_array, SCHEMA_PROM.matcher_positive)
IS 'returns true if the label array and matchers are equal, there should not be a matcher for the metric name';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.eq(SCHEMA_PROM.label_array, SCHEMA_PROM.matcher_positive) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.eq(labels SCHEMA_PROM.label_array, json_labels jsonb)
RETURNS BOOLEAN
AS $func$
    --assumes no duplicate entries
    --do not call eq(label_array, matchers) to allow inlining
     SELECT array_length(labels, 1) = (SCHEMA_CATALOG.count_jsonb_keys(json_labels-'__name__') + 1)
            AND labels @> SCHEMA_PROM.matcher(json_labels)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
COMMENT ON FUNCTION SCHEMA_PROM.eq(SCHEMA_PROM.label_array, jsonb)
IS 'returns true if the labels and jsonb are equal, ignoring the metric name';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.eq(SCHEMA_PROM.label_array, jsonb) TO prom_reader;

--------------------- op @> ------------------------

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_contains(labels SCHEMA_PROM.label_array, json_labels jsonb)
RETURNS BOOLEAN
AS $func$
    SELECT labels @> SCHEMA_PROM.matcher(json_labels)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.label_contains(SCHEMA_PROM.label_array, jsonb) TO prom_reader;

CREATE OPERATOR SCHEMA_PROM.@> (
    LEFTARG = SCHEMA_PROM.label_array,
    RIGHTARG = jsonb,
    FUNCTION = SCHEMA_CATALOG.label_contains
);

--------------------- op ? ------------------------

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_match(labels SCHEMA_PROM.label_array, matchers SCHEMA_PROM.matcher_positive)
RETURNS BOOLEAN
AS $func$
    SELECT labels && matchers
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.label_match(SCHEMA_PROM.label_array, SCHEMA_PROM.matcher_positive) TO prom_reader;

CREATE OPERATOR SCHEMA_PROM.? (
    LEFTARG = SCHEMA_PROM.label_array,
    RIGHTARG = SCHEMA_PROM.matcher_positive,
    FUNCTION = SCHEMA_CATALOG.label_match
);

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_match(labels SCHEMA_PROM.label_array, matchers SCHEMA_PROM.matcher_negative)
RETURNS BOOLEAN
AS $func$
    SELECT NOT (labels && matchers)
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.label_match(SCHEMA_PROM.label_array, SCHEMA_PROM.matcher_negative) TO prom_reader;

CREATE OPERATOR SCHEMA_PROM.? (
    LEFTARG = SCHEMA_PROM.label_array,
    RIGHTARG = SCHEMA_PROM.matcher_negative,
    FUNCTION = SCHEMA_CATALOG.label_match
);

--------------------- op == !== ==~ !=~ ------------------------

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_find_key_equal(key_to_match SCHEMA_PROM.label_key, pat SCHEMA_PROM.pattern)
RETURNS SCHEMA_PROM.matcher_positive
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::SCHEMA_PROM.matcher_positive
    FROM SCHEMA_CATALOG.label l
    WHERE l.key = key_to_match and l.value = pat
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.label_find_key_equal(SCHEMA_PROM.label_key, SCHEMA_PROM.pattern) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_find_key_not_equal(key_to_match SCHEMA_PROM.label_key, pat SCHEMA_PROM.pattern)
RETURNS SCHEMA_PROM.matcher_negative
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::SCHEMA_PROM.matcher_negative
    FROM SCHEMA_CATALOG.label l
    WHERE l.key = key_to_match and l.value = pat
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.label_find_key_not_equal(SCHEMA_PROM.label_key, SCHEMA_PROM.pattern) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_find_key_regex(key_to_match SCHEMA_PROM.label_key, pat SCHEMA_PROM.pattern)
RETURNS SCHEMA_PROM.matcher_positive
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::SCHEMA_PROM.matcher_positive
    FROM SCHEMA_CATALOG.label l
    WHERE l.key = key_to_match and l.value ~ pat
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.label_find_key_regex(SCHEMA_PROM.label_key, SCHEMA_PROM.pattern) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_find_key_not_regex(key_to_match SCHEMA_PROM.label_key, pat SCHEMA_PROM.pattern)
RETURNS SCHEMA_PROM.matcher_negative
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::SCHEMA_PROM.matcher_negative
    FROM SCHEMA_CATALOG.label l
    WHERE l.key = key_to_match and l.value ~ pat
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.label_find_key_not_regex(SCHEMA_PROM.label_key, SCHEMA_PROM.pattern) TO prom_reader;

CREATE OPERATOR SCHEMA_PROM.== (
    LEFTARG = SCHEMA_PROM.label_key,
    RIGHTARG = SCHEMA_PROM.pattern,
    FUNCTION = SCHEMA_CATALOG.label_find_key_equal
);

CREATE OPERATOR SCHEMA_PROM.!== (
    LEFTARG = SCHEMA_PROM.label_key,
    RIGHTARG = SCHEMA_PROM.pattern,
    FUNCTION = SCHEMA_CATALOG.label_find_key_not_equal
);

CREATE OPERATOR SCHEMA_PROM.==~ (
    LEFTARG = SCHEMA_PROM.label_key,
    RIGHTARG = SCHEMA_PROM.pattern,
    FUNCTION = SCHEMA_CATALOG.label_find_key_regex
);

CREATE OPERATOR SCHEMA_PROM.!=~ (
    LEFTARG = SCHEMA_PROM.label_key,
    RIGHTARG = SCHEMA_PROM.pattern,
    FUNCTION = SCHEMA_CATALOG.label_find_key_not_regex
);
