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
    id bigserial,
    metric_id int,
    labels SCHEMA_PROM.label_array --labels are globally unique because of how partitions are defined
) PARTITION BY LIST(metric_id);
CREATE INDEX series_labels_id ON SCHEMA_CATALOG.series USING GIN (labels);

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


CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_default_chunk_interval()
    RETURNS INTERVAL
AS $func$
    SELECT value::INTERVAL FROM SCHEMA_CATALOG.default WHERE key='chunk_interval';
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_default_chunk_interval() TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_default_retention_period()
    RETURNS INTERVAL
AS $func$
    SELECT value::INTERVAL FROM SCHEMA_CATALOG.default WHERE key='retention_period';
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_default_retention_period() TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.make_metric_table()
    RETURNS trigger
    AS $func$
DECLARE
  label_id INT;
BEGIN
   EXECUTE format('CREATE TABLE SCHEMA_DATA.%I(time TIMESTAMPTZ, value DOUBLE PRECISION, series_id INT)',
                    NEW.table_name);
   EXECUTE format('CREATE INDEX ON SCHEMA_DATA.%I (series_id, time) INCLUDE (value)',
                    NEW.table_name);
   PERFORM create_hypertable(format('SCHEMA_DATA.%I', NEW.table_name), 'time',
                             chunk_time_interval=>SCHEMA_CATALOG.get_default_chunk_interval());
   EXECUTE format($$
     ALTER TABLE SCHEMA_DATA.%I SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'series_id',
        timescaledb.compress_orderby = 'time'
    ); $$, NEW.table_name);

    SELECT SCHEMA_CATALOG.get_or_create_label_id('__name__', NEW.metric_name)
    INTO STRICT label_id;


    -- stupidly create table ... partition of X first takes an access share lock
    -- and then an access exclusive lock. Such an upgrade can, and does, cause
    -- deadlocks. Taking the stronger lock beforehand prevents such deadlocks.
    -- We only need the lock on the parent.
    LOCK TABLE ONLY SCHEMA_CATALOG.series in ACCESS EXCLUSIVE mode;

    --note that because labels[1] is unique across partitions and UNIQUE(labels) inside partition, labels are guaranteed globally unique
    EXECUTE format($$
        CREATE TABLE SCHEMA_DATA_SERIES.%1$I PARTITION OF SCHEMA_CATALOG.series (
            CHECK(labels[1] = %2$L AND labels[1] IS NOT NULL),
            CHECK(metric_id = %3$L),
            UNIQUE(labels) INCLUDE (id),
            PRIMARY KEY(id)
        ) FOR VALUES IN (%3$L)
    $$, NEW.table_name, label_id, NEW.id);

   --chunks where the end time is before now()-10 minutes will be compressed
   PERFORM add_compress_chunks_policy(format('SCHEMA_DATA.%I', NEW.table_name), INTERVAL '10 minutes');
   RETURN NEW;
END
$func$
LANGUAGE PLPGSQL VOLATILE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.make_metric_table() TO prom_writer;

CREATE TRIGGER make_metric_table_trigger
    AFTER INSERT ON SCHEMA_CATALOG.metric
    FOR EACH ROW
    EXECUTE PROCEDURE SCHEMA_CATALOG.make_metric_table();




------------------------
-- Internal functions --
------------------------

-- Return a table name built from a full_name and a suffix.
-- The full name is truncated so that the suffix could fit in full.
-- name size will always be exactly 63 chars.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.pg_name_with_suffix(
        full_name text, suffix text)
    RETURNS name
AS $func$
    SELECT (substring(full_name for 63-(char_length(suffix)+1)) || '_' || suffix)::name
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.pg_name_with_suffix(text, text) TO prom_reader;

-- Return a new unique name from a name and id.
-- This tries to use the full_name in full. But if the
-- full name doesn't fit, generates a new unique name.
-- Note that there cannot be a collision betweeen a user
-- defined name and a name with a suffix because user
-- defined names of length 63 always get a suffix and
-- conversely, all names with a suffix are length 63.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.pg_name_unique(
        full_name_arg text, suffix text)
    RETURNS name
AS $func$
    SELECT CASE
        WHEN char_length(full_name_arg) < 63 THEN
            full_name_arg::name
        ELSE
            SCHEMA_CATALOG.pg_name_with_suffix(
                full_name_arg, suffix
            )
        END
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.pg_name_unique(text, text) TO prom_reader;

--Creates a new table for a given metric name.
--This uses up some sequences so should only be called
--If the table does not yet exist.
--The function inserts into the metric catalog table,
--  which causes the make_metric_table trigger to fire,
--  which actually creates the table
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.create_metric_table(
        metric_name_arg text, OUT id int, OUT table_name name)
AS $func$
DECLARE
  new_id int;
BEGIN
new_id = nextval(pg_get_serial_sequence('SCHEMA_CATALOG.metric','id'))::int;
LOOP
    INSERT INTO SCHEMA_CATALOG.metric (id, metric_name, table_name)
        SELECT  new_id,
                metric_name_arg,
                SCHEMA_CATALOG.pg_name_unique(metric_name_arg, new_id::text)
    ON CONFLICT DO NOTHING
    RETURNING SCHEMA_CATALOG.metric.id, SCHEMA_CATALOG.metric.table_name
    INTO id, table_name;
    -- under high concurrency the insert may not return anything, so try a select and loop
    -- https://stackoverflow.com/a/15950324
    EXIT WHEN FOUND;

    SELECT m.id, m.table_name
    INTO id, table_name
    FROM SCHEMA_CATALOG.metric m
    WHERE metric_name = metric_name_arg;

    EXIT WHEN FOUND;
END LOOP;
END
$func$
LANGUAGE PLPGSQL VOLATILE ;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.create_metric_table(text) TO prom_writer;

--Creates a new label_key row for a given key.
--This uses up some sequences so should only be called
--If the table does not yet exist.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.create_label_key(
        new_key TEXT, OUT id INT, OUT value_column_name NAME, OUT id_column_name NAME
)
AS $func$
DECLARE
  new_id int;
BEGIN
new_id = nextval(pg_get_serial_sequence('SCHEMA_CATALOG.label_key','id'))::int;
LOOP
    INSERT INTO SCHEMA_CATALOG.label_key (id, key, value_column_name, id_column_name)
        SELECT  new_id,
                new_key,
                SCHEMA_CATALOG.pg_name_unique(new_key, new_id::text),
                SCHEMA_CATALOG.pg_name_unique(new_key || '_id', format('%s_id', new_id))
    ON CONFLICT DO NOTHING
    RETURNING SCHEMA_CATALOG.label_key.id, SCHEMA_CATALOG.label_key.value_column_name, SCHEMA_CATALOG.label_key.id_column_name
    INTO id, value_column_name, id_column_name;
    -- under high concurrency the insert may not return anything, so try a select and loop
    -- https://stackoverflow.com/a/15950324
    EXIT WHEN FOUND;

    SELECT lk.id, lk.value_column_name, lk.id_column_name
    INTO id, value_column_name, id_column_name
    FROM SCHEMA_CATALOG.label_key lk
    WHERE key = new_key;

    EXIT WHEN FOUND;
END LOOP;
END
$func$
LANGUAGE PLPGSQL VOLATILE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.create_label_key(TEXT) TO prom_writer;

--Get a label key row if one doesn't yet exist.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_or_create_label_key(
        key TEXT, OUT id INT, OUT value_column_name NAME, OUT id_column_name NAME)
AS $func$
   SELECT id, value_column_name, id_column_name
   FROM SCHEMA_CATALOG.label_key lk
   WHERE lk.key = get_or_create_label_key.key
   UNION ALL
   SELECT *
   FROM SCHEMA_CATALOG.create_label_key(get_or_create_label_key.key)
   LIMIT 1
$func$
LANGUAGE SQL VOLATILE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_or_create_label_key(TEXT) to prom_writer;

-- Get a new label array position for a label key. For any metric,
-- we want the positions to be as compact as possible.
-- This uses some pretty heavy locks so use sparingly.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_new_pos_for_key(
        metric_name text, key_name text)
    RETURNS int
AS $func$
DECLARE
    position int;
    next_position int;
    metric_table NAME;
BEGIN
    --use double check locking here
    --fist optimistic check:
    SELECT
        pos
    FROM
        SCHEMA_CATALOG.label_key_position lkp
    WHERE
        lkp.metric_name = get_new_pos_for_key.metric_name
        AND lkp.key = get_new_pos_for_key.key_name
    INTO position;

    IF FOUND THEN
        RETURN position;
    END IF;

    SELECT table_name
    FROM SCHEMA_CATALOG.get_or_create_metric_table_name(get_new_pos_for_key.metric_name)
    INTO metric_table;
    --lock as for ALTER TABLE because we are in effect changing the schema here
    --also makes sure the next_position below is correct in terms of concurrency
    EXECUTE format('LOCK TABLE SCHEMA_DATA.%I IN SHARE UPDATE EXCLUSIVE MODE', metric_table);
    --second check after lock
    SELECT
        pos
    FROM
        SCHEMA_CATALOG.label_key_position lkp
    WHERE
        lkp.metric_name = get_new_pos_for_key.metric_name
        AND lkp.key =  get_new_pos_for_key.key_name INTO position;

    IF FOUND THEN
        RETURN position;
    END IF;

    IF key_name = '__name__' THEN
       next_position := 1; -- 1-indexed arrays, __name__ as first element
    ELSE
        SELECT
            max(pos) + 1
        FROM
            SCHEMA_CATALOG.label_key_position lkp
        WHERE
            lkp.metric_name = get_new_pos_for_key.metric_name INTO next_position;

        IF next_position IS NULL THEN
            next_position := 2; -- element 1 reserved for __name__
        END IF;
    END IF;

    PERFORM SCHEMA_CATALOG.get_or_create_label_key(key_name);

    INSERT INTO SCHEMA_CATALOG.label_key_position
        VALUES (metric_name, key_name, next_position)
    ON CONFLICT
        DO NOTHING
    RETURNING
        pos INTO position;

    IF NOT FOUND THEN
        RAISE 'Could not find a new position';
    END IF;

    PERFORM SCHEMA_CATALOG.create_series_view(metric_name);
    PERFORM SCHEMA_CATALOG.create_metric_view(metric_name);

    RETURN position;
END
$func$
LANGUAGE PLPGSQL VOLATILE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_new_pos_for_key(text, text) TO prom_writer;

--should only be called after a check that that the label doesn't exist
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_new_label_id(key_name text, value_name text, OUT id INT)
AS $func$
BEGIN
LOOP
    INSERT INTO
        SCHEMA_CATALOG.label(key, value)
    VALUES
        (key_name,value_name)
    ON CONFLICT DO NOTHING
    RETURNING SCHEMA_CATALOG.label.id
    INTO id;

    EXIT WHEN FOUND;

    SELECT
        l.id
    INTO id
    FROM SCHEMA_CATALOG.label l
    WHERE
        key = key_name AND
        value = value_name;

    EXIT WHEN FOUND;
END LOOP;
END
$func$
LANGUAGE PLPGSQL VOLATILE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_new_label_id(text, text) to prom_writer;

--wrapper around jsonb_each_text to give a better row_estimate
--for labels (10 not 100)
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_jsonb_each_text(js jsonb, OUT key text, OUT value text)
 RETURNS SETOF record
 LANGUAGE SQL
 IMMUTABLE PARALLEL SAFE STRICT ROWS 10
AS $function$ SELECT (jsonb_each_text(js)).* $function$;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.label_jsonb_each_text(jsonb) to prom_reader;

--wrapper around unnest to give better row estimate (10 not 100)
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_unnest(label_array anyarray)
 RETURNS SETOF anyelement
 LANGUAGE SQL
 IMMUTABLE PARALLEL SAFE STRICT ROWS 10
AS $function$ SELECT unnest(label_array) $function$;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.label_unnest(anyarray) to prom_reader;


---------------------------------------------------
------------------- Public APIs -------------------
---------------------------------------------------

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_metric_table_name_if_exists(
        metric_name text)
    RETURNS TABLE (id int, table_name name)
AS $func$
   SELECT id, table_name::name
   FROM SCHEMA_CATALOG.metric m
   WHERE m.metric_name = get_metric_table_name_if_exists.metric_name
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_metric_table_name_if_exists(text) to prom_reader;

-- Public function to get the name of the table for a given metric
-- This will create the metric table if it does not yet exist.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_or_create_metric_table_name(
        metric_name text, OUT id int, OUT table_name name)
AS $func$
   SELECT id, table_name::name
   FROM SCHEMA_CATALOG.metric m
   WHERE m.metric_name = get_or_create_metric_table_name.metric_name
   UNION ALL
   SELECT *
   FROM SCHEMA_CATALOG.create_metric_table(get_or_create_metric_table_name.metric_name)
   LIMIT 1
$func$
LANGUAGE SQL VOLATILE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_or_create_metric_table_name(text) to prom_writer;

--public function to get the array position for a label key
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_or_create_label_key_pos(
        metric_name text, key text)
    RETURNS INT
AS $$
    --only executes the more expensive PLPGSQL function if the label doesn't exist
    SELECT
        pos
    FROM
        SCHEMA_CATALOG.label_key_position lkp
    WHERE
        lkp.metric_name = get_or_create_label_key_pos.metric_name
        AND lkp.key = get_or_create_label_key_pos.key
    UNION ALL
    SELECT
        SCHEMA_CATALOG.get_new_pos_for_key(get_or_create_label_key_pos.metric_name, get_or_create_label_key_pos.key)
    LIMIT 1
$$
LANGUAGE SQL VOLATILE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_or_create_label_key_pos(text, text) to prom_writer;

--Get the label_id for a key, value pair
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_or_create_label_id(
        key_name text, value_name text)
    RETURNS INT
AS $$
    --first select to prevent sequence from being used up
    --unnecessarily
    SELECT
        id
    FROM SCHEMA_CATALOG.label
    WHERE
        key = key_name AND
        value = value_name
    UNION ALL
    SELECT
        SCHEMA_CATALOG.get_new_label_id(key_name, value_name)
    LIMIT 1
$$
LANGUAGE SQL VOLATILE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_or_create_label_id(text, text) to prom_writer;

--This generates a position based array from the jsonb
--0s represent keys that are not set (we don't use NULL
--since intarray does not support it).
--This is not super performance critical since this
--is only used on the insert client and is cached there.
CREATE OR REPLACE FUNCTION SCHEMA_PROM.label_array(js jsonb)
RETURNS SCHEMA_PROM.label_array AS $$
    WITH idx_val AS (
        SELECT
            -- only call the functions to create new key positions
            -- and label ids if they don't exist (for performance reasons)
            coalesce(lkp.pos,
              SCHEMA_CATALOG.get_or_create_label_key_pos(js->>'__name__', e.key)) idx,
            coalesce(l.id,
              SCHEMA_CATALOG.get_or_create_label_id(e.key, e.value)) val
        FROM label_jsonb_each_text(js) e
             LEFT JOIN SCHEMA_CATALOG.label l
               ON (l.key = e.key AND l.value = e.value)
            LEFT JOIN SCHEMA_CATALOG.label_key_position lkp
               ON
               (
                  lkp.metric_name = js->>'__name__' AND
                  lkp.key = e.key
               )
        --needs to order by key to prevent deadlocks if get_or_create_label_id is creating labels
        ORDER BY l.key
    )
    SELECT ARRAY(
        SELECT coalesce(idx_val.val, 0)
        FROM
            generate_series(
                    1,
                    (SELECT max(idx) FROM idx_val)
            ) g
            LEFT JOIN idx_val ON (idx_val.idx = g)
    )::SCHEMA_PROM.label_array
$$
LANGUAGE SQL VOLATILE;
COMMENT ON FUNCTION SCHEMA_PROM.label_array(jsonb)
IS 'converts a jsonb to a label array';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.label_array(jsonb) TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.label_array(metric_name TEXT, label_keys text[], label_values text[])
RETURNS SCHEMA_PROM.label_array AS $$
    WITH idx_val AS (
        SELECT
            -- only call the functions to create new key positions
            -- and label ids if they don't exist (for performance reasons)
            coalesce(lkp.pos,
              SCHEMA_CATALOG.get_or_create_label_key_pos(label_array.metric_name, kv.key)) idx,
            coalesce(l.id,
              SCHEMA_CATALOG.get_or_create_label_id(kv.key, kv.value)) val
        FROM ROWS FROM(unnest(label_keys), UNNEST(label_values)) AS kv(key, value)
            LEFT JOIN SCHEMA_CATALOG.label l
               ON (l.key = kv.key AND l.value = kv.value)
            LEFT JOIN SCHEMA_CATALOG.label_key_position lkp
               ON
               (
                  lkp.metric_name = label_array.metric_name AND
                  lkp.key = kv.key
               )
        ORDER BY kv.key
    )
    SELECT ARRAY(
        SELECT coalesce(idx_val.val, 0)
        FROM
            generate_series(
                    1,
                    (SELECT max(idx) FROM idx_val)
            ) g
            LEFT JOIN idx_val ON (idx_val.idx = g)
    )::SCHEMA_PROM.label_array
$$
LANGUAGE SQL VOLATILE;
COMMENT ON FUNCTION SCHEMA_PROM.label_array(text, text[], text[])
IS 'converts a metric name, array of keys, and array of values to a label array';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.label_array(TEXT, text[], text[]) TO prom_writer;

-- Returns keys and values for a label_array
-- This function needs to be optimized for performance
CREATE OR REPLACE FUNCTION SCHEMA_PROM.key_value_array(labels SCHEMA_PROM.label_array, OUT keys text[], OUT vals text[])
AS $$
    SELECT
        array_agg(l.key), array_agg(l.value)
    FROM
      label_unnest(labels) label_id
      INNER JOIN SCHEMA_CATALOG.label l ON (l.id = label_id)
$$
LANGUAGE SQL STABLE PARALLEL SAFE;
COMMENT ON FUNCTION SCHEMA_PROM.key_value_array(SCHEMA_PROM.label_array)
IS 'converts a labels array to two arrays: one for keys and another for values';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.key_value_array(SCHEMA_PROM.label_array) TO prom_reader;

--Returns the jsonb for a series defined by a label_array
CREATE OR REPLACE FUNCTION SCHEMA_PROM.jsonb(labels SCHEMA_PROM.label_array)
RETURNS jsonb AS $$
    SELECT
        jsonb_object(keys, vals)
    FROM
      SCHEMA_PROM.key_value_array(labels)
$$
LANGUAGE SQL STABLE PARALLEL SAFE;
COMMENT ON FUNCTION SCHEMA_PROM.jsonb(labels SCHEMA_PROM.label_array)
IS 'converts a labels array to a JSONB object';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.jsonb(SCHEMA_PROM.label_array) TO prom_reader;

--Do not call before checking that the series does not yet exist
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.create_series(
        metric_id int,
        metric_table_name NAME,
        label_array SCHEMA_PROM.label_array,
        OUT series_id BIGINT)
AS $func$
BEGIN
LOOP
    EXECUTE format ($$
        INSERT INTO SCHEMA_DATA_SERIES.%I(metric_id, labels)
        SELECT $1, $2
        ON CONFLICT DO NOTHING
        RETURNING id
    $$, metric_table_name)
    INTO series_id
    USING metric_id, label_array;

    EXIT WHEN series_id is not null;

    EXECUTE format($$
        SELECT id
        FROM SCHEMA_DATA_SERIES.%I
        WHERE labels = $1
    $$, metric_table_name)
    INTO series_id
    USING label_array;

    EXIT WHEN series_id is not null;
END LOOP;
END
$func$
LANGUAGE PLPGSQL VOLATILE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.create_series(int, name, SCHEMA_PROM.label_array) TO prom_writer;

CREATE OR REPLACE  FUNCTION SCHEMA_PROM.series_id(label jsonb)
RETURNS BIGINT AS $$
DECLARE
  series_id bigint;
  table_name name;
  metric_id int;
BEGIN
   --See get_series_id_for_key_value_array for notes about locking
   SELECT mtn.id, mtn.table_name FROM SCHEMA_CATALOG.get_or_create_metric_table_name(label->>'__name__') mtn
   INTO metric_id, table_name;

   LOCK TABLE ONLY SCHEMA_CATALOG.series in ACCESS SHARE mode;

   EXECUTE format($query$
    WITH CTE AS (
        SELECT SCHEMA_PROM.label_array($1)
    )
    SELECT id
    FROM SCHEMA_DATA_SERIES.%1$I as series
    WHERE labels = (SELECT * FROM cte)
    UNION ALL
    SELECT SCHEMA_CATALOG.create_series(%2$L, %1$L, (SELECT * FROM cte))
    LIMIT 1
   $query$, table_name, metric_id)
   USING label
   INTO series_id;

   RETURN series_id;
END
$$
LANGUAGE PLPGSQL VOLATILE;
COMMENT ON FUNCTION SCHEMA_PROM.series_id(jsonb)
IS 'returns the series id that exactly matches a JSONB of labels';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.series_id(jsonb) TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_series_id_for_key_value_array(metric_name TEXT, label_keys text[], label_values text[], OUT table_name NAME, OUT series_id BIGINT)
AS $func$
DECLARE
  metric_id int;
BEGIN
   --need to make sure the series partition exists
   SELECT mtn.id, mtn.table_name FROM SCHEMA_CATALOG.get_or_create_metric_table_name(metric_name) mtn
   INTO metric_id, table_name;

   --lock the series table parent first in access share mode so that
   --its locked before any labels etc are created. Establishes a lock
   --order of series table then labels table.  This prevents
   --deadlocks between calls that create metrics and concurrent calls
   --that don't create metric but do create labels. Where the label-creating
   --workers is trying to create a series and the metric-creator is waiting
   --for a label-creation to complete making it's series. This guarantess that
   -- any metric-creator finshes before other processes create series or labels.
   LOCK TABLE ONLY SCHEMA_CATALOG.series in ACCESS SHARE mode;

   --This query MUST take all of its locks after the create metric table above
   --since this requires a lower level lock on the series table than the potential
   --exclusive lock on series when adding the series partition. This is why this
   --is a PLPGSQL function and not a SQL Function (a SQL function parses/plans all
   --statement during startup, which would take a lock on the series table)
   EXECUTE format($query$
    WITH CTE AS (
        SELECT SCHEMA_PROM.label_array($1, $2, $3)
    )
    SELECT id
    FROM SCHEMA_DATA_SERIES.%1$I as series
    WHERE labels = (SELECT * FROM cte)
    UNION ALL
    SELECT SCHEMA_CATALOG.create_series(%2$L, %1$L, (SELECT * FROM cte))
    LIMIT 1
   $query$, table_name, metric_id)
   USING metric_name, label_keys, label_values
   INTO series_id;

   RETURN;
END
$func$
LANGUAGE PLPGSQL VOLATILE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_series_id_for_key_value_array(TEXT, text[], text[]) TO prom_writer;
--
-- Parameter manipulation functions
--

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.set_chunk_interval_on_metric_table(metric_name TEXT, new_interval INTERVAL)
RETURNS void
AS $func$
    --set interval while adding 1% of randomness to the interval so that chunks are not aligned so that
    --chunks are staggered for compression jobs.
    SELECT set_chunk_time_interval(
        format('SCHEMA_DATA.%I',(SELECT table_name FROM SCHEMA_CATALOG.get_or_create_metric_table_name(metric_name)))::regclass,
        new_interval * (1.0+((random()*0.01)-0.005)));
$func$
LANGUAGE SQL VOLATILE;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.set_default_chunk_interval(chunk_interval INTERVAL)
RETURNS BOOLEAN
AS $$
    INSERT INTO SCHEMA_CATALOG.default(key, value) VALUES('chunk_interval', chunk_interval::text)
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

    SELECT SCHEMA_CATALOG.set_chunk_interval_on_metric_table(metric_name, chunk_interval)
    FROM SCHEMA_CATALOG.metric
    WHERE default_chunk_interval;

    SELECT true;
$$
LANGUAGE SQL VOLATILE;
COMMENT ON FUNCTION SCHEMA_PROM.set_default_chunk_interval(INTERVAL)
IS 'set the chunk interval for any metrics (existing and new) without an explicit override';

CREATE OR REPLACE FUNCTION SCHEMA_PROM.set_metric_chunk_interval(metric_name TEXT, chunk_interval INTERVAL)
RETURNS BOOLEAN
AS $func$
    --use get_or_create_metric_table_name because we want to be able to set /before/ any data is ingested
    --needs to run before update so row exists before update.
    SELECT SCHEMA_CATALOG.get_or_create_metric_table_name(set_metric_chunk_interval.metric_name);

    UPDATE SCHEMA_CATALOG.metric SET default_chunk_interval = false
    WHERE id IN (SELECT id FROM SCHEMA_CATALOG.get_metric_table_name_if_exists(set_metric_chunk_interval.metric_name));

    SELECT SCHEMA_CATALOG.set_chunk_interval_on_metric_table(metric_name, chunk_interval);

    SELECT true;
$func$
LANGUAGE SQL VOLATILE;
COMMENT ON FUNCTION SCHEMA_PROM.set_metric_chunk_interval(TEXT, INTERVAL)
IS 'set a chunk interval for a specific metric (this overrides the default)';

CREATE OR REPLACE FUNCTION SCHEMA_PROM.reset_metric_chunk_interval(metric_name TEXT)
RETURNS BOOLEAN
AS $func$
    UPDATE SCHEMA_CATALOG.metric SET default_chunk_interval = true
    WHERE id = (SELECT id FROM SCHEMA_CATALOG.get_metric_table_name_if_exists(metric_name));

    SELECT SCHEMA_CATALOG.set_chunk_interval_on_metric_table(metric_name,
        SCHEMA_CATALOG.get_default_chunk_interval());

    SELECT true;
$func$
LANGUAGE SQL VOLATILE;
COMMENT ON FUNCTION SCHEMA_PROM.reset_metric_chunk_interval(TEXT)
IS 'resets the chunk interval for a specific metric to using the default';

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_metric_retention_period(metric_name TEXT)
RETURNS INTERVAL
AS $$
    SELECT COALESCE(m.retention_period, SCHEMA_CATALOG.get_default_retention_period())
    FROM SCHEMA_CATALOG.metric m
    WHERE id IN (SELECT id FROM SCHEMA_CATALOG.get_metric_table_name_if_exists(get_metric_retention_period.metric_name))
    UNION ALL
    SELECT SCHEMA_CATALOG.get_default_retention_period()
    LIMIT 1
$$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_metric_retention_period(TEXT) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.set_default_retention_period(retention_period INTERVAL)
RETURNS BOOLEAN
AS $$
    INSERT INTO SCHEMA_CATALOG.default(key, value) VALUES('retention_period', retention_period::text)
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
    SELECT true;
$$
LANGUAGE SQL VOLATILE;
COMMENT ON FUNCTION SCHEMA_PROM.set_default_retention_period(INTERVAL)
IS 'set the retention period for any metrics (existing and new) without an explicit override';

CREATE OR REPLACE FUNCTION SCHEMA_PROM.set_metric_retention_period(metric_name TEXT, new_retention_period INTERVAL)
RETURNS BOOLEAN
AS $func$
    --use get_or_create_metric_table_name because we want to be able to set /before/ any data is ingested
    --needs to run before update so row exists before update.
    SELECT SCHEMA_CATALOG.get_or_create_metric_table_name(set_metric_retention_period.metric_name);

    UPDATE SCHEMA_CATALOG.metric SET retention_period = new_retention_period
    WHERE id IN (SELECT id FROM SCHEMA_CATALOG.get_metric_table_name_if_exists(set_metric_retention_period.metric_name));

    SELECT true;
$func$
LANGUAGE SQL VOLATILE;
COMMENT ON FUNCTION SCHEMA_PROM.set_metric_retention_period(TEXT, INTERVAL)
IS 'set a retention period for a specific metric (this overrides the default)';

CREATE OR REPLACE FUNCTION SCHEMA_PROM.reset_metric_retention_period(metric_name TEXT)
RETURNS BOOLEAN
AS $func$
    UPDATE SCHEMA_CATALOG.metric SET retention_period = NULL
    WHERE id = (SELECT id FROM SCHEMA_CATALOG.get_metric_table_name_if_exists(metric_name));
    SELECT true;
$func$
LANGUAGE SQL VOLATILE;
COMMENT ON FUNCTION SCHEMA_PROM.reset_metric_retention_period(TEXT)
IS 'resets the retention period for a specific metric to using the default';

--drop chunks from metrics tables and delete the appropriate series.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.drop_metric_chunks(metric_name TEXT, older_than TIMESTAMPTZ)
    RETURNS BOOLEAN
    AS $func$
DECLARE
    metric_table NAME;
    check_time TIMESTAMPTZ;
    time_dimension_id INT;
    label_array int[];
BEGIN
    SELECT table_name
    INTO STRICT metric_table
    FROM SCHEMA_CATALOG.get_or_create_metric_table_name(metric_name);

    SELECT older_than + INTERVAL '1 hour'
    INTO check_time;

    --Get the time dimension id for the time dimension
    SELECT d.id
    INTO STRICT time_dimension_id
    FROM _timescaledb_catalog.hypertable h
    INNER JOIN _timescaledb_catalog.dimension d ON (d.hypertable_id = h.id)
    WHERE h.schema_name = 'SCHEMA_DATA' AND h.table_name = metric_table
    ORDER BY d.id ASC
    LIMIT 1;

    --Get a tight older_than (EXCLUSIVE) because we want to know the
    --exact cut-off where things will be dropped
    SELECT _timescaledb_internal.to_timestamp(range_end)
    INTO older_than
    FROM _timescaledb_catalog.chunk c
    INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (c.id = cc.chunk_id)
    INNER JOIN _timescaledb_catalog.dimension_slice ds ON (ds.id = cc.dimension_slice_id)
    --range_end is exclusive so this is everything < older_than (which is also exclusive)
    WHERE ds.dimension_id = time_dimension_id AND ds.range_end <= _timescaledb_internal.to_unix_microseconds(older_than)
    ORDER BY range_end DESC
    LIMIT 1;

    IF older_than IS NULL THEN
        RETURN false;
    END IF;

    --chances are that the hour after the drop point will have the most similar
    --series to what is dropped, so first filter by all series that have been dropped
    --but that aren't in that first hour and then make sure they aren't in the dataset
    EXECUTE format(
    $query$
        WITH potentially_drop_series AS (
            SELECT distinct series_id
            FROM SCHEMA_DATA.%1$I
            WHERE time < %2$L
            EXCEPT
            SELECT distinct series_id
            FROM SCHEMA_DATA.%1$I
            WHERE time >= %2$L AND time < %3$L
        ), confirmed_drop_series AS (
            SELECT series_id
            FROM potentially_drop_series
            WHERE NOT EXISTS (
                 SELECT 1
                 FROM  SCHEMA_DATA.%1$I  data_exists
                 WHERE data_exists.series_id = potentially_drop_series.series_id AND time >= %3$L
                 --use chunk append + more likely to find something starting at earliest time
                 ORDER BY time ASC
                 LIMIT 1
            )
        ), deleted_series AS (
          DELETE from SCHEMA_DATA_SERIES.%1$I
          WHERE id IN (SELECT series_id FROM confirmed_drop_series)
          RETURNING id, labels
        )
        SELECT ARRAY(SELECT DISTINCT unnest(labels) as label_id
        FROM deleted_series)
    $query$, metric_table, older_than, check_time) INTO label_array;

    --needs to be a separate query and not a CTE since this needs to "see"
    --the series rows deleted above as deleted.
    EXECUTE format($query$
    WITH confirmed_drop_labels AS (
            SELECT label_id
            FROM unnest($1) as labels(label_id)
            WHERE NOT EXISTS (
                 SELECT 1
                 FROM  SCHEMA_DATA_SERIES.%1$I series_exists
                 WHERE series_exists.labels && ARRAY[labels.label_id]
                 LIMIT 1
            )
        )
        DELETE FROM SCHEMA_CATALOG.label
        WHERE id IN (SELECT * FROM confirmed_drop_labels);
    $query$, metric_table) USING label_array;

   PERFORM drop_chunks(table_name=>metric_table, schema_name=> 'SCHEMA_DATA', older_than=>older_than);
   RETURN true;
END
$func$
LANGUAGE PLPGSQL VOLATILE;

--Order by random with stable marking gives us same order in a statement and different
-- orderings in different statements
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_metrics_that_need_drop_chunk()
RETURNS SETOF SCHEMA_CATALOG.metric
AS $$
        SELECT m.*
        FROM SCHEMA_CATALOG.metric m
        WHERE EXISTS (
            SELECT 1 FROM
            show_chunks(hypertable=>format('%I.%I', 'SCHEMA_DATA', m.table_name),
                         older_than=>NOW() - SCHEMA_CATALOG.get_metric_retention_period(m.metric_name)))
        --random order also to prevent starvation
        ORDER BY random()
$$
LANGUAGE SQL STABLE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_metrics_that_need_drop_chunk() TO prom_reader;

--public procedure to be called by cron
CREATE PROCEDURE SCHEMA_PROM.drop_chunks()
AS $$
DECLARE
    r RECORD;
BEGIN
    --do one loop with skip locked and then one that blocks to prevent starvation
    FOR r IN
        SELECT *
        FROM SCHEMA_CATALOG.get_metrics_that_need_drop_chunk()
    LOOP
        --lock prevents concurrent drop_chunks on same table
        PERFORM m.*
        FROM SCHEMA_CATALOG.metric m
        WHERE m.id = r.id
        FOR NO KEY UPDATE SKIP LOCKED;

        CONTINUE WHEN NOT FOUND;

        PERFORM SCHEMA_CATALOG.drop_metric_chunks(r.metric_name, NOW() - SCHEMA_CATALOG.get_metric_retention_period(r.metric_name));
        COMMIT;
    END LOOP;

    FOR r IN
        SELECT *
        FROM SCHEMA_CATALOG.get_metrics_that_need_drop_chunk()
    LOOP
        PERFORM SCHEMA_CATALOG.drop_metric_chunks(r.metric_name, NOW() - SCHEMA_CATALOG.get_metric_retention_period(r.metric_name));
        COMMIT;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;
COMMENT ON PROCEDURE SCHEMA_PROM.drop_chunks()
IS 'drops data according to the data retention policy. This procedure should be run regularly in a cron job';

CREATE OR REPLACE FUNCTION SCHEMA_PROM.is_stale_marker(value double precision)
RETURNS BOOLEAN
AS $func$
    SELECT float8send(value) = '\x7ff0000000000002'
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
COMMENT ON FUNCTION SCHEMA_PROM.is_stale_marker(double precision)
IS 'returns true if the value is a Prometheus stale marker';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.is_stale_marker(double precision) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.is_normal_nan(value double precision)
RETURNS BOOLEAN
AS $func$
    SELECT float8send(value) = '\x7ff8000000000001'
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
COMMENT ON FUNCTION SCHEMA_PROM.is_normal_nan(double precision)
IS 'returns true if the value is a NaN';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.is_normal_nan(double precision) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.val(
        label_id INT)
    RETURNS TEXT
AS $$
    SELECT
        value
    FROM SCHEMA_CATALOG.label
    WHERE
        id = label_id
$$
LANGUAGE SQL STABLE PARALLEL SAFE;
COMMENT ON FUNCTION SCHEMA_PROM.val(INT)
IS 'returns the label value from a label id';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.val(INT) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_label_key_column_name_for_view(label_key text, id BOOLEAN)
    returns NAME
AS $func$
DECLARE
  is_reserved boolean;
BEGIN
  SELECT label_key = ANY(ARRAY['time', 'value', 'series_id', 'labels'])
  INTO STRICT is_reserved;

  IF is_reserved THEN
    label_key := 'label_' || label_key;
  END IF;

  IF id THEN
    RETURN (SCHEMA_CATALOG.get_or_create_label_key(label_key)).id_column_name;
  ELSE
    RETURN (SCHEMA_CATALOG.get_or_create_label_key(label_key)).value_column_name;
  END IF;
END
$func$
LANGUAGE PLPGSQL VOLATILE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_label_key_column_name_for_view(text, BOOLEAN) TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.create_series_view(
        metric_name text)
    RETURNS BOOLEAN
AS $func$
DECLARE
   label_value_cols text;
   view_name text;
   metric_id int;
BEGIN
    SELECT
        ',' || string_agg(
            format ('SCHEMA_PROM.val(series.labels[%s]) AS %I',pos::int, SCHEMA_CATALOG.get_label_key_column_name_for_view(key, false))
        , ', ' ORDER BY pos)
    INTO STRICT label_value_cols
    FROM SCHEMA_CATALOG.label_key_position lkp
    WHERE lkp.metric_name = create_series_view.metric_name and key != '__name__';

    SELECT m.table_name, m.id
    INTO STRICT view_name, metric_id
    FROM SCHEMA_CATALOG.metric m
    WHERE m.metric_name = create_series_view.metric_name;

    EXECUTE FORMAT($$
        CREATE OR REPLACE VIEW SCHEMA_SERIES.%1$I AS
        SELECT
            id AS series_id,
            labels
            %2$s
        FROM
            SCHEMA_DATA_SERIES.%1$I AS series
    $$, view_name, label_value_cols);
    RETURN true;
END
$func$
LANGUAGE PLPGSQL VOLATILE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.create_series_view(text) TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.create_metric_view(
        metric_name text)
    RETURNS BOOLEAN
AS $func$
DECLARE
   label_value_cols text;
   table_name text;
   metric_id int;
BEGIN
    SELECT
        ',' || string_agg(
            format ('series.labels[%s] AS %I',pos::int, SCHEMA_CATALOG.get_label_key_column_name_for_view(key, true))
        , ', ' ORDER BY pos)
    INTO STRICT label_value_cols
    FROM SCHEMA_CATALOG.label_key_position lkp
    WHERE lkp.metric_name = create_metric_view.metric_name and key != '__name__';

    SELECT m.table_name, m.id
    INTO STRICT table_name, metric_id
    FROM SCHEMA_CATALOG.metric m
    WHERE m.metric_name = create_metric_view.metric_name;

    EXECUTE FORMAT($$
        CREATE OR REPLACE VIEW SCHEMA_METRIC.%1$I AS
        SELECT
            data.time as time,
            data.value as value,
            data.series_id AS series_id,
            series.labels
            %2$s
        FROM
            SCHEMA_DATA.%1$I AS data
            LEFT JOIN SCHEMA_DATA_SERIES.%1$I AS series ON (series.id = data.series_id)
    $$, table_name, label_value_cols);
    RETURN true;
END
$func$
LANGUAGE PLPGSQL VOLATILE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.create_metric_view(text) TO prom_writer;

----------------------------------
-- Label selectors and matchers --
----------------------------------

CREATE DOMAIN SCHEMA_PROM.matcher_positive AS int[] NOT NULL;
CREATE DOMAIN SCHEMA_PROM.matcher_negative AS int[] NOT NULL;
CREATE DOMAIN SCHEMA_PROM.label_key AS TEXT NOT NULL;
CREATE DOMAIN SCHEMA_PROM.pattern AS TEXT NOT NULL;

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

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_find_key_equal(label_key SCHEMA_PROM.label_key, pattern SCHEMA_PROM.pattern)
RETURNS SCHEMA_PROM.matcher_positive
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::SCHEMA_PROM.matcher_positive
    FROM SCHEMA_CATALOG.label l
    WHERE l.key = label_key and l.value = pattern
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.label_find_key_equal(SCHEMA_PROM.label_key, SCHEMA_PROM.pattern) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_find_key_not_equal(label_key SCHEMA_PROM.label_key, pattern SCHEMA_PROM.pattern)
RETURNS SCHEMA_PROM.matcher_negative
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::SCHEMA_PROM.matcher_negative
    FROM SCHEMA_CATALOG.label l
    WHERE l.key = label_key and l.value = pattern
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.label_find_key_not_equal(SCHEMA_PROM.label_key, SCHEMA_PROM.pattern) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_find_key_regex(label_key SCHEMA_PROM.label_key, pattern SCHEMA_PROM.pattern)
RETURNS SCHEMA_PROM.matcher_positive
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::SCHEMA_PROM.matcher_positive
    FROM SCHEMA_CATALOG.label l
    WHERE l.key = label_key and l.value ~ pattern
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.label_find_key_regex(SCHEMA_PROM.label_key, SCHEMA_PROM.pattern) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_find_key_not_regex(label_key SCHEMA_PROM.label_key, pattern SCHEMA_PROM.pattern)
RETURNS SCHEMA_PROM.matcher_negative
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::SCHEMA_PROM.matcher_negative
    FROM SCHEMA_CATALOG.label l
    WHERE l.key = label_key and l.value ~ pattern
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

--------------------------------- Views --------------------------------

CREATE VIEW SCHEMA_INFO.metric AS
   SELECT
     m.id,
     m.metric_name,
     m.table_name,
     SCHEMA_CATALOG.get_metric_retention_period(m.metric_name) as retention_period,
    (
        SELECT _timescaledb_internal.to_interval(interval_length)
        FROM _timescaledb_catalog.dimension d
        WHERE d.hypertable_id = h.id
        ORDER BY d.id ASC
        LIMIT 1
    ) as chunk_interval,
     ARRAY(
        SELECT key
        FROM SCHEMA_CATALOG.label_key_position lkp
        WHERE lkp.metric_name = m.metric_name
        ORDER BY key) label_keys,
    hi.total_size as size,
    (1.0 - (pg_size_bytes(chs.compressed_total_bytes)::numeric / pg_size_bytes(chs.uncompressed_total_bytes)::numeric)) * 100 as compression_ratio,
    chs.total_chunks,
    chs.number_compressed_chunks as compressed_chunks
   FROM SCHEMA_CATALOG.metric m
   LEFT JOIN timescaledb_information.hypertable hi ON
              (hi.table_schema = 'SCHEMA_DATA' AND hi.table_name = m.table_name)
   LEFT JOIN timescaledb_information.compressed_hypertable_stats chs ON
              (chs.hypertable_name = format('%I.%I', 'SCHEMA_DATA', m.table_name)::regclass)
   LEFT JOIN _timescaledb_catalog.hypertable h ON
              (h.schema_name = 'SCHEMA_DATA' AND h.table_name = m.table_name);

CREATE VIEW SCHEMA_INFO.label AS
  SELECT
    lk.key,
    lk.value_column_name,
    lk.id_column_name,
    ARRAY(SELECT value FROM SCHEMA_CATALOG.label l WHERE l.key = lk.key ORDER BY value)
      AS values
  FROM SCHEMA_CATALOG.label_key lk;
