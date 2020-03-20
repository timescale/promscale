--NOTES
--This code assumes that table names can only be 63 chars long

CREATE SCHEMA _prom_catalog;
CREATE SCHEMA _prom_internal;
CREATE SCHEMA prom; --data tables go here

CREATE EXTENSION IF NOT EXISTS timescaledb;

-----------------------
-- Table definitions --
-----------------------

CREATE TABLE _prom_catalog.series (
    id bigserial,
    metric_id int,
    labels int[],
    UNIQUE(labels) INCLUDE (id)
);
CREATE INDEX series_labels_id ON _prom_catalog.series USING GIN (labels);

CREATE TABLE _prom_catalog.label (
    id serial,
    key TEXT,
    value text,
    PRIMARY KEY (id) INCLUDE (key, value),
    UNIQUE (key, value) INCLUDE (id)
);

CREATE TABLE _prom_catalog.label_key_position (
    metric text,
    key TEXT,
    pos int,
    UNIQUE (metric, key) INCLUDE (pos)
);
CREATE INDEX ON _prom_catalog.label_key_position(metric, key) INCLUDE (pos);

CREATE TABLE _prom_catalog.metric (
    id SERIAL PRIMARY KEY,
    metric_name text,
    table_name name,
    default_chunk_interval BOOLEAN DEFAULT true,
    UNIQUE (metric_name) INCLUDE (table_name),
    UNIQUE(table_name)
);

CREATE TABLE _prom_catalog.default (
    key TEXT PRIMARY KEY,
    value TEXT
);

INSERT INTO _prom_catalog.default(key,value) VALUES ('chunk_interval', (INTERVAL '8 hours')::text);

CREATE OR REPLACE FUNCTION _prom_internal.get_default_chunk_interval()
    RETURNS INTERVAL
AS $func$
    SELECT value::INTERVAL FROM _prom_catalog.default WHERE key='chunk_interval';
$func$
LANGUAGE sql STABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION _prom_internal.make_metric_table()
    RETURNS trigger
    AS $func$
DECLARE
BEGIN
   EXECUTE format('CREATE TABLE prom.%I(time TIMESTAMPTZ, value DOUBLE PRECISION, series_id INT)',
                    NEW.table_name);
   EXECUTE format('CREATE INDEX ON prom.%I (series_id, time) INCLUDE (value)',
                    NEW.table_name);
   PERFORM create_hypertable(format('prom.%I', NEW.table_name), 'time',
                             chunk_time_interval=>_prom_internal.get_default_chunk_interval());
   EXECUTE format($$
     ALTER TABLE prom.%I SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'series_id',
        timescaledb.compress_orderby = 'time'
    ); $$, NEW.table_name);

   --chunks where the end time is before now()-10 minutes will be compressed
   PERFORM add_compress_chunks_policy(format('prom.%I', NEW.table_name), INTERVAL '10 minutes');
   RETURN NEW;
END
$func$
LANGUAGE plpgsql;

CREATE TRIGGER make_metric_table_trigger
    AFTER INSERT ON _prom_catalog.metric
    FOR EACH ROW
    EXECUTE PROCEDURE _prom_internal.make_metric_table();




------------------------
-- Internal functions --
------------------------

-- Return a table name built from a metric_name and a suffix.
-- The metric name is truncated so that the suffix could fit in full.
CREATE OR REPLACE FUNCTION _prom_internal.metric_table_name_with_suffix(
        metric_name text, suffix text)
    RETURNS name
AS $func$
    SELECT (substring(metric_name for 63-(char_length(suffix)+1)) || '_' || suffix)::name
$func$
LANGUAGE sql IMMUTABLE PARALLEL SAFE;

-- Return a new name for a metric table.
-- This tries to use the metric table in full. But if the
-- metric name doesn't fit, generates a new unique name.
-- Note that this can use up the next val of _prom_catalog.metric_name_suffx
-- so it should be called only if a table does not yet exist.
CREATE OR REPLACE FUNCTION _prom_internal.new_metric_table_name(
        metric_name_arg text, metric_id int)
    RETURNS name
AS $func$
    SELECT CASE
        WHEN char_length(metric_name_arg) < 63 THEN
            metric_name_arg::name
        ELSE
            _prom_internal.metric_table_name_with_suffix(
                metric_name_arg, metric_id::text
            )
        END
$func$
LANGUAGE sql VOLATILE PARALLEL SAFE;

--Creates a new table for a given metric name.
--This uses up some sequences so should only be called
--If the table does not yet exist.
CREATE OR REPLACE FUNCTION _prom_internal.create_metric_table(
        metric_name_arg text, OUT id int, OUT table_name name)
AS $func$
DECLARE
  new_id int;
BEGIN
new_id = nextval(pg_get_serial_sequence('_prom_catalog.metric','id'))::int;
LOOP
    INSERT INTO _prom_catalog.metric (id, metric_name, table_name)
        SELECT  new_id,
                metric_name_arg,
                _prom_internal.new_metric_table_name(metric_name_arg, new_id)
    ON CONFLICT DO NOTHING
    RETURNING _prom_catalog.metric.id, _prom_catalog.metric.table_name
    INTO id, table_name;
    -- under high concurrency the insert may not return anything, so try a select and loop
    -- https://stackoverflow.com/a/15950324
    EXIT WHEN FOUND;

    SELECT m.id, m.table_name
    INTO id, table_name
    FROM _prom_catalog.metric m
    WHERE metric_name = metric_name_arg;

    EXIT WHEN FOUND;
END LOOP;
END
$func$
LANGUAGE PLPGSQL VOLATILE PARALLEL SAFE;

-- Get a new label array position for a label key. For any metric,
-- we want the positions to be as compact as possible.
-- This uses some pretty heavy locks so use sparingly.
CREATE OR REPLACE FUNCTION _prom_internal.get_new_pos_for_key(
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
        _prom_catalog.label_key_position
    WHERE
        metric = metric_name
        AND KEY = key_name
    INTO position;

    IF FOUND THEN
        RETURN position;
    END IF;

    SELECT table_name
    FROM get_or_create_metric_table_name(metric_name)
    INTO metric_table;
    --lock as for ALTER TABLE because we are in effect changing the schema here
    --also makes sure the next_position below is correct in terms of concurrency
    EXECUTE format('LOCK TABLE prom.%I IN SHARE UPDATE EXCLUSIVE MODE', metric_table);
    --second check after lock
    SELECT
        pos
    FROM
        _prom_catalog.label_key_position
    WHERE
        metric = metric_name
        AND KEY = key_name INTO position;

    IF FOUND THEN
        RETURN position;
    END IF;

    SELECT
        max(pos) + 1
    FROM
        _prom_catalog.label_key_position
    WHERE
        metric = metric_name INTO next_position;

    IF next_position IS NULL THEN
        next_position := 1; -- 1-indexed arrays
    END IF;

    INSERT INTO _prom_catalog.label_key_position
        VALUES (metric_name, key_name, next_position)
    ON CONFLICT
        DO NOTHING
    RETURNING
        pos INTO position;

    IF NOT FOUND THEN
        RAISE 'Could not find a new position';
    END IF;
    RETURN position;
END
$func$
LANGUAGE plpgsql;

--should only be called after a check that that the label doesn't exist
CREATE OR REPLACE FUNCTION _prom_internal.get_new_label_id(key_name text, value_name text, OUT id INT)
AS $func$
BEGIN
LOOP
    INSERT INTO
        _prom_catalog.label(key, value)
    VALUES
        (key_name,value_name)
    ON CONFLICT DO NOTHING
    RETURNING _prom_catalog.label.id
    INTO id;

    EXIT WHEN FOUND;

    SELECT
        l.id
    INTO id
    FROM _prom_catalog.label l
    WHERE
        key = key_name AND
        value = value_name;

    EXIT WHEN FOUND;
END LOOP;
END
$func$
LANGUAGE PLPGSQL;

--wrapper around jsonb_each_text to give a better row_estimate
--for labels (10 not 100)
CREATE OR REPLACE FUNCTION _prom_internal.label_jsonb_each_text(js jsonb,  OUT key text, OUT value text)
 RETURNS SETOF record
 LANGUAGE internal
 IMMUTABLE PARALLEL SAFE STRICT ROWS 10
AS $function$jsonb_each_text$function$;

--wrapper around unnest to give better row estimate (10 not 100)
CREATE OR REPLACE FUNCTION _prom_internal.label_unnest(label_array anyarray)
 RETURNS SETOF anyelement
 LANGUAGE internal
 IMMUTABLE PARALLEL SAFE STRICT ROWS 10
AS $function$array_unnest$function$;


---------------------------------------------------
------------------- Public APIs -------------------
---------------------------------------------------

-- Public function to get the name of the table for a given metric
-- This will create the metric table if it does not yet exist.
CREATE OR REPLACE FUNCTION get_or_create_metric_table_name(
        metric_name text)
    RETURNS TABLE (id int, table_name name)
AS $func$
   SELECT id, table_name::name
   FROM _prom_catalog.metric m
   WHERE m.metric_name = get_or_create_metric_table_name.metric_name
   UNION ALL
   SELECT *
   FROM _prom_internal.create_metric_table(get_or_create_metric_table_name.metric_name)
   LIMIT 1
$func$
LANGUAGE sql VOLATILE PARALLEL SAFE;

--public function to get the array position for a label key
CREATE OR REPLACE FUNCTION get_label_key_pos(
        metric_name text, key_name text)
    RETURNS INT
AS $$
    --only executes the more expensive PLPGSQL function if the label doesn't exist
    SELECT
        pos
    FROM
        _prom_catalog.label_key_position
    WHERE
        metric = metric_name
        AND KEY = key_name
    UNION ALL
    SELECT
        _prom_internal.get_new_pos_for_key(metric_name, key_name)
    LIMIT 1
$$
LANGUAGE SQL;

--Get the label_id for a key, value pair
CREATE OR REPLACE FUNCTION get_label_id(
        key_name text, value_name text)
    RETURNS INT
AS $$
    --first select to prevent sequence from being used up
    --unnecessarily
    SELECT
        id
    FROM _prom_catalog.label
    WHERE
        key = key_name AND
        value = value_name
    UNION ALL
    SELECT
        _prom_internal.get_new_label_id(key_name, value_name)
    LIMIT 1
$$
LANGUAGE SQL;

--This generates a position based array from the jsonb
--0s represent keys that are not set (we don't use NULL
--since intarray does not support it).
--This is not super performance critical since this
--is only used on the insert client and is cached there.
CREATE OR REPLACE FUNCTION jsonb_to_label_array(js jsonb)
RETURNS INT[] AS $$
    WITH idx_val AS (
        SELECT
            -- only call the functions to create new key positions
            -- and label ids if they don't exist (for performance reasons)
            coalesce(lkp.pos,
              get_label_key_pos(js->>'__name__', e.key)) idx,
            coalesce(l.id,
              get_label_id(e.key, e.value)) val
        FROM _prom_internal.label_jsonb_each_text(js) e
             LEFT JOIN _prom_catalog.label l
               ON (l.key = e.key AND l.value = e.value)
            LEFT JOIN _prom_catalog.label_key_position lkp
               ON
               (
                  lkp.metric = js->>'__name__' AND
                  lkp.key = e.key
               )
        --needs to order by key to prevent deadlocks if get_label_id is creating labels
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
    )
$$
LANGUAGE SQL VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION key_value_array_to_label_array(metric_name TEXT, label_keys text[], label_values text[])
RETURNS INT[] AS $$
    WITH idx_val AS (
        SELECT
            -- only call the functions to create new key positions
            -- and label ids if they don't exist (for performance reasons)
            coalesce(lkp.pos,
              get_label_key_pos(metric_name, kv.key)) idx,
            coalesce(l.id,
              get_label_id(kv.key, kv.value)) val
        FROM ROWS FROM(unnest(label_keys), UNNEST(label_values)) AS kv(key, value)
            LEFT JOIN _prom_catalog.label l
               ON (l.key = kv.key AND l.value = kv.value)
            LEFT JOIN _prom_catalog.label_key_position lkp
               ON
               (
                  lkp.metric = metric_name AND
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
    )
$$
LANGUAGE SQL VOLATILE PARALLEL SAFE;

--Returns the jsonb for a series defined by a label_array
--Note that this function should be optimized for performance
CREATE OR REPLACE FUNCTION label_array_to_jsonb(labels int[])
RETURNS jsonb AS $$
    SELECT
        jsonb_object(array_agg(l.key), array_agg(l.value))
    FROM
      _prom_internal.label_unnest(labels) label_id
      INNER JOIN _prom_catalog.label l ON (l.id = label_id)
$$
LANGUAGE SQL STABLE PARALLEL SAFE;

--Do not call before checking that the series does not yet exist
CREATE OR REPLACE FUNCTION _prom_internal.create_series(
        metric_id int,
        label_array int[],
        OUT series_id BIGINT)
AS $func$
BEGIN
LOOP
    INSERT INTO _prom_catalog.series(metric_id, labels)
    SELECT metric_id, label_array
    ON CONFLICT DO NOTHING
    RETURNING id
    INTO series_id;

    EXIT WHEN FOUND;

    SELECT id
    INTO series_id
    FROM _prom_catalog.series
    WHERE labels = label_array;

    EXIT WHEN FOUND;
END LOOP;
END
$func$
LANGUAGE PLPGSQL VOLATILE PARALLEL SAFE;

CREATE OR REPLACE  FUNCTION get_series_id_for_label(label jsonb)
RETURNS BIGINT AS $$
   WITH CTE AS (
       SELECT jsonb_to_label_array(label)
   )
   SELECT id
   FROM _prom_catalog.series
   WHERE labels = (SELECT * FROM cte)
   UNION ALL
   SELECT _prom_internal.create_series((get_or_create_metric_table_name(label->>'__name__')).id, (SELECT * FROM cte))
   LIMIT 1
$$
LANGUAGE SQL VOLATILE PARALLEL SAFE;

CREATE OR REPLACE  FUNCTION get_series_id_for_key_value_array(metric_name TEXT, label_keys text[], label_values text[])
RETURNS BIGINT AS $$
   WITH CTE AS (
       SELECT key_value_array_to_label_array(metric_name, label_keys, label_values)
   )
   SELECT id
   FROM _prom_catalog.series
   WHERE labels = (SELECT * FROM cte)
   UNION ALL
   SELECT _prom_internal.create_series((get_or_create_metric_table_name(metric_name)).id, (SELECT * FROM cte))
   LIMIT 1
$$
LANGUAGE SQL VOLATILE PARALLEL SAFE;

--
-- Parameter manipulation functions
--

CREATE OR REPLACE FUNCTION _prom_internal.set_chunk_interval_on_metric_table(metric_name TEXT, new_interval INTERVAL)
RETURNS void
AS $func$
    SELECT set_chunk_time_interval(
        format('prom.%I',(SELECT table_name FROM get_or_create_metric_table_name(metric_name)))::regclass,
        new_interval);
$func$
LANGUAGE SQL VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION prom.set_default_chunk_interval(chunk_interval INTERVAL)
RETURNS BOOLEAN
AS $$
    INSERT INTO _prom_catalog.default(key, value) VALUES('chunk_interval', chunk_interval::text)
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

    SELECT _prom_internal.set_chunk_interval_on_metric_table(metric_name, chunk_interval)
    FROM _prom_catalog.metric
    WHERE default_chunk_interval;

    SELECT true;
$$
LANGUAGE sql VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION prom.set_metric_chunk_interval(metric_name TEXT, chunk_interval INTERVAL)
RETURNS BOOLEAN
AS $func$
    UPDATE _prom_catalog.metric SET default_chunk_interval = false
    WHERE id IN (SELECT id FROM get_or_create_metric_table_name(set_metric_chunk_interval.metric_name));

    SELECT _prom_internal.set_chunk_interval_on_metric_table(metric_name, chunk_interval);

    SELECT true;
$func$
LANGUAGE SQL VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION prom.reset_metric_chunk_interval(metric_name TEXT)
RETURNS BOOLEAN
AS $func$
    UPDATE _prom_catalog.metric SET default_chunk_interval = true
    WHERE id = (SELECT id FROM get_or_create_metric_table_name(metric_name));

    SELECT _prom_internal.set_chunk_interval_on_metric_table(metric_name,
        _prom_internal.get_default_chunk_interval());

    SELECT true;
$func$
LANGUAGE SQL VOLATILE PARALLEL SAFE;
