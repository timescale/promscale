--NOTES
--This code assumes that table names can only be 63 chars long

CREATE SCHEMA IF NOT EXISTS SCHEMA_CATALOG; -- catalog tables + internal functions
CREATE SCHEMA IF NOT EXISTS SCHEMA_PROM; -- data tables + public functions
CREATE SCHEMA IF NOT EXISTS SCHEMA_SERIES;
CREATE SCHEMA IF NOT EXISTS SCHEMA_METRIC;


CREATE EXTENSION IF NOT EXISTS timescaledb WITH SCHEMA public;
-----------------------
-- Table definitions --
-----------------------

CREATE TABLE SCHEMA_CATALOG.series (
    id bigserial PRIMARY KEY,
    metric_id int,
    labels int[],
    UNIQUE(labels) INCLUDE (id)
);
CREATE INDEX series_labels_id ON SCHEMA_CATALOG.series USING GIN (labels);

CREATE TABLE SCHEMA_CATALOG.label (
    id serial,
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
    metric text,
    key TEXT, --NOT label_key.id for performance reasons.
    pos int,
    UNIQUE (metric, key) INCLUDE (pos)
);

CREATE TABLE SCHEMA_CATALOG.metric (
    id SERIAL PRIMARY KEY,
    metric_name text,
    table_name name,
    default_chunk_interval BOOLEAN DEFAULT true,
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
('retention_period', (90 * INTERVAL '24 hour')::text);


CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_default_chunk_interval()
    RETURNS INTERVAL
AS $func$
    SELECT value::INTERVAL FROM SCHEMA_CATALOG.default WHERE key='chunk_interval';
$func$
LANGUAGE sql STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_default_retention_period()
    RETURNS INTERVAL
AS $func$
    SELECT value::INTERVAL FROM SCHEMA_CATALOG.default WHERE key='retention_period';
$func$
LANGUAGE sql STABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.make_metric_table()
    RETURNS trigger
    AS $func$
DECLARE
BEGIN
   EXECUTE format('CREATE TABLE SCHEMA_PROM.%I(time TIMESTAMPTZ, value DOUBLE PRECISION, series_id INT)',
                    NEW.table_name);
   EXECUTE format('CREATE INDEX ON SCHEMA_PROM.%I (series_id, time) INCLUDE (value)',
                    NEW.table_name);
   PERFORM create_hypertable(format('SCHEMA_PROM.%I', NEW.table_name), 'time',
                             chunk_time_interval=>SCHEMA_CATALOG.get_default_chunk_interval());
   EXECUTE format($$
     ALTER TABLE SCHEMA_PROM.%I SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'series_id',
        timescaledb.compress_orderby = 'time'
    ); $$, NEW.table_name);

   --chunks where the end time is before now()-10 minutes will be compressed
   PERFORM add_compress_chunks_policy(format('SCHEMA_PROM.%I', NEW.table_name), INTERVAL '10 minutes');
   RETURN NEW;
END
$func$
LANGUAGE plpgsql;

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
LANGUAGE sql IMMUTABLE PARALLEL SAFE;

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
LANGUAGE sql IMMUTABLE PARALLEL SAFE;

--Creates a new table for a given metric name.
--This uses up some sequences so should only be called
--If the table does not yet exist.
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
LANGUAGE PLPGSQL VOLATILE PARALLEL SAFE;

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
LANGUAGE PLPGSQL VOLATILE PARALLEL SAFE;

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
LANGUAGE sql VOLATILE PARALLEL SAFE;

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
        SCHEMA_CATALOG.label_key_position
    WHERE
        metric = metric_name
        AND KEY = key_name
    INTO position;

    IF FOUND THEN
        RETURN position;
    END IF;

    SELECT table_name
    FROM SCHEMA_PROM.get_or_create_metric_table_name(metric_name)
    INTO metric_table;
    --lock as for ALTER TABLE because we are in effect changing the schema here
    --also makes sure the next_position below is correct in terms of concurrency
    EXECUTE format('LOCK TABLE SCHEMA_PROM.%I IN SHARE UPDATE EXCLUSIVE MODE', metric_table);
    --second check after lock
    SELECT
        pos
    FROM
        SCHEMA_CATALOG.label_key_position
    WHERE
        metric = metric_name
        AND KEY = key_name INTO position;

    IF FOUND THEN
        RETURN position;
    END IF;

    IF key_name = '__name__' THEN
       next_position := 1; -- 1-indexed arrays, __name__ as first element
    ELSE
        SELECT
            max(pos) + 1
        FROM
            SCHEMA_CATALOG.label_key_position
        WHERE
            metric = metric_name INTO next_position;

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
LANGUAGE plpgsql;

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
LANGUAGE PLPGSQL;

--wrapper around jsonb_each_text to give a better row_estimate
--for labels (10 not 100)
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_jsonb_each_text(js jsonb,  OUT key text, OUT value text)
 RETURNS SETOF record
 LANGUAGE internal
 IMMUTABLE PARALLEL SAFE STRICT ROWS 10
AS $function$jsonb_each_text$function$;

--wrapper around unnest to give better row estimate (10 not 100)
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_unnest(label_array anyarray)
 RETURNS SETOF anyelement
 LANGUAGE internal
 IMMUTABLE PARALLEL SAFE STRICT ROWS 10
AS $function$array_unnest$function$;


---------------------------------------------------
------------------- Public APIs -------------------
---------------------------------------------------

CREATE OR REPLACE FUNCTION SCHEMA_PROM.get_metric_table_name_if_exists(
        metric_name text)
    RETURNS TABLE (id int, table_name name)
AS $func$
   SELECT id, table_name::name
   FROM SCHEMA_CATALOG.metric m
   WHERE m.metric_name = get_metric_table_name_if_exists.metric_name
$func$
LANGUAGE sql VOLATILE PARALLEL SAFE;

-- Public function to get the name of the table for a given metric
-- This will create the metric table if it does not yet exist.
CREATE OR REPLACE FUNCTION SCHEMA_PROM.get_or_create_metric_table_name(
        metric_name text)
    RETURNS TABLE (id int, table_name name)
AS $func$
   SELECT id, table_name::name
   FROM SCHEMA_CATALOG.metric m
   WHERE m.metric_name = get_or_create_metric_table_name.metric_name
   UNION ALL
   SELECT *
   FROM SCHEMA_CATALOG.create_metric_table(get_or_create_metric_table_name.metric_name)
   LIMIT 1
$func$
LANGUAGE sql VOLATILE PARALLEL SAFE;

--public function to get the array position for a label key
CREATE OR REPLACE FUNCTION SCHEMA_PROM.get_label_key_pos(
        metric_name text, key_name text)
    RETURNS INT
AS $$
    --only executes the more expensive PLPGSQL function if the label doesn't exist
    SELECT
        pos
    FROM
        SCHEMA_CATALOG.label_key_position
    WHERE
        metric = metric_name
        AND KEY = key_name
    UNION ALL
    SELECT
        SCHEMA_CATALOG.get_new_pos_for_key(metric_name, key_name)
    LIMIT 1
$$
LANGUAGE SQL;

--Get the label_id for a key, value pair
CREATE OR REPLACE FUNCTION SCHEMA_PROM.get_label_id(
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
LANGUAGE SQL;

--This generates a position based array from the jsonb
--0s represent keys that are not set (we don't use NULL
--since intarray does not support it).
--This is not super performance critical since this
--is only used on the insert client and is cached there.
CREATE OR REPLACE FUNCTION SCHEMA_PROM.jsonb_to_label_array(js jsonb)
RETURNS INT[] AS $$
    WITH idx_val AS (
        SELECT
            -- only call the functions to create new key positions
            -- and label ids if they don't exist (for performance reasons)
            coalesce(lkp.pos,
              SCHEMA_PROM.get_label_key_pos(js->>'__name__', e.key)) idx,
            coalesce(l.id,
              SCHEMA_PROM.get_label_id(e.key, e.value)) val
        FROM SCHEMA_CATALOG.label_jsonb_each_text(js) e
             LEFT JOIN SCHEMA_CATALOG.label l
               ON (l.key = e.key AND l.value = e.value)
            LEFT JOIN SCHEMA_CATALOG.label_key_position lkp
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

CREATE OR REPLACE FUNCTION SCHEMA_PROM.key_value_array_to_label_array(metric_name TEXT, label_keys text[], label_values text[])
RETURNS INT[] AS $$
    WITH idx_val AS (
        SELECT
            -- only call the functions to create new key positions
            -- and label ids if they don't exist (for performance reasons)
            coalesce(lkp.pos,
              SCHEMA_PROM.get_label_key_pos(metric_name, kv.key)) idx,
            coalesce(l.id,
              SCHEMA_PROM.get_label_id(kv.key, kv.value)) val
        FROM ROWS FROM(unnest(label_keys), UNNEST(label_values)) AS kv(key, value)
            LEFT JOIN SCHEMA_CATALOG.label l
               ON (l.key = kv.key AND l.value = kv.value)
            LEFT JOIN SCHEMA_CATALOG.label_key_position lkp
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

-- Returns keys and values for a label_array
-- This function needs to be optimized for performance
CREATE OR REPLACE FUNCTION SCHEMA_PROM.label_array_to_key_value_array(labels int[], OUT keys text[], OUT vals text[])
AS $$
    SELECT
        array_agg(l.key), array_agg(l.value)
    FROM
      SCHEMA_CATALOG.label_unnest(labels) label_id
      INNER JOIN SCHEMA_CATALOG.label l ON (l.id = label_id)
$$
LANGUAGE SQL STABLE PARALLEL SAFE;

--Returns the jsonb for a series defined by a label_array
CREATE OR REPLACE FUNCTION SCHEMA_PROM.label_array_to_jsonb(labels int[])
RETURNS jsonb AS $$
    SELECT
        jsonb_object(keys, vals)
    FROM
      SCHEMA_PROM.label_array_to_key_value_array(labels)
$$
LANGUAGE SQL STABLE PARALLEL SAFE;

--Do not call before checking that the series does not yet exist
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.create_series(
        metric_id int,
        label_array int[],
        OUT series_id BIGINT)
AS $func$
BEGIN
LOOP
    INSERT INTO SCHEMA_CATALOG.series(metric_id, labels)
    SELECT metric_id, label_array
    ON CONFLICT DO NOTHING
    RETURNING id
    INTO series_id;

    EXIT WHEN FOUND;

    SELECT id
    INTO series_id
    FROM SCHEMA_CATALOG.series
    WHERE labels = label_array;

    EXIT WHEN FOUND;
END LOOP;
END
$func$
LANGUAGE PLPGSQL VOLATILE PARALLEL SAFE;

CREATE OR REPLACE  FUNCTION SCHEMA_PROM.get_series_id_for_label(label jsonb)
RETURNS BIGINT AS $$
   WITH CTE AS (
       SELECT SCHEMA_PROM.jsonb_to_label_array(label)
   )
   SELECT id
   FROM SCHEMA_CATALOG.series
   WHERE labels = (SELECT * FROM cte)
   UNION ALL
   SELECT SCHEMA_CATALOG.create_series((SCHEMA_PROM.get_or_create_metric_table_name(label->>'__name__')).id, (SELECT * FROM cte))
   LIMIT 1
$$
LANGUAGE SQL VOLATILE PARALLEL SAFE;

CREATE OR REPLACE  FUNCTION SCHEMA_PROM.get_series_id_for_key_value_array(metric_name TEXT, label_keys text[], label_values text[])
RETURNS BIGINT AS $$
   WITH CTE AS (
       SELECT SCHEMA_PROM.key_value_array_to_label_array(metric_name, label_keys, label_values)
   )
   SELECT id
   FROM SCHEMA_CATALOG.series
   WHERE labels = (SELECT * FROM cte)
   UNION ALL
   SELECT SCHEMA_CATALOG.create_series((SCHEMA_PROM.get_or_create_metric_table_name(metric_name)).id, (SELECT * FROM cte))
   LIMIT 1
$$
LANGUAGE SQL VOLATILE PARALLEL SAFE;

--
-- Parameter manipulation functions
--

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.set_chunk_interval_on_metric_table(metric_name TEXT, new_interval INTERVAL)
RETURNS void
AS $func$
    --set interval while addeing 1% of randomness to the interval so that chunks are not aligned so that
    --chunks are staggered for compression jobs.
    SELECT set_chunk_time_interval(
        format('SCHEMA_PROM.%I',(SELECT table_name FROM SCHEMA_PROM.get_or_create_metric_table_name(metric_name)))::regclass,
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
LANGUAGE sql VOLATILE;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.set_metric_chunk_interval(metric_name TEXT, chunk_interval INTERVAL)
RETURNS BOOLEAN
AS $func$
    --use get_or_create_metric_table_name because we want to be able to set /before/ any data is ingested
    --needs to run before update so row exists before update.
    SELECT SCHEMA_PROM.get_or_create_metric_table_name(set_metric_chunk_interval.metric_name);

    UPDATE SCHEMA_CATALOG.metric SET default_chunk_interval = false
    WHERE id IN (SELECT id FROM SCHEMA_PROM.get_metric_table_name_if_exists(set_metric_chunk_interval.metric_name));

    SELECT SCHEMA_CATALOG.set_chunk_interval_on_metric_table(metric_name, chunk_interval);

    SELECT true;
$func$
LANGUAGE SQL VOLATILE;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.reset_metric_chunk_interval(metric_name TEXT)
RETURNS BOOLEAN
AS $func$
    UPDATE SCHEMA_CATALOG.metric SET default_chunk_interval = true
    WHERE id = (SELECT id FROM SCHEMA_PROM.get_metric_table_name_if_exists(metric_name));

    SELECT SCHEMA_CATALOG.set_chunk_interval_on_metric_table(metric_name,
        SCHEMA_CATALOG.get_default_chunk_interval());

    SELECT true;
$func$
LANGUAGE SQL VOLATILE;


CREATE OR REPLACE FUNCTION SCHEMA_PROM.get_metric_retention_period(metric_name TEXT)
RETURNS INTERVAL
AS $$
    SELECT COALESCE(m.retention_period, SCHEMA_CATALOG.get_default_retention_period())
    FROM SCHEMA_CATALOG.metric m
    WHERE id IN (SELECT id FROM SCHEMA_PROM.get_metric_table_name_if_exists(get_metric_retention_period.metric_name))
    UNION ALL
    SELECT SCHEMA_CATALOG.get_default_retention_period()
    LIMIT 1
$$
LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.set_default_retention_period(retention_period INTERVAL)
RETURNS BOOLEAN
AS $$
    INSERT INTO SCHEMA_CATALOG.default(key, value) VALUES('retention_period', retention_period::text)
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
    SELECT true;
$$
LANGUAGE sql VOLATILE;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.set_metric_retention_period(metric_name TEXT, new_retention_period INTERVAL)
RETURNS BOOLEAN
AS $func$
    --use get_or_create_metric_table_name because we want to be able to set /before/ any data is ingested
    --needs to run before update so row exists before update.
    SELECT SCHEMA_PROM.get_or_create_metric_table_name(set_metric_retention_period.metric_name);

    UPDATE SCHEMA_CATALOG.metric SET retention_period = new_retention_period
    WHERE id IN (SELECT id FROM SCHEMA_PROM.get_metric_table_name_if_exists(set_metric_retention_period.metric_name));

    SELECT true;
$func$
LANGUAGE SQL VOLATILE;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.reset_metric_retention_period(metric_name TEXT)
RETURNS BOOLEAN
AS $func$
    UPDATE SCHEMA_CATALOG.metric SET retention_period = NULL
    WHERE id = (SELECT id FROM SCHEMA_PROM.get_metric_table_name_if_exists(metric_name));
    SELECT true;
$func$
LANGUAGE SQL VOLATILE;


--drop chunks from metrics tables and delete the appropriate series.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.drop_metric_chunks(metric_name TEXT, older_than TIMESTAMPTZ)
    RETURNS BOOLEAN
    AS $func$
DECLARE
    metric_table NAME;
    check_time TIMESTAMPTZ;
    older_than_chunk TIMESTAMPTZ;
    time_dimension_id INT;
    label_ids int[];
BEGIN
    SELECT table_name
    INTO STRICT metric_table
    FROM SCHEMA_PROM.get_or_create_metric_table_name(metric_name);

    SELECT older_than + INTERVAL '1 hour'
    INTO check_time;

    --Get the time dimension id for the time dimension
    SELECT d.id
    INTO STRICT time_dimension_id
    FROM _timescaledb_catalog.hypertable h
    INNER JOIN _timescaledb_catalog.dimension d ON (d.hypertable_id = h.id)
    WHERE h.schema_name = 'SCHEMA_PROM' AND h.table_name = metric_table
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
            FROM SCHEMA_PROM.%1$I
            WHERE time < %2$L
            EXCEPT
            SELECT distinct series_id
            FROM SCHEMA_PROM.%1$I
            WHERE time >= %2$L AND time < %3$L
        ), confirmed_drop_series AS (
            SELECT series_id
            FROM potentially_drop_series
            WHERE NOT EXISTS (
                 SELECT 1
                 FROM  SCHEMA_PROM.%1$I  data_exists
                 WHERE data_exists.series_id = potentially_drop_series.series_id AND time >= %3$L
                 --use chunk append + more likely to find something starting at earliest time
                 ORDER BY time ASC
                 LIMIT 1
            )
        ), deleted_series AS (
          DELETE from SCHEMA_CATALOG.series
          WHERE id IN (SELECT series_id FROM confirmed_drop_series)
          RETURNING id, labels
        )
        SELECT ARRAY(SELECT DISTINCT unnest(labels) as label_id
        FROM deleted_series)
    $query$, metric_table, older_than, check_time) INTO label_ids;

    --needs to be a separate query and not a CTE since this needs to "see"
    --the series rows deleted above as deleted.
    EXECUTE $query$
    WITH confirmed_drop_labels AS (
            SELECT label_id
            FROM unnest($1) as labels(label_id)
            WHERE NOT EXISTS (
                 SELECT 1
                 FROM  SCHEMA_CATALOG.series series_exists
                 WHERE series_exists.labels && ARRAY[labels.label_id]
                 LIMIT 1
            )
        )
        DELETE FROM SCHEMA_CATALOG.label
        WHERE id IN (SELECT * FROM confirmed_drop_labels);
    $query$ USING label_ids;

   PERFORM drop_chunks(table_name=>metric_table, schema_name=> 'SCHEMA_PROM', older_than=>older_than);
   RETURN true;
END
$func$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_metrics_that_need_drop_chunk()
RETURNS SETOF SCHEMA_CATALOG.metric
AS $$
        SELECT m.*
        FROM SCHEMA_CATALOG.metric m
        WHERE EXISTS (
            SELECT 1 FROM
            show_chunks(hypertable=>format('%I.%I', 'SCHEMA_PROM', m.table_name),
                         older_than=>NOW() - SCHEMA_PROM.get_metric_retention_period(m.metric_name)))
        --random order also to prevent starvation
        ORDER BY random()
$$
LANGUAGE sql STABLE;


--public procedure to be called by cron
CREATE PROCEDURE SCHEMA_PROM.drop_chunks()
LANGUAGE plpgsql
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

        PERFORM SCHEMA_CATALOG.drop_metric_chunks(r.metric_name, NOW() - SCHEMA_PROM.get_metric_retention_period(r.metric_name));
        COMMIT;
    END LOOP;

    FOR r IN
        SELECT *
        FROM SCHEMA_CATALOG.get_metrics_that_need_drop_chunk()
    LOOP
        PERFORM SCHEMA_CATALOG.drop_metric_chunks(r.metric_name, NOW() - SCHEMA_PROM.get_metric_retention_period(r.metric_name));
        COMMIT;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.is_stale_marker(value double precision)
RETURNS BOOLEAN
AS $func$
    SELECT float8send(value) = '\x7ff0000000000002'
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.is_normal_nan(value double precision)
RETURNS BOOLEAN
AS $func$
    SELECT float8send(value) = '\x7ff8000000000001'
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.get_label_value(
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
LANGUAGE PLPGSQL;

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
            format ('SCHEMA_PROM.get_label_value(series.labels[%s]) AS %I',pos::int, SCHEMA_CATALOG.get_label_key_column_name_for_view(key, false))
        , ', ' ORDER BY pos)
    INTO STRICT label_value_cols
    FROM SCHEMA_CATALOG.label_key_position lkp
    WHERE lkp.metric = metric_name and key != '__name__';

    SELECT m.table_name, m.id
    INTO STRICT view_name, metric_id
    FROM SCHEMA_CATALOG.metric m
    WHERE m.metric_name = create_series_view.metric_name;

    EXECUTE FORMAT($$
        CREATE OR REPLACE VIEW SCHEMA_SERIES.%I AS
        SELECT
            id AS series_id,
            labels
            %s
        FROM
            SCHEMA_CATALOG.series
        WHERE metric_id = %L
    $$, view_name, label_value_cols, metric_id);
    RETURN true;
END
$func$
LANGUAGE PLPGSQL;


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
    WHERE lkp.metric = metric_name and key != '__name__';

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
            SCHEMA_PROM.%1$I AS data
            LEFT JOIN SCHEMA_CATALOG.series AS series ON (series.id = data.series_id AND series.metric_id = %3$L)
    $$, table_name, label_value_cols, metric_id);
    RETURN true;
END
$func$
LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.labels_equal(labels1 int[], labels2 int[])
RETURNS BOOLEAN
AS $func$
    --assumes no duplicate entries
    SELECT array_length(labels1, 1) = array_length(labels2, 1) AND labels1 @> labels2
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
COMMENT ON FUNCTION SCHEMA_PROM.labels_equal(int[], int[]) IS 'returns true if two label arrays are equal, including the metric name';


CREATE OR REPLACE FUNCTION SCHEMA_PROM.labels_equal_across_metrics(labels1 int[], labels2 int[])
RETURNS BOOLEAN
AS $func$
    --assumes labels have metric name in position 1 and have no duplicate entries
    SELECT array_length(labels1, 1) = array_length(labels2, 1) AND labels1 @> labels2[2:]
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
COMMENT ON FUNCTION SCHEMA_PROM.labels_equal_across_metrics(int[], int[]) IS 'returns true if two label arrays are equal, ignoring the metric name';

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_matcher_get_from_json(labels jsonb)
RETURNS int[]
AS $func$
    SELECT ARRAY(
           SELECT coalesce(l.id, -1) -- -1 indicates no such label
           FROM SCHEMA_CATALOG.label_jsonb_each_text(labels) e
           LEFT JOIN SCHEMA_CATALOG.label l
               ON (l.key = e.key AND l.value = e.value)
        )
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
COMMENT ON FUNCTION SCHEMA_CATALOG.label_matcher_get_from_json(jsonb)
IS 'returns an array of label ids for the JSONB. This is not a labels array since the order of ids isnt guaranteed.';


CREATE OR REPLACE FUNCTION SCHEMA_PROM.labels_contains(labels int[], partial_labels jsonb)
RETURNS BOOLEAN
AS $func$
    --keep as a simple statement that calls internal function so that planner
    --could expand this into query
    SELECT labels @> SCHEMA_CATALOG.label_matcher_get_from_json(partial_labels)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
COMMENT ON FUNCTION SCHEMA_PROM.labels_contains(int[], int[])
IS 'returns true if the labels array contains the labels inside the JSONB';
