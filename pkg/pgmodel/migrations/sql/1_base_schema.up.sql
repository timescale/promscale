--NOTES
--This code assumes that table names can only be 63 chars long

CREATE SCHEMA _prom_catalog;
CREATE SCHEMA _prom_internal;
CREATE SCHEMA prom; --data tables go here

-----------------------
-- Table definitions --
-----------------------

CREATE TABLE _prom_catalog.series (
    id serial,
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
    UNIQUE (metric_name) INCLUDE (table_name),
    UNIQUE(table_name)
);

CREATE OR REPLACE FUNCTION _prom_internal.make_metric_table()
    RETURNS trigger
    AS $func$
DECLARE
BEGIN
   EXECUTE format('CREATE TABLE prom.%I(time TIMESTAMPTZ, value DOUBLE PRECISION, series_id INT)',
                    NEW.table_name);
   EXECUTE format('CREATE INDEX ON prom.%I (series_id, time) INCLUDE (value)',
                    NEW.table_name);
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
        metric_name_arg text)
    RETURNS TABLE(id int, table_name name)
AS $func$
    WITH new_id AS (
        SELECT nextval(pg_get_serial_sequence('_prom_catalog.metric','id'))::int as id
    ),
    ins AS (
        INSERT INTO _prom_catalog.metric (id, metric_name, table_name)
        SELECT new_id.id,
            metric_name_arg, 
            _prom_internal.new_metric_table_name(metric_name_arg, new_id.id)
        FROM new_id
        RETURNING id, table_name
   )
   SELECT id, table_name FROM ins
   UNION ALL
   --the following select is necessary to guarantee a return
   --when this is executed concurrently
   SELECT id, table_name 
   FROM _prom_catalog.metric
   WHERE metric_name = metric_name_arg 
   LIMIT 1
$func$
LANGUAGE sql VOLATILE PARALLEL SAFE;

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

CREATE OR REPLACE FUNCTION _prom_internal.get_new_label_id(key_name text, value_name text)
RETURNS INT AS $$
    WITH ins AS (
        INSERT INTO 
            _prom_catalog.label(key, value)
        VALUES 
            (key_name,value_name)
        ON CONFLICT DO NOTHING
        RETURNING id
    )
    SELECT id FROM ins 
    UNION ALL 
    --this select necessary in case of concurrent
    --insert where the insert does not return an id
    --since ON CONFLICT DO NOTHING does not return if 
    --item already exists
    SELECT 
        id
    FROM _prom_catalog.label
    WHERE 
        key = key_name AND 
        value = value_name
    LIMIT 1
$$
LANGUAGE SQL;

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
        metric_name_arg text)
    RETURNS TABLE (id int, table_name name)
AS $func$
   SELECT id, table_name::name 
   FROM _prom_catalog.metric
   WHERE metric_name = metric_name_arg
   UNION ALL
   SELECT * 
   FROM _prom_internal.create_metric_table(metric_name_arg)
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
        label_array int[])
    RETURNS int
AS $func$
    WITH ins AS (
        INSERT INTO _prom_catalog.series(metric_id, labels)
        SELECT metric_id, label_array
        RETURNING id
   )
   SELECT id FROM ins
   UNION ALL
   --the following select is necessary to guarantee a return
   --when this is executed concurrently
   SELECT id
   FROM _prom_catalog.series
   WHERE labels = label_array 
   LIMIT 1
$func$
LANGUAGE sql VOLATILE PARALLEL SAFE;

CREATE OR REPLACE  FUNCTION get_series_id_for_label(label jsonb)
RETURNS INT AS $$
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