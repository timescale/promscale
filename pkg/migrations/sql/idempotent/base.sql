--NOTES
--This code assumes that table names can only be 63 chars long


CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_default_chunk_interval()
    RETURNS INTERVAL
AS $func$
    SELECT value::INTERVAL FROM SCHEMA_CATALOG.default WHERE key='chunk_interval';
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_default_chunk_interval() TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_timescale_major_version()
    RETURNS INT
AS $func$
    SELECT split_part(extversion, '.', 1)::INT FROM pg_catalog.pg_extension WHERE extname='timescaledb' LIMIT 1;
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_timescale_major_version() TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_timescale_minor_version()
    RETURNS INT
AS $func$
    SELECT split_part(extversion, '.', 2)::INT FROM pg_catalog.pg_extension WHERE extname='timescaledb' LIMIT 1;
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_timescale_minor_version() TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_default_retention_period()
    RETURNS INTERVAL
AS $func$
    SELECT value::INTERVAL FROM SCHEMA_CATALOG.default WHERE key='retention_period';
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_default_retention_period() TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.is_timescaledb_installed()
RETURNS BOOLEAN
AS $func$
    SELECT count(*) > 0 FROM pg_extension WHERE extname='timescaledb';
$func$
LANGUAGE SQL STABLE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.is_timescaledb_installed() TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.is_timescaledb_oss()
RETURNS BOOLEAN AS
$$
BEGIN
    IF SCHEMA_CATALOG.is_timescaledb_installed() THEN
        IF SCHEMA_CATALOG.get_timescale_major_version() >= 2 THEN
            -- TimescaleDB 2.x
            RETURN (SELECT current_setting('timescaledb.license') = 'apache');
        ELSE
            -- TimescaleDB 1.x
            -- Note: We cannot use current_setting() in 1.x, otherwise we get permission errors as
            -- we need to be superuser. We should not enforce the use of superuser. Hence, we take
            -- help of a view.
            RETURN (SELECT edition = 'apache' FROM timescaledb_information.license);
        END IF;
    END IF;
    RETURN false;
END;
$$
LANGUAGE plpgsql;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.is_timescaledb_oss() TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.is_multinode()
    RETURNS BOOLEAN
AS $func$
DECLARE
    is_distributed BOOLEAN = false;
BEGIN
    IF SCHEMA_CATALOG.get_timescale_major_version() >= 2 THEN
        SELECT count(*) > 0 FROM timescaledb_information.data_nodes
            INTO is_distributed;
    END IF;
    RETURN is_distributed;
EXCEPTION WHEN SQLSTATE '42P01' THEN -- Timescale 1.x, never distributed
    RETURN false;
END
$func$
LANGUAGE PLPGSQL STABLE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.is_multinode() TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_default_compression_setting()
    RETURNS BOOLEAN
AS $func$
    SELECT value::BOOLEAN FROM SCHEMA_CATALOG.default WHERE key='metric_compression';
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_default_compression_setting() TO prom_reader;

--Add 1% of randomness to the interval so that chunks are not aligned so that chunks are staggered for compression jobs.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_staggered_chunk_interval(chunk_interval INTERVAL)
RETURNS INTERVAL
AS $func$
    SELECT chunk_interval * (1.0+((random()*0.01)-0.005));
$func$
LANGUAGE SQL VOLATILE;
--only used for setting chunk interval, and admin function
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_staggered_chunk_interval(INTERVAL) TO prom_admin;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.lock_metric_for_maintenance(metric_id int, wait boolean = true)
    RETURNS BOOLEAN
AS $func$
DECLARE
    res BOOLEAN;
BEGIN
    IF NOT wait THEN
        SELECT pg_try_advisory_lock(ADVISORY_LOCK_PREFIX_MAINTENACE, metric_id) INTO STRICT res;

        RETURN res;
    ELSE
        PERFORM pg_advisory_lock(ADVISORY_LOCK_PREFIX_MAINTENACE, metric_id);

        RETURN TRUE;
    END IF;
END
$func$
LANGUAGE PLPGSQL VOLATILE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.lock_metric_for_maintenance(int, boolean) TO prom_maintenance;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.unlock_metric_for_maintenance(metric_id int)
    RETURNS VOID
AS $func$
DECLARE
BEGIN
    PERFORM pg_advisory_unlock(ADVISORY_LOCK_PREFIX_MAINTENACE, metric_id);
END
$func$
LANGUAGE PLPGSQL VOLATILE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.unlock_metric_for_maintenance(int) TO prom_maintenance;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.attach_series_partition(metric_record SCHEMA_CATALOG.metric) RETURNS VOID
AS $proc$
DECLARE
BEGIN
        EXECUTE format($$
           ALTER TABLE SCHEMA_CATALOG.series ATTACH PARTITION SCHEMA_DATA_SERIES.%1$I FOR VALUES IN (%2$L)
        $$, metric_record.table_name, metric_record.id);
END;
$proc$
LANGUAGE PLPGSQL
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON FUNCTION SCHEMA_CATALOG.attach_series_partition(SCHEMA_CATALOG.metric) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.attach_series_partition(SCHEMA_CATALOG.metric) TO prom_writer;

--Canonical lock ordering:
--metrics
--data table
--labels
--series parent
--series partition

--constraints:
--- The prom_metric view takes locks in the order: data table, series partition.


--This procedure finalizes the creation of a metric. The first part of
--metric creation happens in make_metric_table and the final part happens here.
--We split metric creation into two parts to minimize latency during insertion
--(which happens in the make_metric_table path). Especially noteworthy is that
--attaching the partition to the series table happens here because it requires
--an exclusive lock, which is a high-latency operation. The other actions this
--function does are not as critical latency-wise but are also not necessary
--to perform in order to insert data and thus are put here.
--
--Note: that a consequence of this design is that the series partition is attached
--to the series parent after in this step. Thus a metric might not be seen in some
--cross-metric queries right away. Those queries aren't common however and the delay
--is insignificant in practice.
--
--lock-order: metric table, data_table, series parent, series partition

CREATE OR REPLACE PROCEDURE SCHEMA_CATALOG.finalize_metric_creation()
AS $proc$
DECLARE
    r SCHEMA_CATALOG.metric;
    created boolean;
    is_view boolean;
BEGIN
    FOR r IN
        SELECT *
        FROM SCHEMA_CATALOG.metric
        WHERE NOT creation_completed
        ORDER BY random()
    LOOP
        SELECT m.creation_completed, m.is_view
        INTO created, is_view
        FROM SCHEMA_CATALOG.metric m
        WHERE m.id = r.id
        FOR UPDATE;

        IF created THEN
            --release row lock
            COMMIT;
            CONTINUE;
        END IF;

        --do this before taking exclusive lock to minimize work after taking lock
        UPDATE SCHEMA_CATALOG.metric SET creation_completed = TRUE WHERE id = r.id;

        -- in case of a view, no need to attach the partition
        IF is_view THEN
            --release row lock
            COMMIT;
            CONTINUE;
        END IF;

        --we will need this lock for attaching the partition so take it now
        --This may not be strictly necessary but good
        --to enforce lock ordering (parent->child) explicitly. Note:
        --creating a table as a partition takes a stronger lock (access exclusive)
        --so, attaching a partition is better
        LOCK TABLE ONLY SCHEMA_CATALOG.series IN SHARE UPDATE EXCLUSIVE mode;

        PERFORM SCHEMA_CATALOG.attach_series_partition(r);

        COMMIT;
    END LOOP;
END;
$proc$ LANGUAGE PLPGSQL;
COMMENT ON PROCEDURE SCHEMA_CATALOG.finalize_metric_creation()
IS 'Finalizes metric creation. This procedure should be run by the connector automatically';
GRANT EXECUTE ON PROCEDURE SCHEMA_CATALOG.finalize_metric_creation() TO prom_writer;

--This function is called by a trigger when a new metric is created. It
--sets up the metric just enough to insert data into it. Metric creation
--is completed in finalize_metric_creation() above. See the comments
--on that function for the reasoning for this split design.
--
--Note: latency-sensitive function. Should only contain just enough logic
--to support inserts for the metric.
--lock-order: data table, labels, series partition.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.make_metric_table()
    RETURNS trigger
    AS $func$
DECLARE
  label_id INT;
  compressed_hypertable_name text;
BEGIN

   -- Note: if the inserted metric is a view, nothing to do.
   IF NEW.is_view THEN
        RETURN NEW;
   END IF;

   EXECUTE format('CREATE TABLE %I.%I(time TIMESTAMPTZ NOT NULL, value DOUBLE PRECISION NOT NULL, series_id BIGINT NOT NULL) WITH (autovacuum_vacuum_threshold = 50000, autovacuum_analyze_threshold = 50000)',
                    NEW.table_schema, NEW.table_name);
   EXECUTE format('GRANT SELECT ON TABLE %I.%I TO prom_reader', NEW.table_schema, NEW.table_name);
   EXECUTE format('GRANT SELECT, INSERT ON TABLE %I.%I TO prom_writer', NEW.table_schema, NEW.table_name);
   EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE %I.%I TO prom_modifier', NEW.table_schema, NEW.table_name);
   EXECUTE format('CREATE UNIQUE INDEX data_series_id_time_%s ON %I.%I (series_id, time) INCLUDE (value)',
                    NEW.id, NEW.table_schema, NEW.table_name);

    IF SCHEMA_CATALOG.is_timescaledb_installed() THEN
        IF SCHEMA_CATALOG.is_multinode() THEN
            --Note: we intentionally do not partition by series_id here. The assumption is
            --that we'll have more "heavy metrics" than nodes and thus partitioning /individual/
            --metrics won't gain us much for inserts and would be detrimental for many queries.
            PERFORM SCHEMA_TIMESCALE.create_distributed_hypertable(
                format('%I.%I', NEW.table_schema, NEW.table_name),
                'time',
                chunk_time_interval=>SCHEMA_CATALOG.get_staggered_chunk_interval(SCHEMA_CATALOG.get_default_chunk_interval()),
                create_default_indexes=>false
            );
        ELSE
            PERFORM SCHEMA_TIMESCALE.create_hypertable(format('%I.%I', NEW.table_schema, NEW.table_name), 'time',
            chunk_time_interval=>SCHEMA_CATALOG.get_staggered_chunk_interval(SCHEMA_CATALOG.get_default_chunk_interval()),
                             create_default_indexes=>false);
        END IF;
    END IF;

    --Do not move this into the finalize step, because it's cheap to do while the table is empty
    --but takes a heavyweight blocking lock otherwise.
    IF  SCHEMA_CATALOG.is_timescaledb_installed()
            AND SCHEMA_CATALOG.get_default_compression_setting() THEN
            PERFORM SCHEMA_PROM.set_compression_on_metric_table(NEW.table_name, TRUE);
    END IF;


    SELECT SCHEMA_CATALOG.get_or_create_label_id('__name__', NEW.metric_name)
    INTO STRICT label_id;
    --note that because labels[1] is unique across partitions and UNIQUE(labels) inside partition, labels are guaranteed globally unique
    EXECUTE format($$
        CREATE TABLE SCHEMA_DATA_SERIES.%1$I (
            id bigint NOT NULL,
            metric_id int NOT NULL,
            labels SCHEMA_PROM.label_array NOT NULL,
            delete_epoch BIGINT NULL DEFAULT NULL,
            CHECK(labels[1] = %2$L AND labels[1] IS NOT NULL),
            CHECK(metric_id = %3$L),
            CONSTRAINT series_labels_id_%3$s UNIQUE(labels) INCLUDE (id),
            CONSTRAINT series_pkey_%3$s PRIMARY KEY(id)
        ) WITH (autovacuum_vacuum_threshold = 100, autovacuum_analyze_threshold = 100)
    $$, NEW.table_name, label_id, NEW.id);
   EXECUTE format('GRANT SELECT ON TABLE SCHEMA_DATA_SERIES.%I TO prom_reader', NEW.table_name);
   EXECUTE format('GRANT SELECT, INSERT ON TABLE SCHEMA_DATA_SERIES.%I TO prom_writer', NEW.table_name);
   EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_DATA_SERIES.%I TO prom_modifier', NEW.table_name);
   RETURN NEW;
END
$func$
LANGUAGE PLPGSQL VOLATILE
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON FUNCTION SCHEMA_CATALOG.make_metric_table() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.make_metric_table() TO prom_writer;

DROP TRIGGER IF EXISTS make_metric_table_trigger ON SCHEMA_CATALOG.metric CASCADE;
CREATE TRIGGER make_metric_table_trigger
    AFTER INSERT ON SCHEMA_CATALOG.metric
    FOR EACH ROW
    EXECUTE PROCEDURE SCHEMA_CATALOG.make_metric_table();


------------------------
-- Internal functions --
------------------------

-- Return a table name built from a full_name and a suffix.
-- The full name is truncated so that the suffix could fit in full.
-- name size will always be exactly 62 chars.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.pg_name_with_suffix(
        full_name text, suffix text)
    RETURNS name
AS $func$
    SELECT (substring(full_name for 62-(char_length(suffix)+1)) || '_' || suffix)::name
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.pg_name_with_suffix(text, text) TO prom_reader;

-- Return a new unique name from a name and id.
-- This tries to use the full_name in full. But if the
-- full name doesn't fit, generates a new unique name.
-- Note that there cannot be a collision betweeen a user
-- defined name and a name with a suffix because user
-- defined names of length 62 always get a suffix and
-- conversely, all names with a suffix are length 62.

-- We use a max name length of 62 not 63 because table creation creates an
-- array type named `_tablename`. We need to ensure that this name is
-- unique as well, so have to reserve a space for the underscore.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.pg_name_unique(
        full_name_arg text, suffix text)
    RETURNS name
AS $func$
    SELECT CASE
        WHEN char_length(full_name_arg) < 62 THEN
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
-- locks: metric, make_metric_table[data table, labels, series partition]
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.create_metric_table(
        metric_name_arg text, OUT id int, OUT table_name name)
AS $func$
DECLARE
  new_id int;
  new_table_name name;
BEGIN
new_id = nextval(pg_get_serial_sequence('SCHEMA_CATALOG.metric','id'))::int;
new_table_name = SCHEMA_CATALOG.pg_name_unique(metric_name_arg, new_id::text);
LOOP
    INSERT INTO SCHEMA_CATALOG.metric (id, metric_name, table_schema, table_name, series_table)
        SELECT  new_id,
                metric_name_arg,
                'SCHEMA_DATA',
                new_table_name,
                new_table_name
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
-- locks: label_key_position, data table, series partition (in view creation),
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_new_pos_for_key(
        metric_name text, key_name_array text[], is_for_exemplar boolean)
    RETURNS int[]
AS $func$
DECLARE
    position int;
    position_array int[];
    position_array_idx int;
    count_new int;
    key_name text;
    next_position int;
    max_position int;
    metric_table name;
    position_table_name text;
BEGIN
    If is_for_exemplar THEN
        position_table_name := 'exemplar_label_key_position';
    ELSE
        position_table_name := 'label_key_position';
    END IF;

    --Use double check locking here
    --fist optimistic check:
    position_array := NULL;
    EXECUTE FORMAT('SELECT array_agg(p.pos ORDER BY k.ord)
        FROM
            unnest($1) WITH ORDINALITY as k(key, ord)
        INNER JOIN
            SCHEMA_CATALOG.%I p ON
        (
            p.metric_name = $2
            AND p.key = k.key
        )', position_table_name) USING key_name_array, metric_name
    INTO position_array;

    -- Return the array if the length is same, as the received key_name_array does not contain any new keys.
    IF array_length(key_name_array, 1) = array_length(position_array, 1) THEN
        RETURN position_array;
    END IF;

    -- Lock tables for exclusiveness.
    IF NOT is_for_exemplar THEN
        SELECT table_name
        FROM SCHEMA_CATALOG.get_or_create_metric_table_name(get_new_pos_for_key.metric_name)
        INTO metric_table;
        --lock as for ALTER TABLE because we are in effect changing the schema here
        --also makes sure the next_position below is correct in terms of concurrency
        EXECUTE format('LOCK TABLE prom_data_series.%I IN SHARE UPDATE EXCLUSIVE MODE', metric_table);
    ELSE
        LOCK TABLE SCHEMA_CATALOG.exemplar_label_key_position IN ACCESS EXCLUSIVE MODE;
    END IF;

    max_position := NULL;
    EXECUTE FORMAT('SELECT max(pos) + 1
        FROM SCHEMA_CATALOG.%I
    WHERE metric_name = $1', position_table_name) USING get_new_pos_for_key.metric_name
    INTO max_position;

    IF max_position IS NULL THEN
        IF is_for_exemplar THEN
            max_position := 1;
        ELSE
            -- Specific to label_key_position table only.
            max_position := 2; -- element 1 reserved for __name__
        END IF;
    END IF;

    position_array := array[]::int[];
    position_array_idx := 1;
    count_new := 0;
    FOREACH key_name IN ARRAY key_name_array LOOP
        --second check after lock
        position := NULL;
        EXECUTE FORMAT('SELECT pos FROM SCHEMA_CATALOG.%I lp
        WHERE
            lp.metric_name = $1
        AND
            lp.key = $2', position_table_name) USING metric_name, key_name
        INTO position;

        IF position IS NOT NULL THEN
            position_array[position_array_idx] := position;
            position_array_idx := position_array_idx + 1;
            CONTINUE;
        END IF;
        -- key_name does not exists in the position table.
        count_new := count_new + 1;
        IF (NOT is_for_exemplar) AND (key_name = '__name__') THEN
            next_position := 1; -- 1-indexed arrays, __name__ as first element
        ELSE
            next_position := max_position;
            max_position := max_position + 1;
        END IF;

        IF NOT is_for_exemplar THEN
            PERFORM SCHEMA_CATALOG.get_or_create_label_key(key_name);
        END IF;

        position := NULL;
        EXECUTE FORMAT('INSERT INTO SCHEMA_CATALOG.%I
        VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
        RETURNING pos', position_table_name) USING metric_name, key_name, next_position
        INTO position;

        IF position IS NULL THEN
            RAISE 'Could not find a new position. (is_for_exemplar=%)', is_for_exemplar;
        END IF;
        position_array[position_array_idx] := position;
        position_array_idx := position_array_idx + 1;
    END LOOP;

    IF NOT is_for_exemplar AND count_new > 0 THEN
        --note these functions are expensive in practice so they
        --must be run once across a collection of keys
        PERFORM SCHEMA_CATALOG.create_series_view(metric_name);
        PERFORM SCHEMA_CATALOG.create_metric_view(metric_name);
    END IF;

    RETURN position_array;
END
$func$
LANGUAGE PLPGSQL
--security definer needed to lock the series table
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON FUNCTION SCHEMA_CATALOG.get_new_pos_for_key(text, text[], boolean) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_new_pos_for_key(text, text[], boolean) TO prom_reader; -- For exemplars querying.
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_new_pos_for_key(text, text[], boolean) TO prom_writer;

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

-- safe_approximate_row_count returns the approximate row count of a hypertable if timescaledb is installed
-- else returns the approximate row count in the normal table. This prevents errors in approximate count calculation
-- if timescaledb is not installed, which is the case in plain postgres support.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.safe_approximate_row_count(table_name_input REGCLASS) RETURNS BIGINT
    LANGUAGE PLPGSQL
AS
$$
BEGIN
    IF SCHEMA_CATALOG.get_timescale_major_version() >= 2 THEN
        RETURN (SELECT * FROM approximate_row_count(table_name_input));
    ELSE
        IF SCHEMA_CATALOG.is_timescaledb_installed()
            AND (SELECT count(*) > 0
                FROM _timescaledb_catalog.hypertable
                WHERE format('%I.%I', schema_name, table_name)::regclass=table_name_input)
        THEN
                RETURN (SELECT row_estimate FROM hypertable_approximate_row_count(table_name_input));
        END IF;
        RETURN (SELECT reltuples::BIGINT FROM pg_class WHERE oid=table_name_input);
    END IF;
END;
$$;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.safe_approximate_row_count(regclass) to prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.delete_series_catalog_row(
    metric_table name,
    series_ids bigint[]
) RETURNS VOID AS
$$
BEGIN
    EXECUTE FORMAT(
        'UPDATE SCHEMA_DATA_SERIES.%1$I SET delete_epoch = current_epoch+1 FROM SCHEMA_CATALOG.ids_epoch WHERE delete_epoch IS NULL AND id = ANY($1)',
        metric_table
    ) USING series_ids;
    RETURN;
END;
$$
LANGUAGE PLPGSQL;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.delete_series_catalog_row(name, bigint[]) to prom_modifier;

---------------------------------------------------
------------------- Public APIs -------------------
---------------------------------------------------

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_metric_table_name_if_exists(
        schema text, metric_name text)
    RETURNS TABLE (id int, table_name name, table_schema name, series_table name, is_view boolean)
AS $func$
DECLARE
    rows_found bigint;
BEGIN
    IF get_metric_table_name_if_exists.schema != '' AND get_metric_table_name_if_exists.schema IS NOT NULL THEN
        RETURN QUERY SELECT m.id, m.table_name::name, m.table_schema::name, m.series_table::name, m.is_view
        FROM SCHEMA_CATALOG.metric m
        WHERE m.table_schema = get_metric_table_name_if_exists.schema
        AND m.metric_name = get_metric_table_name_if_exists.metric_name;
        RETURN;
    END IF;

    RETURN QUERY SELECT m.id, m.table_name::name, m.table_schema::name, m.series_table::name, m.is_view
    FROM SCHEMA_CATALOG.metric m
    WHERE m.table_schema = 'SCHEMA_DATA'
    AND m.metric_name = get_metric_table_name_if_exists.metric_name;

    IF FOUND THEN
        RETURN;
    END IF;

    SELECT count(*)
    INTO rows_found
    FROM SCHEMA_CATALOG.metric m
    WHERE m.metric_name = get_metric_table_name_if_exists.metric_name;

    IF rows_found <= 1 THEN
        RETURN QUERY SELECT m.id, m.table_name::name, m.table_schema::name, m.series_table::name, m.is_view
        FROM SCHEMA_CATALOG.metric m
        WHERE m.metric_name = get_metric_table_name_if_exists.metric_name;
        RETURN;
    END IF;

    RAISE EXCEPTION 'found multiple metrics with same name in different schemas, please specify exact schema name';
END
$func$
LANGUAGE PLPGSQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_metric_table_name_if_exists(text, text) to prom_reader;

-- Public function to get the name of the table for a given metric
-- This will create the metric table if it does not yet exist.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_or_create_metric_table_name(
        metric_name text, OUT id int, OUT table_name name, OUT possibly_new BOOLEAN)
AS $func$
   SELECT id, table_name::name, false
   FROM SCHEMA_CATALOG.metric m
   WHERE m.metric_name = get_or_create_metric_table_name.metric_name
   AND m.table_schema = 'SCHEMA_DATA'
   UNION ALL
   SELECT *, true
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
        (SCHEMA_CATALOG.get_new_pos_for_key(get_or_create_label_key_pos.metric_name, array[get_or_create_label_key_pos.key], false))[1]
    LIMIT 1
$$
LANGUAGE SQL VOLATILE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_or_create_label_key_pos(text, text) to prom_writer;

-- label_cardinality returns the cardinality of a label_pair id in the series table.
-- In simple terms, it means the number of times a label_pair/label_matcher is used
-- across all the series.
CREATE OR REPLACE FUNCTION SCHEMA_PROM.label_cardinality(label_id INT)
    RETURNS INT
    LANGUAGE SQL
AS
$$
    SELECT count(*)::INT FROM SCHEMA_CATALOG.series s WHERE s.labels @> array[label_id];
$$ STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.label_cardinality(int) to prom_reader;

--public function to get the array position for a label key if it exists
--useful in case users want to group by a specific label key
CREATE OR REPLACE FUNCTION SCHEMA_PROM.label_key_position(
        metric_name text, key text)
    RETURNS INT
AS $$
    SELECT
        pos
    FROM
        SCHEMA_CATALOG.label_key_position lkp
    WHERE
        lkp.metric_name = label_key_position.metric_name
        AND lkp.key = label_key_position.key
    LIMIT 1
$$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.label_key_position(text, text) to prom_reader;

-- drop_metric deletes a metric and related series hypertable from the database along with the related series, views and unreferenced labels.
CREATE OR REPLACE FUNCTION SCHEMA_PROM.drop_metric(metric_name_to_be_dropped text) RETURNS VOID
AS
$$
    DECLARE
        hypertable_name TEXT;
        deletable_metric_id INTEGER;
    BEGIN
        IF (SELECT NOT pg_try_advisory_xact_lock(SCHEMA_LOCK_ID)) THEN
            RAISE NOTICE 'drop_metric can run only when no Promscale connectors are running. Please shutdown the Promscale connectors';
            PERFORM pg_advisory_xact_lock(SCHEMA_LOCK_ID);
        END IF;
        SELECT table_name, id INTO hypertable_name, deletable_metric_id FROM SCHEMA_CATALOG.metric WHERE metric_name=metric_name_to_be_dropped;
        RAISE NOTICE 'deleting "%" metric with metric_id as "%" and table_name as "%"', metric_name_to_be_dropped, deletable_metric_id, hypertable_name;
        EXECUTE FORMAT('DROP VIEW SCHEMA_SERIES.%1$I;', hypertable_name);
        EXECUTE FORMAT('DROP VIEW SCHEMA_METRIC.%1$I;', hypertable_name);
        EXECUTE FORMAT('DROP TABLE SCHEMA_DATA_SERIES.%1$I;', hypertable_name);
        EXECUTE FORMAT('DROP TABLE SCHEMA_DATA.%1$I;', hypertable_name);
        DELETE FROM SCHEMA_CATALOG.metric WHERE id=deletable_metric_id;
        -- clean up unreferenced labels, label_keys and its position.
        DELETE FROM SCHEMA_CATALOG.label_key_position WHERE metric_name=metric_name_to_be_dropped;
        DELETE FROM SCHEMA_CATALOG.label_key WHERE key NOT IN (select key from SCHEMA_CATALOG.label_key_position);
    END;
$$
LANGUAGE plpgsql;

--Get the label_id for a key, value pair
-- no need for a get function only as users will not be using ids directly
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
--Read queries can use the eq function or others with the jsonb to find equality
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_or_create_label_array(js jsonb)
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
COMMENT ON FUNCTION SCHEMA_CATALOG.get_or_create_label_array(jsonb)
IS 'converts a jsonb to a label array';
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_or_create_label_array(jsonb) TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_or_create_label_array(metric_name TEXT, label_keys text[], label_values text[])
RETURNS SCHEMA_PROM.label_array AS $$
    WITH idx_val AS (
        SELECT
            -- only call the functions to create new key positions
            -- and label ids if they don't exist (for performance reasons)
            coalesce(lkp.pos,
              SCHEMA_CATALOG.get_or_create_label_key_pos(get_or_create_label_array.metric_name, kv.key)) idx,
            coalesce(l.id,
              SCHEMA_CATALOG.get_or_create_label_id(kv.key, kv.value)) val
        FROM ROWS FROM(unnest(label_keys), UNNEST(label_values)) AS kv(key, value)
            LEFT JOIN SCHEMA_CATALOG.label l
               ON (l.key = kv.key AND l.value = kv.value)
            LEFT JOIN SCHEMA_CATALOG.label_key_position lkp
               ON
               (
                  lkp.metric_name = get_or_create_label_array.metric_name AND
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
COMMENT ON FUNCTION SCHEMA_CATALOG.get_or_create_label_array(text, text[], text[])
IS 'converts a metric name, array of keys, and array of values to a label array';
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_or_create_label_array(TEXT, text[], text[]) TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_or_create_label_ids(metric_name TEXT, label_keys text[], label_values text[])
RETURNS TABLE(pos int[], id int[], label_key text[], label_value text[]) AS $$
        WITH cte as (
        SELECT
            -- only call the functions to create new key positions
            -- and label ids if they don't exist (for performance reasons)
            lkp.pos as known_pos,
            coalesce(l.id, SCHEMA_CATALOG.get_or_create_label_id(kv.key, kv.value)) label_id,
            kv.key key_str,
            kv.value val_str
        FROM ROWS FROM(unnest(label_keys), UNNEST(label_values)) AS kv(key, value)
            LEFT JOIN SCHEMA_CATALOG.label l
               ON (l.key = kv.key AND l.value = kv.value)
            LEFT JOIN SCHEMA_CATALOG.label_key_position lkp ON
            (
                    lkp.metric_name = get_or_create_label_ids.metric_name AND
                    lkp.key = kv.key
            )
        ORDER BY kv.key, kv.value
        )
        SELECT
           case when count(*) = count(known_pos) Then
              array_agg(known_pos)
           else
              SCHEMA_CATALOG.get_new_pos_for_key(get_or_create_label_ids.metric_name, array_agg(key_str), false)
           end as poss,
           array_agg(label_id) as label_ids,
           array_agg(key_str) as keys,
           array_agg(val_str) as vals
        FROM cte
$$
LANGUAGE SQL VOLATILE;
COMMENT ON FUNCTION SCHEMA_CATALOG.get_or_create_label_ids(text, text[], text[])
IS 'converts a metric name, array of keys, and array of values to a list of label ids';
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_or_create_label_ids(TEXT, text[], text[]) TO prom_writer;


-- Returns ids, keys and values for a label_array
-- the order may not be the same as the original labels
-- This function needs to be optimized for performance
CREATE OR REPLACE FUNCTION SCHEMA_PROM.labels_info(INOUT labels INT[], OUT keys text[], OUT vals text[])
AS $$
    SELECT
        array_agg(l.id), array_agg(l.key), array_agg(l.value)
    FROM
      SCHEMA_CATALOG.label_unnest(labels) label_id
      INNER JOIN SCHEMA_CATALOG.label l ON (l.id = label_id)
$$
LANGUAGE SQL STABLE PARALLEL SAFE;
COMMENT ON FUNCTION SCHEMA_PROM.labels_info(INT[])
IS 'converts an array of label ids to three arrays: one for ids, one for keys and another for values';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.labels_info(INT[]) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.key_value_array(labels SCHEMA_PROM.label_array, OUT keys text[], OUT vals text[])
AS $$
    SELECT keys, vals FROM SCHEMA_PROM.labels_info(labels)
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

--Returns the label_array given a series_id
CREATE OR REPLACE FUNCTION SCHEMA_PROM.labels(series_id BIGINT)
RETURNS SCHEMA_PROM.label_array AS $$
    SELECT
        labels
    FROM
        SCHEMA_CATALOG.series
    WHERE id = series_id
$$
LANGUAGE SQL STABLE PARALLEL SAFE;
COMMENT ON FUNCTION SCHEMA_PROM.labels(series_id BIGINT)
IS 'fetches labels array for the given series id';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.labels(series_id BIGINT) TO prom_reader;

--Do not call before checking that the series does not yet exist
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.create_series(
        metric_id int,
        metric_table_name NAME,
        label_array SCHEMA_PROM.label_array,
        OUT series_id BIGINT)
AS $func$
DECLARE
   new_series_id bigint;
BEGIN
  new_series_id = nextval('SCHEMA_CATALOG.series_id');
LOOP
    EXECUTE format ($$
        INSERT INTO SCHEMA_DATA_SERIES.%I(id, metric_id, labels)
        SELECT $1, $2, $3
        ON CONFLICT DO NOTHING
        RETURNING id
    $$, metric_table_name)
    INTO series_id
    USING new_series_id, metric_id, label_array;

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

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.resurrect_series_ids(metric_table name, series_id bigint)
    RETURNS VOID
AS $func$
BEGIN
    EXECUTE FORMAT($query$
        UPDATE SCHEMA_DATA_SERIES.%1$I
        SET delete_epoch = NULL
        WHERE id = $1
    $query$, metric_table) using series_id;
END
$func$
LANGUAGE PLPGSQL VOLATILE
--security definer to add jobs as the logged-in user
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON FUNCTION SCHEMA_CATALOG.resurrect_series_ids(name, bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.resurrect_series_ids(name, bigint) TO prom_writer;

-- There shouldn't be a need to have a read only version of this as we'll use
-- the eq or other matcher functions to find series ids like this. However,
-- there are possible use cases that need the series id directly for performance
-- that we might want to see if we need to support, in which case a
-- read only version might be useful in future.
CREATE OR REPLACE  FUNCTION SCHEMA_CATALOG.get_or_create_series_id(label jsonb)
RETURNS BIGINT AS $$
DECLARE
  series_id bigint;
  table_name name;
  metric_id int;
BEGIN
   --See get_or_create_series_id_for_kv_array for notes about locking
   SELECT mtn.id, mtn.table_name FROM SCHEMA_CATALOG.get_or_create_metric_table_name(label->>'__name__') mtn
   INTO metric_id, table_name;

   LOCK TABLE ONLY SCHEMA_CATALOG.series in ACCESS SHARE mode;

   EXECUTE format($query$
    WITH cte AS (
        SELECT SCHEMA_CATALOG.get_or_create_label_array($1)
    ), existing AS (
        SELECT
            id,
            CASE WHEN delete_epoch IS NOT NULL THEN
                SCHEMA_CATALOG.resurrect_series_ids(%1$L, id)
            END
        FROM SCHEMA_DATA_SERIES.%1$I as series
        WHERE labels = (SELECT * FROM cte)
    )
    SELECT id FROM existing
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
COMMENT ON FUNCTION SCHEMA_CATALOG.get_or_create_series_id(jsonb)
IS 'returns the series id that exactly matches a JSONB of labels';
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_or_create_series_id(jsonb) TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_or_create_series_id_for_kv_array(metric_name TEXT, label_keys text[], label_values text[], OUT table_name NAME, OUT series_id BIGINT)
AS $func$
DECLARE
  metric_id int;
BEGIN
   --need to make sure the series partition exists
   SELECT mtn.id, mtn.table_name FROM SCHEMA_CATALOG.get_or_create_metric_table_name(metric_name) mtn
   INTO metric_id, table_name;

   -- the data table could be locked during label key creation
   -- and must be locked before the series parent according to lock ordering
   EXECUTE format($query$
        LOCK TABLE ONLY SCHEMA_DATA.%1$I IN ACCESS SHARE MODE
    $query$, table_name);

   EXECUTE format($query$
    WITH cte AS (
        SELECT SCHEMA_CATALOG.get_or_create_label_array($1, $2, $3)
    ), existing AS (
        SELECT
            id,
            CASE WHEN delete_epoch IS NOT NULL THEN
                SCHEMA_CATALOG.resurrect_series_ids(%1$L, id)
            END
        FROM SCHEMA_DATA_SERIES.%1$I as series
        WHERE labels = (SELECT * FROM cte)
    )
    SELECT id FROM existing
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
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_or_create_series_id_for_kv_array(TEXT, text[], text[]) TO prom_writer;



CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_or_create_series_id_for_label_array(metric_name TEXT, larray SCHEMA_PROM.label_array, OUT table_name NAME, OUT series_id BIGINT)
AS $func$
DECLARE
  metric_id int;
BEGIN
   --need to make sure the series partition exists
   SELECT mtn.id, mtn.table_name FROM SCHEMA_CATALOG.get_or_create_metric_table_name(metric_name) mtn
   INTO metric_id, table_name;

   -- the data table could be locked during label key creation
   -- and must be locked before the series parent according to lock ordering
   EXECUTE format($query$
        LOCK TABLE ONLY SCHEMA_DATA.%1$I IN ACCESS SHARE MODE
    $query$, table_name);

   EXECUTE format($query$
    WITH existing AS (
        SELECT
            id,
            CASE WHEN delete_epoch IS NOT NULL THEN
                SCHEMA_CATALOG.resurrect_series_ids(%1$L, id)
            END
        FROM SCHEMA_DATA_SERIES.%1$I as series
        WHERE labels = $1
    )
    SELECT id FROM existing
    UNION ALL
    SELECT SCHEMA_CATALOG.create_series(%2$L, %1$L, $1)
    LIMIT 1
   $query$, table_name, metric_id)
   USING larray
   INTO series_id;

   RETURN;
END
$func$
LANGUAGE PLPGSQL VOLATILE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_or_create_series_id_for_label_array(TEXT, SCHEMA_PROM.label_array) TO prom_writer;

--
-- Parameter manipulation functions
--

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.set_chunk_interval_on_metric_table(metric_name TEXT, new_interval INTERVAL)
RETURNS void
AS $func$
BEGIN
    IF NOT SCHEMA_CATALOG.is_timescaledb_installed() THEN
        RAISE EXCEPTION 'cannot set chunk time interval without timescaledb installed';
    END IF;
    --set interval while adding 1% of randomness to the interval so that chunks are not aligned so that
    --chunks are staggered for compression jobs.
    EXECUTE SCHEMA_TIMESCALE.set_chunk_time_interval(
        format('SCHEMA_DATA.%I',(SELECT table_name FROM SCHEMA_CATALOG.get_or_create_metric_table_name(metric_name)))::regclass,
         SCHEMA_CATALOG.get_staggered_chunk_interval(new_interval));
END
$func$
LANGUAGE PLPGSQL VOLATILE
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON FUNCTION SCHEMA_CATALOG.set_chunk_interval_on_metric_table(TEXT, INTERVAL) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.set_chunk_interval_on_metric_table(TEXT, INTERVAL) TO prom_admin;

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
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.set_default_chunk_interval(INTERVAL) TO prom_admin;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.set_metric_chunk_interval(metric_name TEXT, chunk_interval INTERVAL)
RETURNS BOOLEAN
AS $func$
    --use get_or_create_metric_table_name because we want to be able to set /before/ any data is ingested
    --needs to run before update so row exists before update.
    SELECT SCHEMA_CATALOG.get_or_create_metric_table_name(set_metric_chunk_interval.metric_name);

    UPDATE SCHEMA_CATALOG.metric SET default_chunk_interval = false
    WHERE id IN (SELECT id FROM SCHEMA_CATALOG.get_metric_table_name_if_exists('SCHEMA_DATA', set_metric_chunk_interval.metric_name));

    SELECT SCHEMA_CATALOG.set_chunk_interval_on_metric_table(metric_name, chunk_interval);

    SELECT true;
$func$
LANGUAGE SQL VOLATILE;
COMMENT ON FUNCTION SCHEMA_PROM.set_metric_chunk_interval(TEXT, INTERVAL)
IS 'set a chunk interval for a specific metric (this overrides the default)';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.set_metric_chunk_interval(TEXT, INTERVAL) TO prom_admin;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.reset_metric_chunk_interval(metric_name TEXT)
RETURNS BOOLEAN
AS $func$
    UPDATE SCHEMA_CATALOG.metric SET default_chunk_interval = true
    WHERE id = (SELECT id FROM SCHEMA_CATALOG.get_metric_table_name_if_exists('SCHEMA_DATA', metric_name));

    SELECT SCHEMA_CATALOG.set_chunk_interval_on_metric_table(metric_name,
        SCHEMA_CATALOG.get_default_chunk_interval());

    SELECT true;
$func$
LANGUAGE SQL VOLATILE;
COMMENT ON FUNCTION SCHEMA_PROM.reset_metric_chunk_interval(TEXT)
IS 'resets the chunk interval for a specific metric to using the default';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.reset_metric_chunk_interval(TEXT) TO prom_admin;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_metric_retention_period(schema_name TEXT, metric_name TEXT)
RETURNS INTERVAL
AS $$
    SELECT COALESCE(m.retention_period, SCHEMA_CATALOG.get_default_retention_period())
    FROM SCHEMA_CATALOG.metric m
    WHERE id IN (SELECT id FROM SCHEMA_CATALOG.get_metric_table_name_if_exists(schema_name, get_metric_retention_period.metric_name))
    UNION ALL
    SELECT SCHEMA_CATALOG.get_default_retention_period()
    LIMIT 1
$$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_metric_retention_period(TEXT, TEXT) TO prom_reader;

-- convenience function for returning retention period of raw metrics
-- without the need to specify the schema
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_metric_retention_period(metric_name TEXT)
RETURNS INTERVAL
AS $$
    SELECT *
    FROM SCHEMA_CATALOG.get_metric_retention_period('SCHEMA_DATA', metric_name)
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
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.set_default_retention_period(INTERVAL) TO prom_admin;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.set_metric_retention_period(schema_name TEXT, metric_name TEXT, new_retention_period INTERVAL)
RETURNS BOOLEAN
AS $func$
DECLARE
    r SCHEMA_CATALOG.metric;
    _is_cagg BOOLEAN;
    _cagg_schema NAME;
    _cagg_name NAME;
BEGIN
    --use get_or_create_metric_table_name because we want to be able to set /before/ any data is ingested
    --needs to run before update so row exists before update.
    PERFORM SCHEMA_CATALOG.get_or_create_metric_table_name(set_metric_retention_period.metric_name)
    WHERE schema_name = 'SCHEMA_DATA';

    --check if its a metric view with cagg
    SELECT is_cagg, cagg_schema, cagg_name
    INTO _is_cagg, _cagg_schema, _cagg_name
    FROM SCHEMA_CATALOG.get_cagg_info(schema_name, metric_name);

    IF NOT _is_cagg OR (_cagg_name = metric_name AND _cagg_schema = schema_name) THEN
        UPDATE SCHEMA_CATALOG.metric m SET retention_period = new_retention_period
        WHERE m.table_schema = schema_name
        AND m.metric_name = set_metric_retention_period.metric_name;

        RETURN true;
    END IF;

    --handles 2-step aggregatop
    RAISE NOTICE 'Setting data retention period for all metrics with underlying continuous aggregate %.%', _cagg_schema, _cagg_name;

    FOR r IN
        SELECT m.*
        FROM information_schema.view_table_usage v
        INNER JOIN SCHEMA_CATALOG.metric m
          ON (m.table_name = v.view_name AND m.table_schema = v.view_schema)
        WHERE v.table_name = _cagg_name
          AND v.table_schema = _cagg_schema
    LOOP
        RAISE NOTICE 'Setting data retention for metrics %.%', r.table_schema, r.metric_name;

        UPDATE SCHEMA_CATALOG.metric m
        SET retention_period = new_retention_period
        WHERE m.table_schema = r.table_schema
        AND m.metric_name = r.metric_name;
    END LOOP;

    RETURN true;
END
$func$
LANGUAGE PLPGSQL VOLATILE;
COMMENT ON FUNCTION SCHEMA_PROM.set_metric_retention_period(TEXT, TEXT, INTERVAL)
IS 'set a retention period for a specific metric (this overrides the default)';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.set_metric_retention_period(TEXT, TEXT, INTERVAL)TO prom_admin;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.set_metric_retention_period(metric_name TEXT, new_retention_period INTERVAL)
RETURNS BOOLEAN
AS $func$
    SELECT SCHEMA_PROM.set_metric_retention_period('SCHEMA_DATA', metric_name, new_retention_period);
$func$
LANGUAGE SQL VOLATILE;
COMMENT ON FUNCTION SCHEMA_PROM.set_metric_retention_period(TEXT, INTERVAL)
IS 'set a retention period for a specific raw metric in default schema (this overrides the default)';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.set_metric_retention_period(TEXT, INTERVAL)TO prom_admin;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.reset_metric_retention_period(schema_name TEXT, metric_name TEXT)
RETURNS BOOLEAN
AS $func$
DECLARE
    r SCHEMA_CATALOG.metric;
    _is_cagg BOOLEAN;
    _cagg_schema NAME;
    _cagg_name NAME;
BEGIN

    --check if its a metric view with cagg
    SELECT is_cagg, cagg_schema, cagg_name
    INTO _is_cagg, _cagg_schema, _cagg_name
    FROM SCHEMA_CATALOG.get_cagg_info(schema_name, metric_name);

    IF NOT _is_cagg OR (_cagg_name = metric_name AND _cagg_schema = schema_name) THEN
        UPDATE SCHEMA_CATALOG.metric m SET retention_period = NULL
        WHERE m.table_schema = schema_name
        AND m.metric_name = reset_metric_retention_period.metric_name;

        RETURN true;
    END IF;

    RAISE NOTICE 'Resetting data retention period for all metrics with underlying continuous aggregate %.%', _cagg_schema, cagg_name;

    FOR r IN
        SELECT m.*
        FROM information_schema.view_table_usage v
        INNER JOIN SCHEMA_CATALOG.metric m
          ON (m.table_name = v.view_name AND m.table_schema = v.view_schema)
        WHERE v.table_name = _cagg_name
          AND v.table_schema = _cagg_schem
    LOOP
        RAISE NOTICE 'Resetting data retention for metrics %.%', r.table_schema, r.metric_name;

        UPDATE SCHEMA_CATALOG.metric m
        SET retention_period = NULL
        WHERE m.table_schema = r.table_schema
        AND m.metric_name = r.metric_name;
    END LOOP;

    RETURN true;
END
$func$
LANGUAGE PLPGSQL VOLATILE;
COMMENT ON FUNCTION SCHEMA_PROM.reset_metric_retention_period(TEXT, TEXT)
IS 'resets the retention period for a specific metric to using the default';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.reset_metric_retention_period(TEXT, TEXT) TO prom_admin;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.reset_metric_retention_period(metric_name TEXT)
RETURNS BOOLEAN
AS $func$
    SELECT SCHEMA_PROM.reset_metric_retention_period('SCHEMA_DATA', metric_name);
$func$
LANGUAGE SQL VOLATILE;
COMMENT ON FUNCTION SCHEMA_PROM.reset_metric_retention_period(TEXT)
IS 'resets the retention period for a specific raw metric in the default schema to using the default retention period';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.reset_metric_retention_period(TEXT) TO prom_admin;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_metric_compression_setting(metric_name TEXT)
RETURNS BOOLEAN
AS $$
DECLARE
    can_compress boolean;
    result boolean;
    metric_table_name text;
BEGIN
    SELECT exists(select * from pg_proc where proname = 'compress_chunk')
    INTO STRICT can_compress;

    IF NOT can_compress THEN
        RETURN FALSE;
    END IF;

    SELECT table_name
    INTO STRICT metric_table_name
    FROM SCHEMA_CATALOG.get_metric_table_name_if_exists('SCHEMA_DATA', metric_name);

    IF SCHEMA_CATALOG.get_timescale_major_version() >= 2  THEN
        SELECT compression_enabled
        FROM timescaledb_information.hypertables
        WHERE hypertable_schema ='SCHEMA_DATA'
          AND hypertable_name = metric_table_name
        INTO STRICT result;
    ELSE
        SELECT EXISTS (
            SELECT FROM _timescaledb_catalog.hypertable h
            WHERE h.schema_name = 'SCHEMA_DATA'
            AND h.table_name = metric_table_name
            AND h.compressed_hypertable_id IS NOT NULL)
        INTO result;
    END IF;
    RETURN result;
END
$$
LANGUAGE PLPGSQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_metric_compression_setting(TEXT) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.set_default_compression_setting(compression_setting BOOLEAN)
RETURNS BOOLEAN
AS $$
DECLARE
    can_compress BOOLEAN;
BEGIN
    IF compression_setting = SCHEMA_CATALOG.get_default_compression_setting() THEN
        RETURN TRUE;
    END IF;

    SELECT exists(select * from pg_proc where proname = 'compress_chunk')
    INTO STRICT can_compress;

    IF NOT can_compress AND compression_setting THEN
        RAISE EXCEPTION 'Cannot enable metrics compression, feature not found';
    END IF;

    INSERT INTO SCHEMA_CATALOG.default(key, value) VALUES('metric_compression', compression_setting::text)
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

    PERFORM SCHEMA_PROM.set_compression_on_metric_table(table_name, compression_setting)
    FROM SCHEMA_CATALOG.metric
    WHERE default_compression;
    RETURN true;
END
$$
LANGUAGE PLPGSQL VOLATILE;
COMMENT ON FUNCTION SCHEMA_PROM.set_default_compression_setting(BOOLEAN)
IS 'set the compression setting for any metrics (existing and new) without an explicit override';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.set_default_compression_setting(BOOLEAN) TO prom_admin;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.set_metric_compression_setting(metric_name TEXT, new_compression_setting BOOLEAN)
RETURNS BOOLEAN
AS $func$
DECLARE
    can_compress boolean;
    metric_table_name text;
BEGIN
    --if already set to desired value, nothing to do
    IF SCHEMA_CATALOG.get_metric_compression_setting(metric_name) = new_compression_setting THEN
        RETURN TRUE;
    END IF;

    SELECT exists(select * from pg_proc where proname = 'compress_chunk')
    INTO STRICT can_compress;

    --if compression is missing, cannot enable it
    IF NOT can_compress AND new_compression_setting THEN
        RAISE EXCEPTION 'Cannot enable metrics compression, feature not found';
    END IF;

    --use get_or_create_metric_table_name because we want to be able to set /before/ any data is ingested
    --needs to run before update so row exists before update.
    SELECT table_name
    INTO STRICT metric_table_name
    FROM SCHEMA_CATALOG.get_or_create_metric_table_name(set_metric_compression_setting.metric_name);

    PERFORM SCHEMA_PROM.set_compression_on_metric_table(metric_table_name, new_compression_setting);

    UPDATE SCHEMA_CATALOG.metric
    SET default_compression = false
    WHERE table_name = metric_table_name;

    RETURN true;
END
$func$
LANGUAGE PLPGSQL VOLATILE;
COMMENT ON FUNCTION SCHEMA_PROM.set_metric_compression_setting(TEXT, BOOLEAN)
IS 'set a compression setting for a specific metric (this overrides the default)';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.set_metric_compression_setting(TEXT, BOOLEAN) TO prom_admin;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.set_compression_on_metric_table(metric_table_name TEXT, compression_setting BOOLEAN)
RETURNS void
AS $func$
DECLARE
BEGIN
    IF SCHEMA_CATALOG.is_timescaledb_oss() THEN
        RAISE NOTICE 'Compression not available in TimescaleDB-OSS. Cannot set compression on "%" metric table name', metric_table_name;
        RETURN;
    END IF;
    IF compression_setting THEN
        EXECUTE format($$
            ALTER TABLE SCHEMA_DATA.%I SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = 'series_id',
                timescaledb.compress_orderby = 'time, value'
            ); $$, metric_table_name);

        --rc4 of multinode doesn't properly hand down compression when turned on
        --inside of a function; this gets around that.
        IF SCHEMA_CATALOG.is_multinode() THEN
            CALL SCHEMA_TIMESCALE.distributed_exec(
                format($$
                ALTER TABLE SCHEMA_DATA.%I SET (
                    timescaledb.compress,
                   timescaledb.compress_segmentby = 'series_id',
                   timescaledb.compress_orderby = 'time, value'
               ); $$, metric_table_name),
               transactional => false);
        END IF;

        --chunks where the end time is before now()-1 hour will be compressed
        --per-ht compression policy only used for timescale 1.x
        IF SCHEMA_CATALOG.get_timescale_major_version() < 2 THEN
            PERFORM SCHEMA_TIMESCALE.add_compress_chunks_policy(format('SCHEMA_DATA.%I', metric_table_name), INTERVAL '1 hour');
        END IF;
    ELSE
        IF SCHEMA_CATALOG.get_timescale_major_version() < 2 THEN
            PERFORM SCHEMA_TIMESCALE.remove_compress_chunks_policy(format('SCHEMA_DATA.%I', metric_table_name));
        END IF;

        CALL SCHEMA_CATALOG.decompress_chunks_after(metric_table_name::name, timestamptz '-Infinity', transactional=>true);

        EXECUTE format($$
            ALTER TABLE SCHEMA_DATA.%I SET (
                timescaledb.compress = false
            ); $$, metric_table_name);
    END IF;
END
$func$
LANGUAGE PLPGSQL VOLATILE
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON FUNCTION SCHEMA_PROM.set_compression_on_metric_table(TEXT, BOOLEAN) FROM PUBLIC;
COMMENT ON FUNCTION SCHEMA_PROM.set_compression_on_metric_table(TEXT, BOOLEAN)
IS 'set a compression for a specific metric table';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.set_compression_on_metric_table(TEXT, BOOLEAN) TO prom_admin;


CREATE OR REPLACE FUNCTION SCHEMA_PROM.reset_metric_compression_setting(metric_name TEXT)
RETURNS BOOLEAN
AS $func$
DECLARE
    metric_table_name text;
BEGIN
    SELECT table_name
    INTO STRICT metric_table_name
    FROM SCHEMA_CATALOG.get_or_create_metric_table_name(reset_metric_compression_setting.metric_name);

    UPDATE SCHEMA_CATALOG.metric
    SET default_compression = true
    WHERE table_name = metric_table_name;

    PERFORM SCHEMA_PROM.set_compression_on_metric_table(metric_table_name, SCHEMA_CATALOG.get_default_compression_setting());
    RETURN true;
END
$func$
LANGUAGE PLPGSQL VOLATILE;
COMMENT ON FUNCTION SCHEMA_PROM.reset_metric_compression_setting(TEXT)
IS 'resets the compression setting for a specific metric to using the default';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.reset_metric_compression_setting(TEXT) TO prom_admin;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.epoch_abort(user_epoch BIGINT)
RETURNS VOID AS $func$
DECLARE db_epoch BIGINT;
BEGIN
    SELECT current_epoch FROM ids_epoch LIMIT 1
        INTO db_epoch;
    RAISE EXCEPTION 'epoch % to old to continue INSERT, current: %',
        user_epoch, db_epoch
        USING ERRCODE='PS001';
END;
$func$ LANGUAGE PLPGSQL VOLATILE;
COMMENT ON FUNCTION SCHEMA_CATALOG.epoch_abort(BIGINT)
IS 'ABORT an INSERT transaction due to the ID epoch being out of date';
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.epoch_abort TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_confirmed_unused_series(
    metric_schema NAME, metric_table NAME, series_table NAME, potential_series_ids BIGINT[], check_time TIMESTAMPTZ
) RETURNS BIGINT[]
AS $func$
DECLARE
    r RECORD;
    check_time_condition TEXT;
BEGIN
    FOR r IN
        SELECT *
        FROM SCHEMA_CATALOG.metric m
        WHERE m.series_table = get_confirmed_unused_series.series_table
    LOOP

    check_time_condition := '';
    IF r.table_schema = metric_schema::NAME AND r.table_name = metric_table::NAME THEN
        check_time_condition := FORMAT('AND time >= %L', check_time);
    END IF;

    --at each iteration of the loop filter potential_series_ids to only
    --have those series ids that don't exist in the metric tables.
    EXECUTE format(
    $query$
        SELECT array_agg(potential_series.series_id)
        FROM unnest($1) as potential_series(series_id)
        LEFT JOIN LATERAL(
            SELECT 1
            FROM  %1$I.%2$I data_exists
            WHERE data_exists.series_id = potential_series.series_id
            %3$s
            --use chunk append + more likely to find something starting at earliest time
            ORDER BY time ASC
            LIMIT 1
        ) as lateral_exists(indicator) ON (true)
        WHERE lateral_exists.indicator IS NULL
    $query$, r.table_schema, r.table_name, check_time_condition)
		USING potential_series_ids
    INTO potential_series_ids;

    END LOOP;

    RETURN potential_series_ids;
END
$func$
LANGUAGE PLPGSQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_confirmed_unused_series(NAME, NAME, NAME, BIGINT[], TIMESTAMPTZ) TO prom_maintenance;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.mark_unused_series(
    metric_schema TEXT, metric_table TEXT, metric_series_table TEXT, older_than TIMESTAMPTZ, check_time TIMESTAMPTZ
) RETURNS VOID AS $func$
DECLARE
BEGIN
    --chances are that the hour after the drop point will have the most similar
    --series to what is dropped, so first filter by all series that have been dropped
    --but that aren't in that first hour and then make sure they aren't in the dataset
    EXECUTE format(
    $query$
        WITH potentially_drop_series AS (
            SELECT distinct series_id
            FROM %1$I.%2$I
            WHERE time < %4$L
            EXCEPT
            SELECT distinct series_id
            FROM %1$I.%2$I
            WHERE time >= %4$L AND time < %5$L
        ), confirmed_drop_series AS (
            SELECT SCHEMA_CATALOG.get_confirmed_unused_series('%1$s','%2$s','%3$s', array_agg(series_id), %5$L) as ids
            FROM potentially_drop_series
        ) -- we want this next statement to be the last one in the txn since it could block series fetch (both of them update delete_epoch)
        UPDATE SCHEMA_DATA_SERIES.%3$I SET delete_epoch = current_epoch+1
        FROM SCHEMA_CATALOG.ids_epoch
        WHERE delete_epoch IS NULL
            AND id IN (SELECT unnest(ids) FROM confirmed_drop_series)
    $query$, metric_schema, metric_table, metric_series_table, older_than, check_time);
END
$func$
LANGUAGE PLPGSQL VOLATILE
--security definer to add jobs as the logged-in user
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON FUNCTION SCHEMA_CATALOG.mark_unused_series(text, text, text, timestamptz, timestamptz) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.mark_unused_series(text, text, text, timestamptz, timestamptz) TO prom_maintenance;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.delete_expired_series(
    metric_schema TEXT, metric_table TEXT, metric_series_table TEXT, ran_at TIMESTAMPTZ, present_epoch BIGINT, last_updated_epoch TIMESTAMPTZ
) RETURNS VOID AS $func$
DECLARE
    label_array int[];
    next_epoch BIGINT;
    deletion_epoch BIGINT;
BEGIN
    next_epoch := present_epoch + 1;
    -- technically we can delete any ID <= current_epoch - 1
    -- but it's always safe to leave them around for a bit longer
    deletion_epoch := present_epoch - 4;

    -- we don't want to delete too soon
    IF ran_at < last_updated_epoch + '1 hour' THEN
        RETURN;
    END IF;

    EXECUTE format($query$
        -- recheck that the series IDs we might delete are actually dead
        WITH dead_series AS (
            SELECT potential.id
            FROM
            (
                SELECT id
                FROM SCHEMA_DATA_SERIES.%3$I
                WHERE delete_epoch <= %4$L
            ) as potential
            LEFT JOIN LATERAL (
                SELECT 1
                FROM %1$I.%2$I metric_data
                WHERE metric_data.series_id = potential.id
                LIMIT 1
            ) as lateral_exists(indicator) ON (TRUE)
            WHERE indicator IS NULL
        ), deleted_series AS (
            DELETE FROM SCHEMA_DATA_SERIES.%3$I
            WHERE delete_epoch <= %4$L
                AND id IN (SELECT id FROM dead_series) -- concurrency means we need this qual in both
            RETURNING id, labels
        ), resurrected_series AS (
            UPDATE SCHEMA_DATA_SERIES.%3$I
            SET delete_epoch = NULL
            WHERE delete_epoch <= %4$L
                AND id NOT IN (SELECT id FROM dead_series) -- concurrency means we need this qual in both
        )
        SELECT ARRAY(SELECT DISTINCT unnest(labels) as label_id
            FROM deleted_series)
    $query$, metric_schema, metric_table, metric_series_table, deletion_epoch) INTO label_array;


    IF array_length(label_array, 1) > 0 THEN
        --jit interacts poorly why the multi-partition query below
        SET LOCAL jit = 'off';
        --needs to be a separate query and not a CTE since this needs to "see"
        --the series rows deleted above as deleted.
        --Note: we never delete metric name keys since there are check constraints that
        --rely on those ids not changing.
        EXECUTE format($query$
        WITH check_local_series AS (
                --the series table from which we just deleted is much more likely to have the label, so check that first to exclude most labels.
                SELECT label_id
                FROM unnest($1) as labels(label_id)
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM  SCHEMA_DATA_SERIES.%1$I series_exists_local
                    WHERE series_exists_local.labels && ARRAY[labels.label_id]
                    LIMIT 1
                )
            ),
            confirmed_drop_labels AS (
                --do the global check to confirm
                SELECT label_id
                FROM check_local_series
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM  SCHEMA_CATALOG.series series_exists
                    WHERE series_exists.labels && ARRAY[label_id]
                    LIMIT 1
                )
            )
            DELETE FROM SCHEMA_CATALOG.label
            WHERE id IN (SELECT * FROM confirmed_drop_labels) AND key != '__name__';
        $query$, metric_series_table) USING label_array;

        SET LOCAL jit = DEFAULT;
    END IF;

    UPDATE SCHEMA_CATALOG.ids_epoch
        SET (current_epoch, last_update_time) = (next_epoch, now())
        WHERE current_epoch < next_epoch;
    RETURN;
END
$func$
LANGUAGE PLPGSQL VOLATILE
--security definer to add jobs as the logged-in user
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON FUNCTION SCHEMA_CATALOG.delete_expired_series(text, text, text, timestamptz, BIGINT, timestamptz) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.delete_expired_series(text, text, text, timestamptz, BIGINT, timestamptz) TO prom_maintenance;

--drop chunks from metrics tables and delete the appropriate series.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.drop_metric_chunk_data(
    schema_name TEXT, metric_name TEXT, older_than TIMESTAMPTZ
) RETURNS VOID AS $func$
DECLARE
    metric_schema NAME;
    metric_table NAME;
    metric_view BOOLEAN;
    _is_cagg BOOLEAN;
BEGIN
    SELECT table_schema, table_name, is_view
    INTO STRICT metric_schema, metric_table, metric_view
    FROM SCHEMA_CATALOG.get_metric_table_name_if_exists(schema_name, metric_name);

    -- need to check for caggs when dealing with metric views
    IF metric_view THEN
        SELECT is_cagg, cagg_schema, cagg_name
        INTO _is_cagg, metric_schema, metric_table
        FROM SCHEMA_CATALOG.get_cagg_info(schema_name, metric_name);
        IF NOT _is_cagg THEN
          RETURN;
        END IF;
    END IF;

    IF SCHEMA_CATALOG.is_timescaledb_installed() THEN
        IF SCHEMA_CATALOG.get_timescale_major_version() >= 2 THEN
            PERFORM SCHEMA_TIMESCALE.drop_chunks(
                relation=>format('%I.%I', metric_schema, metric_table),
                older_than=>older_than
            );
        ELSE
            PERFORM SCHEMA_TIMESCALE.drop_chunks(
                table_name=>metric_table,
                schema_name=> metric_schema,
                older_than=>older_than,
                cascade_to_materializations=>FALSE
            );
        END IF;
    ELSE
        EXECUTE format($$ DELETE FROM %I.%I WHERE time < %L $$, metric_schema, metric_table, older_than);
    END IF;
END
$func$
LANGUAGE PLPGSQL VOLATILE
--security definer to add jobs as the logged-in user
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON FUNCTION SCHEMA_CATALOG.drop_metric_chunk_data(text, text, timestamptz) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.drop_metric_chunk_data(text, text, timestamptz) TO prom_maintenance;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.set_app_name(full_name text)
    RETURNS VOID
AS $func$
    --setting a name that's too long create surpurflous NOTICE messages in the log
    SELECT set_config('application_name', substring(full_name for 63), false);
$func$
LANGUAGE SQL VOLATILE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.set_app_name(text) TO prom_maintenance;

--drop chunks from metrics tables and delete the appropriate series.
CREATE OR REPLACE PROCEDURE SCHEMA_CATALOG.drop_metric_chunks(
    schema_name TEXT, metric_name TEXT, older_than TIMESTAMPTZ, ran_at TIMESTAMPTZ = now(), log_verbose BOOLEAN = FALSE
) AS $func$
DECLARE
    metric_id int;
    metric_schema NAME;
    metric_table NAME;
    metric_series_table NAME;
    is_metric_view BOOLEAN;
    check_time TIMESTAMPTZ;
    time_dimension_id INT;
    last_updated TIMESTAMPTZ;
    present_epoch BIGINT;
    lastT TIMESTAMPTZ;
    startT TIMESTAMPTZ;
BEGIN
    SELECT id, table_schema, table_name, series_table, is_view
    INTO STRICT metric_id, metric_schema, metric_table, metric_series_table, is_metric_view
    FROM SCHEMA_CATALOG.get_metric_table_name_if_exists(schema_name, metric_name);

    SELECT older_than + INTERVAL '1 hour'
    INTO check_time;

    startT := clock_timestamp();

    PERFORM SCHEMA_CATALOG.set_app_name(format('promscale maintenance: data retention: metric %s', metric_name));
    IF log_verbose THEN
        RAISE LOG 'promscale maintenance: data retention: metric %: starting', metric_name;
    END IF;

    -- transaction 1
        IF SCHEMA_CATALOG.is_timescaledb_installed() THEN
            --Get the time dimension id for the time dimension
            SELECT d.id
            INTO STRICT time_dimension_id
            FROM _timescaledb_catalog.dimension d
            INNER JOIN SCHEMA_CATALOG.get_storage_hypertable_info(metric_schema, metric_table, is_metric_view) hi ON (hi.id = d.hypertable_id)
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
        END IF;
        -- end this txn so we're not holding any locks on the catalog

        SELECT current_epoch, last_update_time INTO present_epoch, last_updated FROM
            SCHEMA_CATALOG.ids_epoch LIMIT 1;
    COMMIT;

    IF older_than IS NULL THEN
        -- even though there are no new Ids in need of deletion,
        -- we may still have old ones to delete
        lastT := clock_timestamp();
        PERFORM SCHEMA_CATALOG.set_app_name(format('promscale maintenance: data retention: metric %s: delete expired series', metric_name));
        PERFORM SCHEMA_CATALOG.delete_expired_series(metric_schema, metric_table, metric_series_table, ran_at, present_epoch, last_updated);
        IF log_verbose THEN
            RAISE LOG 'promscale maintenance: data retention: metric %: done deleting expired series as only action in %', metric_name, clock_timestamp()-lastT;
            RAISE LOG 'promscale maintenance: data retention: metric %: finished in %', metric_name, clock_timestamp()-startT;
        END IF;
        RETURN;
    END IF;

    -- transaction 2
        lastT := clock_timestamp();
        PERFORM SCHEMA_CATALOG.set_app_name(format('promscale maintenance: data retention: metric %s: mark unused series', metric_name));
        PERFORM SCHEMA_CATALOG.mark_unused_series(metric_schema, metric_table, metric_series_table, older_than, check_time);
        IF log_verbose THEN
            RAISE LOG 'promscale maintenance: data retention: metric %: done marking unused series in %', metric_name, clock_timestamp()-lastT;
        END IF;
    COMMIT;

    -- transaction 3
        lastT := clock_timestamp();
        PERFORM SCHEMA_CATALOG.set_app_name( format('promscale maintenance: data retention: metric %s: drop chunks', metric_name));
        PERFORM SCHEMA_CATALOG.drop_metric_chunk_data(metric_schema, metric_name, older_than);
        IF log_verbose THEN
            RAISE LOG 'promscale maintenance: data retention: metric %: done dropping chunks in %', metric_name, clock_timestamp()-lastT;
        END IF;
        SELECT current_epoch, last_update_time INTO present_epoch, last_updated FROM
            SCHEMA_CATALOG.ids_epoch LIMIT 1;
    COMMIT;


    -- transaction 4
        lastT := clock_timestamp();
        PERFORM SCHEMA_CATALOG.set_app_name( format('promscale maintenance: data retention: metric %s: delete expired series', metric_name));
        PERFORM SCHEMA_CATALOG.delete_expired_series(metric_schema, metric_table, metric_series_table, ran_at, present_epoch, last_updated);
        IF log_verbose THEN
            RAISE LOG 'promscale maintenance: data retention: metric %: done deleting expired series in %', metric_name, clock_timestamp()-lastT;
            RAISE LOG 'promscale maintenance: data retention: metric %: finished in %', metric_name, clock_timestamp()-startT;
        END IF;
    RETURN;
END
$func$
LANGUAGE PLPGSQL;
GRANT EXECUTE ON PROCEDURE SCHEMA_CATALOG.drop_metric_chunks(text, text, timestamptz, timestamptz, boolean) TO prom_maintenance;


-- Get hypertable information for where data is stored for raw metrics and for
-- the materialized hypertable for cagg metrics. For non-materialized views return
-- no rows
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_storage_hypertable_info(metric_schema_name text, metric_table_name text, is_view boolean)
RETURNS TABLE (id int, hypertable_relation text)
AS $$
DECLARE
    agg_schema name;
    agg_name name;
    _is_cagg boolean;
    _materialized_hypertable_id int;
    _storage_hypertable_relation text;
BEGIN
        IF NOT SCHEMA_CATALOG.is_timescaledb_installed() THEN
            RETURN;
        END IF;

        IF NOT is_view THEN
                RETURN QUERY
                SELECT h.id, format('%I.%I', h.schema_name,  h.table_name)
                FROM _timescaledb_catalog.hypertable h
                WHERE h.schema_name = metric_schema_name
                AND h.table_name = metric_table_name;
                RETURN;
        END IF;

        SELECT is_cagg, materialized_hypertable_id, storage_hypertable_relation
        INTO _is_cagg, _materialized_hypertable_id, _storage_hypertable_relation
        FROM SCHEMA_CATALOG.get_cagg_info(metric_schema_name, metric_table_name);

        IF NOT _is_cagg THEN
            RETURN;
        END IF;

        RETURN QUERY SELECT _materialized_hypertable_id, _storage_hypertable_relation;
END
$$
LANGUAGE PLPGSQL STABLE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_storage_hypertable_info(text, text, boolean) TO prom_reader;


--Get underlying metric view schema and name
--we need to support up to two levels of views to support 2-step caggs
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_first_level_view_on_metric(metric_schema text, metric_table text)
RETURNS TABLE (view_schema name, view_name name, metric_table_name name)
AS $$
BEGIN
    --RAISE WARNING 'checking view: % %', metric_schema, metric_table;
    RETURN QUERY
    SELECT v.view_schema::name, v.view_name::name, v.table_name::name
    FROM information_schema.view_table_usage v
    WHERE v.view_schema = metric_schema
    AND v.view_name = metric_table
    AND v.table_schema = 'SCHEMA_DATA';

    IF FOUND THEN
        RETURN;
    END IF;

    -- if first level not found, return 2nd level if any
    RETURN QUERY
    SELECT v2.view_schema::name, v2.view_name::name, v2.table_name::name
    FROM information_schema.view_table_usage v
    LEFT JOIN information_schema.view_table_usage v2
        ON (v2.view_schema = v.table_schema
            AND v2.view_name = v.table_name)
    WHERE v.view_schema = metric_schema
    AND v.view_name = metric_table
    AND v2.table_schema = 'SCHEMA_DATA';
    RETURN;
END
$$
LANGUAGE PLPGSQL STABLE
--security definer to add jobs as the logged-in user
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON FUNCTION SCHEMA_CATALOG.get_first_level_view_on_metric(text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_first_level_view_on_metric(text, text) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_cagg_info(
    metric_schema text, metric_table text,
    OUT is_cagg BOOLEAN, OUT cagg_schema name, OUT cagg_name name, OUT metric_table_name name,
    OUT materialized_hypertable_id INT, OUT storage_hypertable_relation TEXT)
AS $$
BEGIN
    is_cagg := FALSE;
    SELECT *
    FROM SCHEMA_CATALOG.get_first_level_view_on_metric(metric_schema, metric_table)
    INTO cagg_schema, cagg_name, metric_table_name;

    IF NOT FOUND THEN
      RETURN;
    END IF;

    -- for TSDB 2.x we return the view schema and name because functions like
    -- show_chunks don't work on materialized hypertables, which is a difference
    -- from 1.x version
    IF SCHEMA_CATALOG.get_timescale_major_version() >= 2 THEN
        SELECT h.id, format('%I.%I', c.view_schema,  c.view_name)
        INTO materialized_hypertable_id, storage_hypertable_relation
        FROM timescaledb_information.continuous_aggregates c
        INNER JOIN _timescaledb_catalog.hypertable h
            ON (h.schema_name = c.materialization_hypertable_schema
                AND h.table_name = c.materialization_hypertable_name)
        WHERE c.view_schema = cagg_schema
        AND c.view_name = cagg_name;
    ELSE
        SELECT h.id, format('%I.%I', h.schema_name,  h.table_name)
        INTO materialized_hypertable_id, storage_hypertable_relation
        FROM timescaledb_information.continuous_aggregates c
        INNER JOIN _timescaledb_catalog.hypertable h
            ON (c.materialization_hypertable::text = format('%I.%I', h.schema_name,  h.table_name))
        WHERE c.view_name::text = format('%I.%I', cagg_schema, cagg_name);
    END IF;

    IF NOT FOUND THEN
      cagg_schema := NULL;
      cagg_name := NULL;
      metric_table_name := NULL;
      RETURN;
    END IF;

    is_cagg := true;
    return;
END
$$
LANGUAGE PLPGSQL STABLE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_cagg_info(text, text) TO prom_reader;


--Order by random with stable marking gives us same order in a statement and different
-- orderings in different statements
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_metrics_that_need_drop_chunk()
RETURNS SETOF SCHEMA_CATALOG.metric
AS $$
BEGIN
        IF NOT SCHEMA_CATALOG.is_timescaledb_installed() THEN
                    -- no real shortcut to figure out if deletion needed, return all
                    RETURN QUERY
                    SELECT m.*
                    FROM SCHEMA_CATALOG.metric m
                    WHERE is_view = FALSE
                    ORDER BY random();
                    RETURN;
        END IF;

        RETURN QUERY
        SELECT m.*
        FROM SCHEMA_CATALOG.metric m
        WHERE EXISTS (
            SELECT 1 FROM
            SCHEMA_CATALOG.get_storage_hypertable_info(m.table_schema, m.table_name, m.is_view) hi
            INNER JOIN show_chunks(hi.hypertable_relation,
                         older_than=>NOW() - SCHEMA_CATALOG.get_metric_retention_period(m.table_schema, m.metric_name)) sc ON TRUE)
        --random order also to prevent starvation
        ORDER BY random();
        RETURN;
END
$$
LANGUAGE PLPGSQL STABLE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_metrics_that_need_drop_chunk() TO prom_reader;

CREATE OR REPLACE PROCEDURE SCHEMA_CATALOG.execute_data_retention_policy(log_verbose boolean)
AS $$
DECLARE
    r SCHEMA_CATALOG.metric;
    remaining_metrics SCHEMA_CATALOG.metric[] DEFAULT '{}';
BEGIN
     --Do one loop with metric that could be locked without waiting.
    --This allows you to do everything you can while avoiding lock contention.
    --Then come back for the metrics that would have needed to wait on the lock.
    --Hopefully, that lock is now freed. The secoond loop waits for the lock
    --to prevent starvation.
    FOR r IN
        SELECT *
        FROM SCHEMA_CATALOG.get_metrics_that_need_drop_chunk()
    LOOP
        IF NOT SCHEMA_CATALOG.lock_metric_for_maintenance(r.id, wait=>false) THEN
            remaining_metrics := remaining_metrics || r;
            CONTINUE;
        END IF;
        CALL SCHEMA_CATALOG.drop_metric_chunks(r.table_schema, r.metric_name, NOW() - SCHEMA_CATALOG.get_metric_retention_period(r.table_schema, r.metric_name), log_verbose=>log_verbose);
        PERFORM SCHEMA_CATALOG.unlock_metric_for_maintenance(r.id);

        COMMIT;
    END LOOP;

    IF log_verbose AND array_length(remaining_metrics, 1) > 0 THEN
        RAISE LOG 'promscale maintenance: data retention: need to wait to grab locks on % metrics', array_length(remaining_metrics, 1);
    END IF;

    FOR r IN
        SELECT *
        FROM unnest(remaining_metrics)
    LOOP
        PERFORM SCHEMA_CATALOG.set_app_name( format('promscale maintenance: data retention: metric %s: wait for lock', r.metric_name));
        PERFORM SCHEMA_CATALOG.lock_metric_for_maintenance(r.id);
        CALL SCHEMA_CATALOG.drop_metric_chunks(r.table_schema, r.metric_name, NOW() - SCHEMA_CATALOG.get_metric_retention_period(r.table_schema, r.metric_name), log_verbose=>log_verbose);
        PERFORM SCHEMA_CATALOG.unlock_metric_for_maintenance(r.id);

        COMMIT;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;
COMMENT ON PROCEDURE SCHEMA_CATALOG.execute_data_retention_policy(boolean)
IS 'drops old data according to the data retention policy. This procedure should be run regularly in a cron job';
GRANT EXECUTE ON PROCEDURE SCHEMA_CATALOG.execute_data_retention_policy(boolean) TO prom_maintenance;

--public procedure to be called by cron
--right now just does data retention but name is generic so that
--we can add stuff later without needing people to change their cron scripts
--should be the last thing run in a session so that all session locks
--are guaranteed released on error.
CREATE OR REPLACE PROCEDURE SCHEMA_PROM.execute_maintenance(log_verbose boolean = false)
AS $$
DECLARE
   startT TIMESTAMPTZ;
BEGIN
    startT := clock_timestamp();
    IF log_verbose THEN
        RAISE LOG 'promscale maintenance: data retention: starting';
    END IF;

    PERFORM SCHEMA_CATALOG.set_app_name( format('promscale maintenance: data retention'));
    CALL SCHEMA_CATALOG.execute_data_retention_policy(log_verbose=>log_verbose);

    IF NOT SCHEMA_CATALOG.is_timescaledb_oss() AND SCHEMA_CATALOG.get_timescale_major_version() >= 2 THEN
        IF log_verbose THEN
            RAISE LOG 'promscale maintenance: compression: starting';
        END IF;

        PERFORM SCHEMA_CATALOG.set_app_name( format('promscale maintenance: compression'));
        CALL SCHEMA_CATALOG.execute_compression_policy(log_verbose=>log_verbose);
    END IF;

    IF log_verbose THEN
        RAISE LOG 'promscale maintenance: finished in %', clock_timestamp()-startT;
    END IF;

    IF clock_timestamp()-startT > INTERVAL '12 hours' THEN
        RAISE WARNING 'promscale maintenance jobs are taking too long (one run took %)', clock_timestamp()-startT
              USING HINT = 'Please consider increasing the number of maintenance jobs using config_maintenance_jobs()';
    END IF;
END;
$$ LANGUAGE PLPGSQL;
COMMENT ON PROCEDURE SCHEMA_PROM.execute_maintenance(boolean)
IS 'Execute maintenance tasks like dropping data according to retention policy. This procedure should be run regularly in a cron job';
GRANT EXECUTE ON PROCEDURE SCHEMA_PROM.execute_maintenance(boolean) TO prom_maintenance;

CREATE OR REPLACE PROCEDURE SCHEMA_CATALOG.execute_maintenance_job(job_id int, config jsonb)
AS $$
DECLARE
   log_verbose boolean;
   ae_key text;
   ae_value text;
   ae_load boolean := FALSE;
BEGIN
    log_verbose := coalesce(config->>'log_verbose', 'false')::boolean;

    --if auto_explain enabled in config, turn it on in a best-effort way
    --i.e. if it fails (most likely due to lack of superuser priviliges) move on anyway.
    BEGIN
        FOR ae_key, ae_value IN
           SELECT * FROM jsonb_each_text(config->'auto_explain')
        LOOP
            IF NOT ae_load THEN
                ae_load := true;
                LOAD 'auto_explain';
            END IF;

            PERFORM set_config('auto_explain.'|| ae_key, ae_value, FALSE);
        END LOOP;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'could not set auto_explain options';
    END;


    CALL SCHEMA_PROM.execute_maintenance(log_verbose=>log_verbose);
END
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.config_maintenance_jobs(number_jobs int, new_schedule_interval interval, new_config jsonb = NULL)
RETURNS BOOLEAN
AS $func$
DECLARE
  cnt int;
  log_verbose boolean;
BEGIN
    --check format of config
    log_verbose := coalesce(new_config->>'log_verbose', 'false')::boolean;

    PERFORM SCHEMA_TIMESCALE.delete_job(job_id)
    FROM timescaledb_information.jobs
    WHERE proc_schema = 'SCHEMA_CATALOG' AND proc_name = 'execute_maintenance_job' AND (schedule_interval != new_schedule_interval OR new_config IS DISTINCT FROM config) ;


    SELECT count(*) INTO cnt
    FROM timescaledb_information.jobs
    WHERE proc_schema = 'SCHEMA_CATALOG' AND proc_name = 'execute_maintenance_job';

    IF cnt < number_jobs THEN
        PERFORM SCHEMA_TIMESCALE.add_job('SCHEMA_CATALOG.execute_maintenance_job', new_schedule_interval, config=>new_config)
        FROM generate_series(1, number_jobs-cnt);
    END IF;

    IF cnt > number_jobs THEN
        PERFORM SCHEMA_TIMESCALE.delete_job(job_id)
        FROM timescaledb_information.jobs
        WHERE proc_schema = 'SCHEMA_CATALOG' AND proc_name = 'execute_maintenance_job'
        LIMIT (cnt-number_jobs);
    END IF;

    RETURN TRUE;
END
$func$
LANGUAGE PLPGSQL VOLATILE
--security definer to add jobs as the logged-in user
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON FUNCTION SCHEMA_PROM.config_maintenance_jobs(int, interval, jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.config_maintenance_jobs(int, interval, jsonb) TO prom_admin;
COMMENT ON FUNCTION SCHEMA_PROM.config_maintenance_jobs(int, interval, jsonb)
IS 'Configure the number of maintence jobs run by the job scheduler, as well as their scheduled interval';


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
   view_exists boolean;
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
    WHERE m.metric_name = create_series_view.metric_name
    AND m.table_schema = 'SCHEMA_DATA';

    SELECT COUNT(*) > 0 into view_exists
    FROM pg_class
    WHERE
      relname = view_name AND
      relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'SCHEMA_SERIES');

    EXECUTE FORMAT($$
        CREATE OR REPLACE VIEW SCHEMA_SERIES.%1$I AS
        SELECT
            id AS series_id,
            labels
            %2$s
        FROM
            SCHEMA_DATA_SERIES.%1$I AS series
        WHERE delete_epoch IS NULL
    $$, view_name, label_value_cols);

    IF NOT view_exists THEN
        EXECUTE FORMAT('GRANT SELECT ON SCHEMA_SERIES.%1$I TO prom_reader', view_name);
    END IF;
    RETURN true;
END
$func$
LANGUAGE PLPGSQL VOLATILE
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON FUNCTION SCHEMA_CATALOG.create_series_view(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.create_series_view(text) TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.create_metric_view(
        metric_name text)
    RETURNS BOOLEAN
AS $func$
DECLARE
   label_value_cols text;
   table_name text;
   metric_id int;
   view_exists boolean;
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
    WHERE m.metric_name = create_metric_view.metric_name
    AND m.table_schema = 'SCHEMA_DATA';

    SELECT COUNT(*) > 0 into view_exists
    FROM pg_class
    WHERE
      relname = table_name AND
      relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'SCHEMA_METRIC');

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

    IF NOT view_exists THEN
        EXECUTE FORMAT('GRANT SELECT ON SCHEMA_METRIC.%1$I TO prom_reader', table_name);
    END IF;

    RETURN true;
END
$func$
LANGUAGE PLPGSQL VOLATILE
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON FUNCTION SCHEMA_CATALOG.create_metric_view(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.create_metric_view(text) TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.register_metric_view(schema_name name, view_name name, if_not_exists BOOLEAN = false)
    RETURNS BOOLEAN
AS $func$
DECLARE
   agg_schema name;
   agg_name name;
   metric_table_name name;
   column_count int;
BEGIN
    -- check if table/view exists
    PERFORM * FROM information_schema.tables
    WHERE  table_schema = register_metric_view.schema_name
    AND    table_name   = register_metric_view.view_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'cannot register non-existant metric view in specified schema';
    END IF;

    -- cannot register view in data schema
    IF schema_name = 'SCHEMA_DATA' THEN
        RAISE EXCEPTION 'cannot register metric view in SCHEMA_DATA schema';
    END IF;

    -- check if view is based on a metric from prom_data
    -- we check for two levels so we can support 2-step continuous aggregates
    SELECT v.view_schema, v.view_name, v.metric_table_name
    INTO agg_schema, agg_name, metric_table_name
    FROM SCHEMA_CATALOG.get_first_level_view_on_metric(schema_name, view_name) v;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'view not based on a metric table from SCHEMA_DATA schema';
    END IF;

    -- check if the view contains necessary columns with the correct types
    SELECT count(*) FROM information_schema.columns
    INTO column_count
    WHERE table_schema = register_metric_view.schema_name
    AND table_name   = register_metric_view.view_name
    AND ((column_name = 'time' AND data_type = 'timestamp with time zone')
    OR (column_name = 'series_id' AND data_type = 'bigint')
    OR data_type = 'double precision');

    IF column_count < 3 THEN
        RAISE EXCEPTION 'view must contain time (data type: timestamp with time zone), series_id (data type: bigint), and at least one column with double precision data type';
    END IF;

    -- insert into metric table
    INSERT INTO SCHEMA_CATALOG.metric (metric_name, table_name, table_schema, series_table, is_view, creation_completed)
    VALUES (register_metric_view.view_name, register_metric_view.view_name, register_metric_view.schema_name, metric_table_name, true, true)
    ON CONFLICT DO NOTHING;

    IF NOT FOUND THEN
        IF register_metric_view.if_not_exists THEN
            RAISE NOTICE 'metric with same name and schema already exists';
            RETURN FALSE;
        ELSE
            RAISE EXCEPTION 'metric with the same name and schema already exists, could not register';
        END IF;
    END IF;

    EXECUTE format('GRANT USAGE ON SCHEMA %I TO prom_reader', register_metric_view.schema_name);
    EXECUTE format('GRANT SELECT ON TABLE %I.%I TO prom_reader', register_metric_view.schema_name, register_metric_view.view_name);
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO prom_reader', agg_schema);
    EXECUTE format('GRANT SELECT ON TABLE %I.%I TO prom_reader', agg_schema, agg_name);

    PERFORM *
    FROM SCHEMA_CATALOG.get_storage_hypertable_info(agg_schema, agg_name, true);

    RETURN true;
END
$func$
LANGUAGE PLPGSQL VOLATILE
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON FUNCTION SCHEMA_PROM.register_metric_view(name, name, boolean) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.register_metric_view(name, name, boolean) TO prom_admin;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.unregister_metric_view(schema_name name, view_name name, if_exists BOOLEAN = false)
    RETURNS BOOLEAN
AS $func$
DECLARE
   metric_table_name name;
   column_count int;
BEGIN
    DELETE FROM SCHEMA_CATALOG.metric
    WHERE unregister_metric_view.schema_name = table_schema
    AND unregister_metric_view.view_name = table_name
    AND is_view = TRUE;

    IF NOT FOUND THEN
        IF unregister_metric_view.if_exists THEN
            RAISE NOTICE 'metric with specified name and schema does not exist';
            RETURN FALSE;
        ELSE
            RAISE EXCEPTION 'metric with specified name and schema does not exist, could not unregister';
        END IF;
    END IF;

    RETURN TRUE;
END
$func$
LANGUAGE PLPGSQL VOLATILE
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON FUNCTION SCHEMA_PROM.unregister_metric_view(name, name, boolean) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.unregister_metric_view(name, name, boolean) TO prom_admin;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.delete_series_from_metric(name text, series_ids bigint[])
RETURNS BIGINT
AS
$$
DECLARE
    metric_table name;
    delete_stmt text;
    delete_query text;
    rows_affected bigint;
    num_rows_deleted bigint := 0;
BEGIN
    SELECT table_name INTO metric_table FROM SCHEMA_CATALOG.metric m WHERE m.metric_name=name AND m.is_view = false;
    IF SCHEMA_CATALOG.is_timescaledb_installed() THEN
        FOR delete_stmt IN
            SELECT FORMAT('DELETE FROM %1$I.%2$I WHERE series_id = ANY($1)', schema_name, table_name)
            FROM (
                SELECT (COALESCE(chc, ch)).* FROM pg_class c
                    INNER JOIN pg_namespace n ON c.relnamespace = n.oid
                    INNER JOIN _timescaledb_catalog.chunk ch ON (ch.schema_name, ch.table_name) = (n.nspname, c.relname)
                    LEFT JOIN _timescaledb_catalog.chunk chc ON ch.compressed_chunk_id = chc.id
                WHERE c.oid IN (SELECT SCHEMA_TIMESCALE.show_chunks(format('%I.%I','SCHEMA_DATA', metric_table))::oid)
                ) a
        LOOP
            EXECUTE delete_stmt USING series_ids;
            GET DIAGNOSTICS rows_affected = ROW_COUNT;
            num_rows_deleted = num_rows_deleted + rows_affected;
        END LOOP;
    ELSE
        EXECUTE FORMAT('DELETE FROM SCHEMA_DATA.%1$I WHERE series_id = ANY($1)', metric_table) USING series_ids;
        GET DIAGNOSTICS rows_affected = ROW_COUNT;
        num_rows_deleted = num_rows_deleted + rows_affected;
    END IF;
    PERFORM SCHEMA_CATALOG.delete_series_catalog_row(metric_table, series_ids);
    RETURN num_rows_deleted;
END;
$$
LANGUAGE PLPGSQL VOLATILE
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON FUNCTION SCHEMA_CATALOG.delete_series_from_metric(text, bigint[])FROM PUBLIC;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.delete_series_from_metric(text, bigint[]) to prom_modifier;

-- the following functions require timescaledb >= 2.0.0
DO $block$
BEGIN
    IF SCHEMA_CATALOG.is_timescaledb_installed() AND SCHEMA_CATALOG.get_timescale_major_version() >= 2 THEN

        CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.hypertable_local_size(schema_name_in name)
        RETURNS TABLE(hypertable_name name, table_bytes bigint, index_bytes bigint, toast_bytes bigint, total_bytes bigint)
        AS $function$
        BEGIN
            IF SCHEMA_CATALOG.get_timescale_minor_version() < 3 THEN
                -- two columns in _timescaledb_internal.hypertable_chunk_local_size were renamed in version 2.2
                -- schema_name -> hypertable_schema
                -- table_name -> hypertable_name
                -- we explicit aliases to deal with this
                RETURN QUERY
                SELECT
                    ch.hypertable_name,
                    (COALESCE(sum(ch.total_bytes), 0) - COALESCE(sum(ch.index_bytes), 0) - COALESCE(sum(ch.toast_bytes), 0) + COALESCE(sum(ch.compressed_heap_size), 0))::bigint + pg_relation_size(format('%I.%I', ch.hypertable_schema, ch.hypertable_name)::regclass)::bigint AS heap_bytes,
                    (COALESCE(sum(ch.index_bytes), 0) + COALESCE(sum(ch.compressed_index_size), 0))::bigint + pg_indexes_size(format('%I.%I', ch.hypertable_schema, ch.hypertable_name)::regclass)::bigint AS index_bytes,
                    (COALESCE(sum(ch.toast_bytes), 0) + COALESCE(sum(ch.compressed_toast_size), 0))::bigint AS toast_bytes,
                    (COALESCE(sum(ch.total_bytes), 0) + COALESCE(sum(ch.compressed_heap_size), 0) + COALESCE(sum(ch.compressed_index_size), 0) + COALESCE(sum(ch.compressed_toast_size), 0))::bigint + pg_total_relation_size(format('%I.%I', ch.hypertable_schema, ch.hypertable_name)::regclass)::bigint AS total_bytes
                FROM _timescaledb_internal.hypertable_chunk_local_size ch
                (
                    hypertable_schema,
                    hypertable_name,
                    hypertable_id,
                    chunk_id,
                    chunk_schema,
                    chunk_name,
                    total_bytes,
                    index_bytes,
                    toast_bytes,
                    compressed_heap_size,
                    compressed_index_size,
                    compressed_toast_size
                )
                WHERE ch.hypertable_schema = schema_name_in
                GROUP BY ch.hypertable_name, ch.hypertable_schema;
            ELSE
                RETURN QUERY
                SELECT
                    ch.hypertable_name,
                    (COALESCE(sum(ch.total_bytes), 0) - COALESCE(sum(ch.index_bytes), 0) - COALESCE(sum(ch.toast_bytes), 0) + COALESCE(sum(ch.compressed_heap_size), 0))::bigint + pg_relation_size(format('%I.%I', ch.hypertable_schema, ch.hypertable_name)::regclass)::bigint AS heap_bytes,
                    (COALESCE(sum(ch.index_bytes), 0) + COALESCE(sum(ch.compressed_index_size), 0))::bigint + pg_indexes_size(format('%I.%I', ch.hypertable_schema, ch.hypertable_name)::regclass)::bigint AS index_bytes,
                    (COALESCE(sum(ch.toast_bytes), 0) + COALESCE(sum(ch.compressed_toast_size), 0))::bigint AS toast_bytes,
                    (COALESCE(sum(ch.total_bytes), 0) + COALESCE(sum(ch.compressed_heap_size), 0) + COALESCE(sum(ch.compressed_index_size), 0) + COALESCE(sum(ch.compressed_toast_size), 0))::bigint + pg_total_relation_size(format('%I.%I', ch.hypertable_schema, ch.hypertable_name)::regclass)::bigint AS total_bytes
                FROM _timescaledb_internal.hypertable_chunk_local_size ch
                (
                    hypertable_schema,
                    hypertable_name,
                    hypertable_id,
                    chunk_id,
                    chunk_schema,
                    chunk_name,
                    total_bytes,
                    index_bytes,
                    toast_bytes,
                    compressed_total_size,
                    compressed_index_size,
                    compressed_toast_size,
                    compressed_heap_size
                )
                WHERE ch.hypertable_schema = schema_name_in
                GROUP BY ch.hypertable_name, ch.hypertable_schema;
            END IF;
        END;
        $function$
        LANGUAGE plpgsql STRICT STABLE
        SECURITY DEFINER
        --search path must be set for security definer
        SET search_path = pg_temp;
        REVOKE ALL ON FUNCTION SCHEMA_CATALOG.hypertable_local_size(name) FROM PUBLIC;
        GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.hypertable_local_size(name) to prom_reader;

        CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.hypertable_node_up(schema_name_in name)
        RETURNS TABLE(hypertable_name name, node_name name, node_up boolean)
        AS $function$
            -- list of distributed hypertables and whether or not the associated data node is up
            -- only ping each distinct data node once and no more
            -- there is no guarantee that a node will stay "up" for the duration of a transaction
            -- but we don't want to pay the penalty of asking more than once, so we mark this
            -- function as stable to allow the results to be cached
            WITH dht AS MATERIALIZED (
                -- list of distributed hypertables
                SELECT
                    ht.table_name,
                    s.node_name
                FROM _timescaledb_catalog.hypertable ht
                JOIN _timescaledb_catalog.hypertable_data_node s ON (
                    ht.replication_factor > 0 AND s.hypertable_id = ht.id
                )
                WHERE ht.schema_name = schema_name_in
            ),
            up AS MATERIALIZED (
                -- list of nodes we care about and whether they are up
                SELECT
                    x.node_name,
                    _timescaledb_internal.ping_data_node(x.node_name) AS node_up
                FROM (
                    SELECT DISTINCT dht.node_name -- only ping each node once
                    FROM dht
                ) x
            )
            SELECT
                dht.table_name as hypertable_name,
                dht.node_name,
                up.node_up
            FROM dht
            JOIN up ON (dht.node_name = up.node_name)
        $function$
        LANGUAGE sql
        STRICT STABLE
        SECURITY DEFINER
        --search path must be set for security definer
        SET search_path = pg_temp;
        REVOKE ALL ON FUNCTION SCHEMA_CATALOG.hypertable_node_up(name) FROM PUBLIC;
        GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.hypertable_node_up(name) to prom_reader;

        CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.hypertable_remote_size(schema_name_in name)
        RETURNS TABLE(hypertable_name name, table_bytes bigint, index_bytes bigint, toast_bytes bigint, total_bytes bigint)
        AS $function$
            SELECT
                dht.hypertable_name,
                sum(x.table_bytes)::bigint AS table_bytes,
                sum(x.index_bytes)::bigint AS index_bytes,
                sum(x.toast_bytes)::bigint AS toast_bytes,
                sum(x.total_bytes)::bigint AS total_bytes
            FROM SCHEMA_CATALOG.hypertable_node_up(schema_name_in) dht
            LEFT OUTER JOIN LATERAL _timescaledb_internal.data_node_hypertable_info(
                CASE WHEN dht.node_up THEN
                    dht.node_name
                ELSE
                    NULL
                END, schema_name_in, dht.hypertable_name) x ON true
            GROUP BY dht.hypertable_name
        $function$
        LANGUAGE sql
        STRICT STABLE
        SECURITY DEFINER
        --search path must be set for security definer
        SET search_path = pg_temp;
        REVOKE ALL ON FUNCTION SCHEMA_CATALOG.hypertable_remote_size(name) FROM PUBLIC;
        GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.hypertable_remote_size(name) to prom_reader;

        -- two columns in _timescaledb_internal.compressed_chunk_stats were renamed in version 2.2
        -- schema_name -> hypertable_schema
        -- table_name -> hypertable_name
        -- we use explicit column aliases on _timescaledb_internal.compressed_chunk_stats to rename the
        -- `schema_name` and `table_name` in the older versions to the new names
        CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.hypertable_compression_stats_for_schema(schema_name_in name)
        RETURNS TABLE(hypertable_name name, total_chunks bigint, number_compressed_chunks bigint, before_compression_total_bytes bigint, after_compression_total_bytes bigint)
        AS $function$
            SELECT
                x.hypertable_name,
                count(*)::bigint AS total_chunks,
                (count(*) FILTER (WHERE x.compression_status = 'Compressed'))::bigint AS number_compressed_chunks,
                sum(x.before_compression_total_bytes)::bigint AS before_compression_total_bytes,
                sum(x.after_compression_total_bytes)::bigint AS after_compression_total_bytes
            FROM
            (
                -- local hypertables
                SELECT
                    ch.hypertable_name,
                    ch.compression_status,
                    ch.uncompressed_total_size AS before_compression_total_bytes,
                    ch.compressed_total_size AS after_compression_total_bytes,
                    NULL::text AS node_name
                FROM _timescaledb_internal.compressed_chunk_stats ch
                (
                    hypertable_schema,
                    hypertable_name,
                    chunk_schema,
                    chunk_name,
                    compression_status,
                    uncompressed_heap_size,
                    uncompressed_index_size,
                    uncompressed_toast_size,
                    uncompressed_total_size,
                    compressed_heap_size,
                    compressed_index_size,
                    compressed_toast_size,
                    compressed_total_size
                )
                WHERE ch.hypertable_schema = schema_name_in
                UNION ALL
                -- distributed hypertables
                SELECT
                    dht.hypertable_name,
                    ch.compression_status,
                    ch.before_compression_total_bytes,
                    ch.after_compression_total_bytes,
                    dht.node_name
                FROM SCHEMA_CATALOG.hypertable_node_up(schema_name_in) dht
                LEFT OUTER JOIN LATERAL _timescaledb_internal.data_node_compressed_chunk_stats (
                    CASE WHEN dht.node_up THEN
                        dht.node_name
                    ELSE
                        NULL
                    END, schema_name_in, dht.hypertable_name) ch ON true
                WHERE ch.chunk_name IS NOT NULL
            ) x
            GROUP BY x.hypertable_name
        $function$
        LANGUAGE sql
        STRICT STABLE
        SECURITY DEFINER
        --search path must be set for security definer
        SET search_path = pg_temp;
        REVOKE ALL ON FUNCTION SCHEMA_CATALOG.hypertable_compression_stats_for_schema(name) FROM PUBLIC;
        GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.hypertable_compression_stats_for_schema(name) to prom_reader;

    END IF;
END;
$block$
;

--------------------------------- Views --------------------------------

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.metric_view()
RETURNS TABLE(id int, metric_name text, table_name name, label_keys text[], retention_period interval,
              chunk_interval interval, compressed_interval interval, total_interval interval,
              before_compression_bytes bigint, after_compression_bytes bigint,
              total_size_bytes bigint, total_size text, compression_ratio numeric,
              total_chunks bigint, compressed_chunks bigint)
AS $func$
BEGIN
        IF NOT SCHEMA_CATALOG.is_timescaledb_installed() THEN
            RETURN QUERY
                SELECT
                   i.id,
                   i.metric_name,
                   i.table_name,
                   i.label_keys,
                   i.retention_period,
                   i.chunk_interval,
                   NULL::interval compressed_interval,
                   NULL::interval total_interval,
                   pg_size_bytes(i.total_size) as before_compression_bytes,
                   NULL::bigint as after_compression_bytes,
                   pg_size_bytes(i.total_size) as total_size_bytes,
                   i.total_size,
                   i.compression_ratio,
                   i.total_chunks,
                   i.compressed_chunks
                FROM
                (
                    SELECT
                        m.id,
                        m.metric_name,
                        m.table_name,
                        ARRAY(
                            SELECT key
                            FROM SCHEMA_CATALOG.label_key_position lkp
                            WHERE lkp.metric_name = m.metric_name
                            ORDER BY key) label_keys,
                        SCHEMA_CATALOG.get_metric_retention_period(m.table_schema, m.metric_name) as retention_period,
                        NULL::interval as chunk_interval,
                        pg_size_pretty(pg_total_relation_size(format('SCHEMA_DATA.%I', m.table_name)::regclass)) as total_size,
                        0.0 as compression_ratio,
                        NULL::bigint as total_chunks,
                        NULL::bigint as compressed_chunks
                    FROM SCHEMA_CATALOG.metric m
                ) AS i;
            RETURN;
        END IF;

        IF SCHEMA_CATALOG.get_timescale_major_version() >= 2 THEN
            RETURN QUERY
            WITH ci AS (
                SELECT
                    hypertable_name as hypertable_name,
                    COALESCE(SUM(range_end-range_start) FILTER(WHERE is_compressed), INTERVAL '0') AS compressed_interval,
                    COALESCE(SUM(range_end-range_start), INTERVAL '0') AS total_interval
                FROM timescaledb_information.chunks c
                WHERE hypertable_schema='SCHEMA_DATA'
                GROUP BY hypertable_schema, hypertable_name
            )
            SELECT
                m.id,
                m.metric_name,
                m.table_name,
                ARRAY(
                    SELECT key
                    FROM SCHEMA_CATALOG.label_key_position lkp
                    WHERE lkp.metric_name = m.metric_name
                    ORDER BY key) label_keys,
                SCHEMA_CATALOG.get_metric_retention_period(m.table_schema, m.metric_name) as retention_period,
                dims.time_interval as chunk_interval,
                ci.compressed_interval,
                ci.total_interval,
                hcs.before_compression_total_bytes::bigint,
                hcs.after_compression_total_bytes::bigint,
                hs.total_bytes::bigint as total_size_bytes,
                pg_size_pretty(hs.total_bytes::bigint) as total_size,
                (1.0 - (hcs.after_compression_total_bytes::NUMERIC / hcs.before_compression_total_bytes::NUMERIC)) * 100 as compression_ratio,
                hcs.total_chunks::BIGINT,
                hcs.number_compressed_chunks::BIGINT as compressed_chunks
            FROM SCHEMA_CATALOG.metric m
            LEFT JOIN
            (
                SELECT
                  x.hypertable_name
                , sum(x.total_bytes::bigint) as total_bytes
                FROM
                (
                    SELECT *
                    FROM SCHEMA_CATALOG.hypertable_local_size('SCHEMA_DATA')
                    UNION ALL
                    SELECT *
                    FROM SCHEMA_CATALOG.hypertable_remote_size('SCHEMA_DATA')
                ) x
                GROUP BY x.hypertable_name
            ) hs ON (hs.hypertable_name = m.table_name)
            LEFT JOIN timescaledb_information.dimensions dims ON
                (dims.hypertable_schema = 'SCHEMA_DATA' AND dims.hypertable_name = m.table_name)
            LEFT JOIN SCHEMA_CATALOG.hypertable_compression_stats_for_schema('SCHEMA_DATA') hcs ON (hcs.hypertable_name = m.table_name)
            LEFT JOIN ci ON (ci.hypertable_name = m.table_name)
            ;
        ELSE
            RETURN QUERY
                SELECT
                    m.id,
                    m.metric_name,
                    m.table_name,
                    ARRAY(
                        SELECT key
                            FROM _prom_catalog.label_key_position lkp
                        WHERE lkp.metric_name = m.metric_name
                        ORDER BY key
                    ) label_keys,
                    _prom_catalog.get_metric_retention_period(m.table_schema, m.metric_name) as retention_period,
                    (
                        SELECT _timescaledb_internal.to_interval(interval_length)
                            FROM _timescaledb_catalog.dimension d
                        WHERE d.hypertable_id = h.id
                        ORDER BY d.id ASC LIMIT 1
                    ) as chunk_interval,
                    ci.compressed_interval,
                    ci.total_interval,
                    pg_size_bytes(chs.uncompressed_total_bytes) as before_compression_bytes,
                    pg_size_bytes(chs.compressed_total_bytes) as after_compression_bytes,
                    pg_size_bytes(hi.total_size) as total_bytes,
                    hi.total_size as total_size,
                    (1.0 - (pg_size_bytes(chs.compressed_total_bytes)::numeric / pg_size_bytes(chs.uncompressed_total_bytes)::numeric)) * 100 as compression_ratio,
                    chs.total_chunks,
                    chs.number_compressed_chunks as compressed_chunks
                FROM _prom_catalog.metric m
                    LEFT JOIN timescaledb_information.hypertable hi ON
                        (hi.table_schema = 'prom_data' AND hi.table_name = m.table_name)
                    LEFT JOIN timescaledb_information.compressed_hypertable_stats chs ON
                        (chs.hypertable_name = format('%I.%I', 'prom_data', m.table_name)::regclass)
                    LEFT JOIN _timescaledb_catalog.hypertable h ON
                        (h.schema_name = 'prom_data' AND h.table_name = m.table_name)
                    LEFT JOIN LATERAL
                        (
                            SELECT COALESCE(
                                    SUM(
                                        UPPER(rs.ranges[1]::TSTZRANGE) - LOWER(rs.ranges[1]::TSTZRANGE)
                                    ),
                                    INTERVAL '0'
                                ) total_interval,
                                COALESCE(
                                    SUM(
                                        UPPER(rs.ranges[1]::TSTZRANGE) - LOWER(rs.ranges[1]::TSTZRANGE)
                                    ) FILTER (WHERE cs.compression_status = 'Compressed'),
                                    INTERVAL '0'
                                ) compressed_interval
                            FROM SCHEMA_TIMESCALE.chunk_relation_size_pretty(FORMAT('prom_data.%I', m.table_name)) rs
                            LEFT JOIN timescaledb_information.compressed_chunk_stats cs ON
                                (cs.chunk_name::text = rs.chunk_table::text)
                        ) as ci ON TRUE;
        END IF;
END
$func$
LANGUAGE PLPGSQL STABLE
SECURITY DEFINER
--search path must be set for security definer
--need to include public(the timescaledb schema) for some timescale functions to work.
SET search_path = public, pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON FUNCTION SCHEMA_CATALOG.metric_view() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.metric_view() TO prom_reader;

CREATE OR REPLACE VIEW SCHEMA_INFO.metric AS
   SELECT
     *
    FROM SCHEMA_CATALOG.metric_view();
GRANT SELECT ON SCHEMA_INFO.metric TO prom_reader;

CREATE OR REPLACE VIEW SCHEMA_INFO.label AS
    SELECT
        lk.key,
        lk.value_column_name,
        lk.id_column_name,
        va.values as values,
        cardinality(va.values) as num_values
    FROM SCHEMA_CATALOG.label_key lk
    INNER JOIN LATERAL(SELECT key, array_agg(value ORDER BY value) as values FROM SCHEMA_CATALOG.label GROUP BY key)
    AS va ON (va.key = lk.key) ORDER BY num_values DESC;
GRANT SELECT ON SCHEMA_INFO.label TO prom_reader;

CREATE OR REPLACE VIEW SCHEMA_INFO.system_stats AS
    SELECT
    (
        SELECT SCHEMA_CATALOG.safe_approximate_row_count('_prom_catalog.series'::REGCLASS)
    ) AS num_series_approx,
    (
        SELECT count(*) FROM SCHEMA_CATALOG.metric
    ) AS num_metric,
    (
        SELECT count(*) FROM SCHEMA_CATALOG.label_key
    ) AS num_label_keys,
    (
        SELECT count(*) FROM SCHEMA_CATALOG.label
    ) AS num_labels;
GRANT SELECT ON SCHEMA_INFO.system_stats TO prom_reader;

CREATE OR REPLACE VIEW SCHEMA_INFO.metric_stats AS
    SELECT metric_name,
    SCHEMA_CATALOG.safe_approximate_row_count(format('prom_series.%I', table_name)::regclass) AS num_series_approx,
    (SELECT SCHEMA_CATALOG.safe_approximate_row_count(format('prom_data.%I',table_name)::regclass)) AS num_samples_approx
    FROM SCHEMA_CATALOG.metric ORDER BY metric_name;
GRANT SELECT ON SCHEMA_INFO.metric_stats TO prom_reader;

--this should the only thing run inside the transaction. It's important the txn ends after calling this function
--to release locks
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.delay_compression_job(ht_table name, new_start timestamptz) RETURNS VOID
AS $$
DECLARE
    bgw_job_id int;
BEGIN
    UPDATE SCHEMA_CATALOG.metric m
    SET delay_compression_until = new_start
    WHERE table_name = ht_table;

    IF SCHEMA_CATALOG.get_timescale_major_version() < 2 THEN
        SELECT job_id INTO bgw_job_id
        FROM _timescaledb_config.bgw_policy_compress_chunks p
        INNER JOIN _timescaledb_catalog.hypertable h ON (h.id = p.hypertable_id)
        WHERE h.schema_name = 'SCHEMA_DATA' and h.table_name = ht_table;

        --alter job schedule is not currently concurrency-safe (timescaledb issue #2165)
        PERFORM pg_advisory_xact_lock(ADVISORY_LOCK_PREFIX_JOB, bgw_job_id);

        PERFORM SCHEMA_TIMESCALE.alter_job_schedule(bgw_job_id, next_start=>GREATEST(new_start, (SELECT next_start FROM timescaledb_information.policy_stats WHERE job_id = bgw_job_id)));
    END IF;
END
$$
LANGUAGE PLPGSQL
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON FUNCTION SCHEMA_CATALOG.delay_compression_job(name, timestamptz) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.delay_compression_job(name, timestamptz) TO prom_writer;

CALL SCHEMA_CATALOG.execute_everywhere('SCHEMA_CATALOG.do_decompress_chunks_after', $ee$
DO $DO$
BEGIN
    --this function isolates the logic that needs to be security definer
    --cannot fold it into do_decompress_chunks_after because cannot have security
    --definer do txn-al stuff like commit
    CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.decompress_chunk_for_metric(metric_table TEXT, chunk_schema_name name, chunk_table_name name) RETURNS VOID
    AS $$
    DECLARE
        chunk_full_name text;
    BEGIN

       --double check chunk belongs to metric table
       SELECT
        format('%I.%I', c.schema_name, c.table_name)
       INTO chunk_full_name
       FROM _timescaledb_catalog.chunk c
       INNER JOIN  _timescaledb_catalog.hypertable h ON (h.id = c.hypertable_id)
       WHERE
            c.schema_name = chunk_schema_name AND c.table_name = chunk_table_name AND
            h.schema_name = 'SCHEMA_DATA' AND h.table_name = metric_table AND
            c.compressed_chunk_id IS NOT NULL;

        IF NOT FOUND Then
            RETURN;
        END IF;

       --lock the chunk exclusive.
       EXECUTE format('LOCK %I.%I;', chunk_schema_name, chunk_table_name);

       --double check it's still compressed.
       PERFORM c.*
       FROM _timescaledb_catalog.chunk c
       WHERE schema_name = chunk_schema_name AND table_name = chunk_table_name AND
       c.compressed_chunk_id IS NOT NULL;

       IF NOT FOUND Then
          RETURN;
       END IF;

       RAISE NOTICE 'Promscale is decompressing chunk: %.%', chunk_schema_name, chunk_table_name;
       PERFORM SCHEMA_TIMESCALE.decompress_chunk(chunk_full_name);
    END;
    $$
    LANGUAGE PLPGSQL
    SECURITY DEFINER
    --search path must be set for security definer
    SET search_path = pg_temp;
    REVOKE ALL ON FUNCTION SCHEMA_CATALOG.decompress_chunk_for_metric(TEXT, name, name) FROM PUBLIC;
    GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.decompress_chunk_for_metric(TEXT, name, name) TO prom_writer;


    --Decompression should take place in a procedure because we don't want locks held across
    --decompress_chunk calls since that function takes some heavier locks at the end.
    --Thus, transactional parameter should usually be false
    CREATE OR REPLACE PROCEDURE SCHEMA_CATALOG.do_decompress_chunks_after(metric_table NAME, min_time TIMESTAMPTZ, transactional BOOLEAN = false)
    AS $$
    DECLARE
        chunk_row record;
        dimension_row record;
        hypertable_row record;
        min_time_internal bigint;
    BEGIN
        SELECT h.* INTO STRICT hypertable_row FROM _timescaledb_catalog.hypertable h
        WHERE table_name = metric_table AND schema_name = 'SCHEMA_DATA';

        SELECT d.* INTO STRICT dimension_row FROM _timescaledb_catalog.dimension d WHERE hypertable_id = hypertable_row.id ORDER BY id LIMIT 1;

        IF min_time = timestamptz '-Infinity' THEN
            min_time_internal := -9223372036854775808;
        ELSE
           SELECT _timescaledb_internal.time_to_internal(min_time) INTO STRICT min_time_internal;
        END IF;

        FOR chunk_row IN
            SELECT c.*
            FROM _timescaledb_catalog.dimension_slice ds
            INNER JOIN _timescaledb_catalog.chunk_constraint cc ON cc.dimension_slice_id = ds.id
            INNER JOIN _timescaledb_catalog.chunk c ON cc.chunk_id = c.id
            WHERE dimension_id = dimension_row.id
            -- the range_ends are non-inclusive
            AND min_time_internal < ds.range_end
            AND c.compressed_chunk_id IS NOT NULL
            ORDER BY ds.range_start
        LOOP
            PERFORM SCHEMA_CATALOG.decompress_chunk_for_metric(metric_table, chunk_row.schema_name, chunk_row.table_name);
            IF NOT transactional THEN
              COMMIT;
            END IF;
        END LOOP;
    END;
    $$ LANGUAGE PLPGSQL;
    GRANT EXECUTE ON PROCEDURE SCHEMA_CATALOG.do_decompress_chunks_after(NAME, TIMESTAMPTZ, BOOLEAN) TO prom_writer;
END
$DO$;
$ee$);

CREATE OR REPLACE PROCEDURE SCHEMA_CATALOG.decompress_chunks_after(metric_table NAME, min_time TIMESTAMPTZ, transactional BOOLEAN = false)
AS $proc$
BEGIN
    -- In early versions of timescale multinode the access node catalog does not
    -- store whether chunks were compressed, so we need to run the actual search
    -- for nodes in need of decompression on the data nodes, and /not/ the
    -- access node; right now executing on the access node will do a lot of work
    -- and locking for no result.
    IF SCHEMA_CATALOG.is_multinode() THEN
        CALL SCHEMA_TIMESCALE.distributed_exec(
            format(
                $dist$ CALL SCHEMA_CATALOG.do_decompress_chunks_after(%L, %L, %L) $dist$,
                metric_table, min_time, transactional),
            transactional => false);
    ELSE
        CALL SCHEMA_CATALOG.do_decompress_chunks_after(metric_table, min_time, transactional);
    END IF;
END
$proc$ LANGUAGE PLPGSQL;
GRANT EXECUTE ON PROCEDURE SCHEMA_CATALOG.decompress_chunks_after(name, TIMESTAMPTZ, boolean) TO prom_writer;

CALL SCHEMA_CATALOG.execute_everywhere('SCHEMA_CATALOG.compress_old_chunks', $ee$
DO $DO$
BEGIN
    --this function isolates the logic that needs to be security definer
    --cannot fold it into compress_old_chunks because cannot have security
    --definer do txn-all stuff like commit
    CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.compress_chunk_for_metric(metric_table TEXT, chunk_schema_name name, chunk_table_name name) RETURNS VOID
    AS $$
    DECLARE
        chunk_full_name text;
    BEGIN
        SELECT
            format('%I.%I', chunk_schema, chunk_name)
        INTO chunk_full_name
        FROM timescaledb_information.chunks
        WHERE hypertable_schema = 'SCHEMA_DATA'
          AND hypertable_name = metric_table
          AND chunk_schema = chunk_schema_name
          AND chunk_name = chunk_table_name;

        PERFORM SCHEMA_TIMESCALE.compress_chunk(chunk_full_name, if_not_compressed => true);
    END;
    $$
    LANGUAGE PLPGSQL
    SECURITY DEFINER
    --search path must be set for security definer
    SET search_path = pg_temp;
    REVOKE ALL ON FUNCTION SCHEMA_CATALOG.compress_chunk_for_metric(TEXT, name, name) FROM PUBLIC;
    GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.compress_chunk_for_metric(TEXT, name, name) TO prom_maintenance;

    CREATE OR REPLACE PROCEDURE SCHEMA_CATALOG.compress_old_chunks(metric_table TEXT, compress_before TIMESTAMPTZ)
    AS $$
    DECLARE
        chunk_schema_name name;
        chunk_table_name name;
        chunk_num INT;
    BEGIN
        FOR chunk_schema_name, chunk_table_name, chunk_num IN
            SELECT
                chunk_schema,
                chunk_name,
                row_number() OVER (ORDER BY range_end DESC)
            FROM timescaledb_information.chunks
            WHERE hypertable_schema = 'SCHEMA_DATA'
                AND hypertable_name = metric_table
                AND NOT is_compressed
                AND range_end <= compress_before
            ORDER BY range_end ASC
        LOOP
            CONTINUE WHEN chunk_num <= 1;
            PERFORM SCHEMA_CATALOG.compress_chunk_for_metric(metric_table, chunk_schema_name, chunk_table_name);
            COMMIT;
        END LOOP;
    END;
    $$ LANGUAGE PLPGSQL;
    GRANT EXECUTE ON PROCEDURE SCHEMA_CATALOG.compress_old_chunks(TEXT, TIMESTAMPTZ) TO prom_maintenance;
END
$DO$;
$ee$);

CREATE OR REPLACE PROCEDURE SCHEMA_CATALOG.compress_metric_chunks(metric_name TEXT)
AS $$
DECLARE
  metric_table NAME;
BEGIN
    SELECT table_name
    INTO STRICT metric_table
    FROM SCHEMA_CATALOG.get_metric_table_name_if_exists('SCHEMA_DATA', metric_name);

    -- as of timescaledb-2.0-rc4 the is_compressed column of the chunks view is
    -- not updated on the access node, therefore we need to one the compressor
    -- on all the datanodes to search for uncompressed chunks
    IF SCHEMA_CATALOG.is_multinode() THEN
        CALL SCHEMA_TIMESCALE.distributed_exec(format($dist$
            CALL SCHEMA_CATALOG.compress_old_chunks(%L, now() - INTERVAL '1 hour')
        $dist$, metric_table), transactional => false);
    ELSE
        CALL SCHEMA_CATALOG.compress_old_chunks(metric_table, now() - INTERVAL '1 hour');
    END IF;
END
$$ LANGUAGE PLPGSQL;
GRANT EXECUTE ON PROCEDURE SCHEMA_CATALOG.compress_metric_chunks(text) TO prom_maintenance;

--Order by random with stable marking gives us same order in a statement and different
-- orderings in different statements
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_metrics_that_need_compression()
RETURNS SETOF SCHEMA_CATALOG.metric
AS $$
DECLARE
BEGIN
        RETURN QUERY
        SELECT m.*
        FROM SCHEMA_CATALOG.metric m
        WHERE
          is_view = false AND
          SCHEMA_CATALOG.get_metric_compression_setting(m.metric_name) AND
          delay_compression_until IS NULL OR delay_compression_until < now() AND
          is_view = FALSE
        ORDER BY random();
END
$$
LANGUAGE PLPGSQL STABLE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_metrics_that_need_compression() TO prom_maintenance;

--only for timescaledb 2.0 in 1.x we use compression policies
CREATE OR REPLACE PROCEDURE SCHEMA_CATALOG.execute_compression_policy(log_verbose boolean = false)
AS $$
DECLARE
    r SCHEMA_CATALOG.metric;
    remaining_metrics SCHEMA_CATALOG.metric[] DEFAULT '{}';
    startT TIMESTAMPTZ;
    lockStartT TIMESTAMPTZ;
BEGIN
    --Do one loop with metric that could be locked without waiting.
    --This allows you to do everything you can while avoiding lock contention.
    --Then come back for the metrics that would have needed to wait on the lock.
    --Hopefully, that lock is now freed. The secoond loop waits for the lock
    --to prevent starvation.
    FOR r IN
        SELECT *
        FROM SCHEMA_CATALOG.get_metrics_that_need_compression()
    LOOP
        IF NOT SCHEMA_CATALOG.lock_metric_for_maintenance(r.id, wait=>false) THEN
            remaining_metrics := remaining_metrics || r;
            CONTINUE;
        END IF;
        IF log_verbose THEN
            startT := clock_timestamp();
            RAISE LOG 'promscale maintenance: compression: metric %: starting, without lock wait', r.metric_name;
        END IF;
        PERFORM SCHEMA_CATALOG.set_app_name( format('promscale maintenance: compression: metric %s', r.metric_name));
        CALL SCHEMA_CATALOG.compress_metric_chunks(r.metric_name);
        IF log_verbose THEN
            RAISE LOG 'promscale maintenance: compression: metric %: finished in %', r.metric_name, clock_timestamp()-startT;
        END IF;
        PERFORM SCHEMA_CATALOG.unlock_metric_for_maintenance(r.id);

        COMMIT;
    END LOOP;

    FOR r IN
        SELECT *
        FROM unnest(remaining_metrics)
    LOOP
        IF log_verbose THEN
            lockStartT := clock_timestamp();
            RAISE LOG 'promscale maintenance: compression: metric %: waiting for lock', r.metric_name;
        END IF;
        PERFORM SCHEMA_CATALOG.set_app_name( format('promscale maintenance: compression: metric %s: waiting on lock', r.metric_name));
        PERFORM SCHEMA_CATALOG.lock_metric_for_maintenance(r.id);
        IF log_verbose THEN
            startT := clock_timestamp();
            RAISE LOG 'promscale maintenance: compression: metric %: starting', r.metric_name;
        END IF;
        PERFORM SCHEMA_CATALOG.set_app_name( format('promscale maintenance: compression: metric %s', r.metric_name));
        CALL SCHEMA_CATALOG.compress_metric_chunks(r.metric_name);
        IF log_verbose THEN
            RAISE LOG 'promscale maintenance: compression: metric %: finished in % (lock took %; compression took %)', r.metric_name, clock_timestamp()-lockStartT, startT-lockStartT, clock_timestamp()-startT;
        END IF;
        PERFORM SCHEMA_CATALOG.unlock_metric_for_maintenance(r.id);

        COMMIT;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;
COMMENT ON PROCEDURE SCHEMA_CATALOG.execute_compression_policy(boolean)
IS 'compress data according to the policy. This procedure should be run regularly in a cron job';
GRANT EXECUTE ON PROCEDURE SCHEMA_CATALOG.execute_compression_policy(boolean) TO prom_maintenance;

CREATE OR REPLACE PROCEDURE SCHEMA_PROM.add_prom_node(node_name TEXT, attach_to_existing_metrics BOOLEAN = true)
AS $func$
DECLARE
    command_row record;
BEGIN
    FOR command_row IN
        SELECT command, transactional
        FROM SCHEMA_CATALOG.remote_commands
        ORDER BY seq asc
    LOOP
        CALL SCHEMA_TIMESCALE.distributed_exec(command_row.command,node_list=>array[node_name]);
    END LOOP;

    IF attach_to_existing_metrics THEN
        PERFORM attach_data_node(node_name, hypertable => format('%I.%I', 'SCHEMA_DATA', table_name))
        FROM SCHEMA_CATALOG.metric;
    END IF;
END
$func$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.insert_metric_row(
    metric_table name,
    time_array timestamptz[],
    value_array DOUBLE PRECISION[],
    series_id_array bigint[]
) RETURNS BIGINT AS
$$
DECLARE
  num_rows BIGINT;
BEGIN
    EXECUTE FORMAT(
     'INSERT INTO  SCHEMA_DATA.%1$I (time, value, series_id)
          SELECT * FROM unnest($1, $2, $3) a(t,v,s) ORDER BY s,t ON CONFLICT DO NOTHING',
        metric_table
    ) USING time_array, value_array, series_id_array;
    GET DIAGNOSTICS num_rows = ROW_COUNT;
    RETURN num_rows;
END;
$$
LANGUAGE PLPGSQL;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.insert_metric_row(NAME, TIMESTAMPTZ[], DOUBLE PRECISION[], BIGINT[]) TO prom_writer;
