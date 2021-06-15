-- get_exemplar_label_positions returns the position of label_keys as a one-to-one mapping with label_keys. It returns
-- the positions of all label keys corresponding to that metric, so that it remains easier to add null values to those indexes
-- whose labels are not present in the exemplar being inserted at the golang level.
CREATE OR REPLACE FUNCTION _prom_catalog.get_exemplar_label_key_positions(metric_name_text TEXT, label_keys TEXT[])
RETURNS TABLE (metric_family_text TEXT, label_positions_map JSON) AS
$$
DECLARE
    existing_keys TEXT[];
    existing_key_positions INTEGER[];

    num_existing_keys INTEGER;
    num_label_keys INTEGER;

    current_position INTEGER := 1; -- index in postgres starts from 1. Let's maintain the convention for less confusion.

    new_position INTEGER;
    found BOOLEAN := false;
    k TEXT;
BEGIN
    SELECT array_agg(key), array_agg(pos) INTO existing_keys, existing_key_positions FROM _prom_catalog.exemplar_label_key_position WHERE metric_name=metric_name_text;

    num_existing_keys := array_length(existing_keys, 1);
    num_label_keys := array_length(label_keys, 1);

    LOCK TABLE _prom_catalog.exemplar_label_key_position IN ACCESS EXCLUSIVE MODE;
    -- If there isn't any data for the given metric_name_text.
    IF num_existing_keys IS NULL THEN
        FOREACH k in ARRAY label_keys LOOP
            INSERT INTO _prom_catalog.exemplar_label_key_position VALUES (metric_name_text, k, current_position);
            current_position := current_position + 1;
        END LOOP;
        RETURN QUERY (
            SELECT row.metric_name, json_object_agg(row.key, row.position) FROM (
                SELECT metric_name, key, pos as position FROM _prom_catalog.exemplar_label_key_position
                    WHERE metric_name=metric_name_text GROUP BY metric_name, key, pos ORDER BY pos
            ) AS row GROUP BY row.metric_name
        );
    END IF;

    -- Positions already exists for some keys for the given metric.
    -- Let's create new positions for new keys only.
    FOR i in 1..num_label_keys LOOP
        found := false;
        FOR j in 1..num_existing_keys LOOP
            -- todo (harkishen): optimize below using a plain sql query
            IF label_keys[i] = existing_keys[j] THEN
                -- key found.
                found := true;
                EXIT;
            END IF;
        END LOOP;
        IF NOT found THEN
            -- key not found.
            -- todo: optimize the below query using the local var 'existing_key_postions'
            SELECT max(pos) + 1 INTO new_position FROM _prom_catalog.exemplar_label_key_position WHERE metric_name=metric_name_text;
            INSERT INTO _prom_catalog.exemplar_label_key_position VALUES (metric_name_text, label_keys[i], new_position);
        END IF;
    END LOOP;
    RETURN QUERY (
        SELECT row.metric_name, json_object_agg(row.key, row.position) FROM (
            SELECT metric_name, key, pos as position FROM _prom_catalog.exemplar_label_key_position
                WHERE metric_name=metric_name_text GROUP BY metric_name, key, pos ORDER BY pos
        ) AS row GROUP BY row.metric_name
    );
END;
$$
LANGUAGE PLPGSQL;
-- todo: set security_definer

-- get label position for the given key in the metric.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_exemplar_label_positions(metric_name_text TEXT, label_key TEXT])
RETURNS INTEGER AS
$$
    SELECT pos FROM SCHEMA_CATALOG.exemplar_label_key_position WHERE metric_name=metric_name_text AND key=label_key;
$$
LANGUAGE SQL;

-- get all label positions for the given metric.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_exemplar_label_positions(metric_name_text TEXT, label_key TEXT])
RETURNS INTEGER AS
$$
    SELECT key, pos FROM SCHEMA_CATALOG.exemplar_label_key_position WHERE metric_name=metric_name_text ORDER BY pos;
$$
LANGUAGE SQL;

-- creates exemplar table in prom_data_exemplar schema if the table does not exists. This function
-- must be called after the metric is created in _prom_catalog.metric sa it utilies the table_name
-- from that table. It returns true if the table was created.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.create_exemplar_table_if_not_exists(metric_name_text TEXT)
RETURNS BOOLEAN
AS
$$
DECLARE
    table_name_text TEXT;
BEGIN
    SELECT table_name INTO table_name_text FROM SCHEMA_CATALOG.metric WHERE metric_name=metric_name_text;
    IF table_name_text IS NULL THEN
        -- metric table entry does not exists in SCHEMA_CATALOG.metric, hence we cannot create. Error out.
        -- Note: even though we can create an entry from here, we should not as it keeps the approach systematic.
        RAISE EXCEPTION 'SCHEMA_CATALOG.metric does not contain the table entry for % metric', metric_name_text;
    END IF;
    -- check if table is already created.
    IF (
        SELECT count(table_name) > 0 FROM SCHEMA_CATALOG.exemplar WHERE metric_id=(
            SELECT id FROM SCHEMA_CATALOG.metric WHERE metric_name=metric_name_text
        )
    ) THEN
        RETURN FALSE;
    END IF;
    -- table does not exists. Let's create it.
    EXECUTE FORMAT('CREATE TABLE SCHEMA_DATA_EXEMPLAR.%I (time TIMESTAMPTZ NOT NULL, series_id BIGINT NOT NULL, exemplar_labels TEXT[], value DOUBLE PRECISION NOT NULL) WITH (autovacuum_vacuum_threshold = 50000, autovacuum_analyze_threshold = 50000)',
        table_name_text);
    EXECUTE format('GRANT SELECT ON TABLE SCHEMA_DATA_EXEMPLAR.%I TO prom_reader', table_name_text);
    EXECUTE format('GRANT SELECT, INSERT ON TABLE SCHEMA_DATA_EXEMPLAR.%I TO prom_writer', table_name_text);
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_DATA_EXEMPLAR.%I TO prom_modifier', table_name_text);
    EXECUTE format('CREATE UNIQUE INDEX exemplar_index%s ON SCHEMA_DATA_EXEMPLAR.%I (series_id, time) INCLUDE (value)',
                        table_name_text, table_name_text);
    INSERT INTO SCHEMA_CATALOG.exemplar (metric_id, table_name) VALUES (
        (
            SELECT id FROM SCHEMA_CATALOG.metric WHERE metric_name=metric_name_text
        ),
        table_name_text
    );
    RETURN TRUE;
END;
$$
LANGUAGE PLPGSQL;
