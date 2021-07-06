GRANT USAGE, SELECT ON SEQUENCE exemplar_id_seq TO prom_writer;

-- get_exemplar_label_positions returns the position of label_keys as a one-to-one mapping with label_keys. It returns
-- the positions of all label keys corresponding to that metric, so that it remains easier to add null values to those indexes
-- whose labels are not present in the exemplar being inserted at the golang level.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_exemplar_label_key_positions(metric_name_text TEXT, label_keys TEXT[])
RETURNS TABLE (metric_family_text TEXT, label_positions_map JSON) AS
$$
DECLARE
    current_position INTEGER := 1; -- index in postgres starts from 1. Let's maintain the convention for less confusion.
    new_position INTEGER;
    k TEXT;
BEGIN
    LOCK TABLE SCHEMA_CATALOG.exemplar_label_key_position IN ACCESS EXCLUSIVE MODE;
    -- If there isn't any data for the given metric_name_text.
    IF ( SELECT count(key) = 0 FROM SCHEMA_CATALOG.exemplar_label_key_position WHERE metric_name=metric_name_text ) THEN
        FOREACH k in ARRAY label_keys LOOP
            INSERT INTO SCHEMA_CATALOG.exemplar_label_key_position VALUES (metric_name_text, k, current_position);
            current_position := current_position + 1;
        END LOOP;
        RETURN QUERY (
            SELECT row.metric_name, json_object_agg(row.key, row.position) FROM (
                SELECT metric_name, key, pos as position FROM SCHEMA_CATALOG.exemplar_label_key_position
                    WHERE metric_name=metric_name_text GROUP BY metric_name, key, pos ORDER BY pos
            ) AS row GROUP BY row.metric_name LIMIT 1
        );
    RETURN;
    END IF;

    -- Position already exists for some keys. Let's add for the new keys only.
    FOREACH k in ARRAY label_keys LOOP
        IF (SELECT count(key) = 0 FROM SCHEMA_CATALOG.exemplar_label_key_position WHERE metric_name=metric_name_text AND key=k) THEN
            SELECT max(pos) + 1 INTO new_position FROM SCHEMA_CATALOG.exemplar_label_key_position WHERE metric_name=metric_name_text;
            RAISE NOTICE 'inserting key % at pos %', k, new_position;
            INSERT INTO SCHEMA_CATALOG.exemplar_label_key_position VALUES (metric_name_text, k, new_position);
            new_position := -1;
        END IF;
    END LOOP;
    RETURN QUERY (
    SELECT row.metric_name, json_object_agg(row.key, row.position) FROM (
        SELECT metric_name, key, pos as position FROM SCHEMA_CATALOG.exemplar_label_key_position
            WHERE metric_name=metric_name_text GROUP BY metric_name, key, pos ORDER BY pos
        ) AS row GROUP BY row.metric_name
    );
END;
$$
LANGUAGE PLPGSQL;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_exemplar_label_key_positions(TEXT, TEXT[]) TO prom_writer;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_exemplar_label_key_positions(TEXT, TEXT[]) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_exemplar_label_key_positions(metric_name_text TEXT)
RETURNS JSON AS
$$
    SELECT json_object_agg(row.key, row.position) FROM (
        SELECT key, pos as position FROM SCHEMA_CATALOG.exemplar_label_key_position
            WHERE metric_name=metric_name_text GROUP BY metric_name, key, pos ORDER BY pos
        ) AS row
$$
LANGUAGE SQL;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_exemplar_label_key_positions(TEXT) TO prom_writer;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.get_exemplar_label_key_positions(TEXT) TO prom_reader;

-- creates exemplar table in prom_data_exemplar schema if the table does not exists. This function
-- must be called after the metric is created in _prom_catalog.metric as it utilizes the table_name
-- from the metric table. It returns true if the table was created.
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.create_exemplar_table_if_not_exists(metric_name_text TEXT)
RETURNS BOOLEAN
AS
$$
DECLARE
    table_name_fetched TEXT;
    metric_name_fetched TEXT;
BEGIN
    SELECT metric_name, table_name INTO metric_name_fetched, table_name_fetched FROM SCHEMA_CATALOG.metric WHERE metric_name=metric_name_text;
    IF table_name_fetched IS NULL THEN
        -- metric table entry does not exists in SCHEMA_CATALOG.metric, hence we cannot create. Error out.
        -- Note: even though we can create an entry from here, we should not as it keeps the approach systematic.
        RAISE EXCEPTION 'SCHEMA_CATALOG.metric does not contain the table entry for % metric', metric_name_text;
    END IF;
    -- check if table is already created.
    IF (
        SELECT count(table_name) > 0 FROM SCHEMA_CATALOG.exemplar WHERE metric_name=metric_name_text
    ) THEN
        RETURN FALSE;
    END IF;
    -- table does not exists. Let's create it.
    EXECUTE FORMAT('CREATE TABLE SCHEMA_DATA_EXEMPLAR.%I (time TIMESTAMPTZ NOT NULL, series_id BIGINT NOT NULL, exemplar_label_values SCHEMA_PROM.label_value_array, value DOUBLE PRECISION NOT NULL) WITH (autovacuum_vacuum_threshold = 50000, autovacuum_analyze_threshold = 50000)',
        table_name_fetched);
    EXECUTE format('GRANT SELECT ON TABLE SCHEMA_DATA_EXEMPLAR.%I TO prom_reader', table_name_fetched);
    EXECUTE format('GRANT SELECT, INSERT ON TABLE SCHEMA_DATA_EXEMPLAR.%I TO prom_writer', table_name_fetched);
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_DATA_EXEMPLAR.%I TO prom_modifier', table_name_fetched);
    EXECUTE format('CREATE UNIQUE INDEX exemplar_index%s ON SCHEMA_DATA_EXEMPLAR.%I (series_id, time) INCLUDE (value)',
                        table_name_fetched, table_name_fetched);
    INSERT INTO SCHEMA_CATALOG.exemplar (metric_name, table_name)
        VALUES (metric_name_fetched, table_name_fetched);
    RETURN TRUE;
END;
$$
LANGUAGE PLPGSQL;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.create_exemplar_table_if_not_exists(TEXT) TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.insert_exemplar_row(
    metric_table NAME,
    time_array TIMESTAMPTZ[],
    series_id_array BIGINT[],
    exemplar_label_values_array SCHEMA_PROM.label_value_array[],
    value_array DOUBLE PRECISION[]
) RETURNS BIGINT AS
$$
DECLARE
    num_rows BIGINT;
BEGIN
    EXECUTE FORMAT(
        'INSERT INTO SCHEMA_DATA_EXEMPLAR.%1$I (time, series_id, exemplar_label_values, value)
             SELECT * FROM unnest($1, $2::BIGINT[], $3::SCHEMA_PROM.label_value_array[], $4::DOUBLE PRECISION[]) a(t,s,lv,v) ORDER BY s,t ON CONFLICT DO NOTHING',
        metric_table
    ) USING time_array, series_id_array, exemplar_label_values_array, value_array;
    GET DIAGNOSTICS num_rows = ROW_COUNT;
    RETURN num_rows;
END;
$$
LANGUAGE PLPGSQL;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.insert_exemplar_row(NAME, TIMESTAMPTZ[], BIGINT[], SCHEMA_PROM.label_value_array[], DOUBLE PRECISION[]) TO prom_writer;
