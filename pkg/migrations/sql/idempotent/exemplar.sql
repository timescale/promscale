CREATE OR REPLACE FUNCTION _prom_catalog.get_exemplar_label_key_positions(metric_name TEXT)
RETURNS JSON AS
$$
    SELECT json_object_agg(row.key, row.position)
    FROM (
        SELECT p.key as key, p.pos as position
        FROM _prom_catalog.exemplar_label_key_position p
        WHERE p.metric_name=get_exemplar_label_key_positions.metric_name
        GROUP BY p.metric_name, p.key, p.pos
        ORDER BY p.pos
    ) AS row
$$
LANGUAGE SQL
STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _prom_catalog.get_exemplar_label_key_positions(TEXT) TO prom_reader;

-- creates exemplar table in prom_data_exemplar schema if the table does not exists. This function
-- must be called after the metric is created in _prom_catalog.metric as it utilizes the table_name
-- from the metric table. It returns true if the table was created.
CREATE OR REPLACE FUNCTION _prom_catalog.create_exemplar_table_if_not_exists(metric_name TEXT)
RETURNS BOOLEAN
AS
$$
DECLARE
    table_name_fetched TEXT;
    metric_name_fetched TEXT;
BEGIN
    SELECT m.metric_name, m.table_name
    INTO metric_name_fetched, table_name_fetched
    FROM _prom_catalog.metric m
    WHERE m.metric_name=create_exemplar_table_if_not_exists.metric_name AND table_schema = 'prom_data';

    IF NOT FOUND THEN
        -- metric table entry does not exists in _prom_catalog.metric, hence we cannot create. Error out.
        -- Note: even though we can create an entry from here, we should not as it keeps the approach systematic.
        RAISE EXCEPTION '_prom_catalog.metric does not contain the table entry for % metric', metric_name;
    END IF;
    -- check if table is already created.
    IF (
        SELECT count(e.table_name) > 0 FROM _prom_catalog.exemplar e WHERE e.metric_name=create_exemplar_table_if_not_exists.metric_name
    ) THEN
        RETURN FALSE;
    END IF;
    -- table does not exists. Let's create it.
    EXECUTE FORMAT('CREATE TABLE prom_data_exemplar.%I (time TIMESTAMPTZ NOT NULL, series_id BIGINT NOT NULL, exemplar_label_values prom_api.label_value_array, value DOUBLE PRECISION NOT NULL) WITH (autovacuum_vacuum_threshold = 50000, autovacuum_analyze_threshold = 50000)',
        table_name_fetched);
    EXECUTE format('GRANT SELECT ON TABLE prom_data_exemplar.%I TO prom_reader', table_name_fetched);
    EXECUTE format('GRANT SELECT, INSERT ON TABLE prom_data_exemplar.%I TO prom_writer', table_name_fetched);
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE prom_data_exemplar.%I TO prom_modifier', table_name_fetched);
    EXECUTE format('CREATE UNIQUE INDEX ei_%s ON prom_data_exemplar.%I (series_id, time) INCLUDE (value)',
                   table_name_fetched, table_name_fetched);
    INSERT INTO _prom_catalog.exemplar (metric_name, table_name)
        VALUES (metric_name_fetched, table_name_fetched);
    RETURN TRUE;
END;
$$
LANGUAGE PLPGSQL;
GRANT EXECUTE ON FUNCTION _prom_catalog.create_exemplar_table_if_not_exists(TEXT) TO prom_writer;

CREATE OR REPLACE FUNCTION _prom_catalog.insert_exemplar_row(
    metric_table NAME,
    time_array TIMESTAMPTZ[],
    series_id_array BIGINT[],
    exemplar_label_values_array prom_api.label_value_array[],
    value_array DOUBLE PRECISION[]
) RETURNS BIGINT AS
$$
DECLARE
    num_rows BIGINT;
BEGIN
    EXECUTE FORMAT(
        'INSERT INTO prom_data_exemplar.%1$I (time, series_id, exemplar_label_values, value)
             SELECT * FROM unnest($1, $2::BIGINT[], $3::prom_api.label_value_array[], $4::DOUBLE PRECISION[]) a(t,s,lv,v) ORDER BY s,t ON CONFLICT DO NOTHING',
        metric_table
    ) USING time_array, series_id_array, exemplar_label_values_array, value_array;
    GET DIAGNOSTICS num_rows = ROW_COUNT;
    RETURN num_rows;
END;
$$
LANGUAGE PLPGSQL;
GRANT EXECUTE ON FUNCTION _prom_catalog.insert_exemplar_row(NAME, TIMESTAMPTZ[], BIGINT[], prom_api.label_value_array[], DOUBLE PRECISION[]) TO prom_writer;
