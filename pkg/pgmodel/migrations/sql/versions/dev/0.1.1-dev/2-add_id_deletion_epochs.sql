ALTER TABLE SCHEMA_CATALOG.series
    ADD COLUMN delete_epoch BIGINT;

CREATE INDEX series_deleted
    ON SCHEMA_CATALOG.series(delete_epoch, id)
    WHERE delete_epoch IS NOT NULL;


-- epoch for deleting series and label_key ids
CREATE TABLE SCHEMA_CATALOG.ids_epoch(
    current_epoch BIGINT NOT NULL,
    last_update_time TIMESTAMPTZ NOT NULL,
    -- force there to only be a single row
    is_unique BOOLEAN NOT NULL DEFAULT true CHECK (is_unique = true),
    UNIQUE (is_unique)
);

INSERT INTO SCHEMA_CATALOG.ids_epoch VALUES (0, '1970-01-01 00:00:00 UTC', true);

-- recreate this function now to add the WHERE delete_epoch IS NULL
-- the idempotent scripts are run too late for us to get the correct
-- version and update the old views
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
        WHERE delete_epoch IS NULL
    $$, view_name, label_value_cols);
    RETURN true;
END
$func$
LANGUAGE PLPGSQL VOLATILE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.create_series_view(text) TO prom_writer;

-- update old series views to have WHERE delete_epoch IS NOT NULL
DO $$
DECLARE
    metric_names TEXT[];
    metric_name  TEXT;
BEGIN
    SELECT coalesce(array_agg(m.metric_name), array[]::TEXT[]) FROM SCHEMA_CATALOG.metric m
        INTO metric_names;

    FOREACH metric_name IN ARRAY metric_names
    LOOP
        PERFORM SCHEMA_CATALOG.create_series_view(metric_name);
    END LOOP;
END$$;


-- we're replacing with a procedure, so we need an explicit DROP
DROP FUNCTION SCHEMA_CATALOG.drop_metric_chunks(TEXT, TIMESTAMPTZ);
