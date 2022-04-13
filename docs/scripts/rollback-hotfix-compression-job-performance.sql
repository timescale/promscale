CREATE OR REPLACE FUNCTION _prom_catalog.compress_chunk_for_metric(metric_table TEXT, chunk_schema_name name, chunk_table_name name) RETURNS VOID
AS $$
DECLARE
    chunk_full_name text;
BEGIN
    SELECT
        format('%I.%I', chunk_schema, chunk_name)
    INTO chunk_full_name
    FROM timescaledb_information.chunks
    WHERE hypertable_schema = 'prom_data'
      AND hypertable_name = metric_table
      AND chunk_schema = chunk_schema_name
      AND chunk_name = chunk_table_name;

    PERFORM public.compress_chunk(chunk_full_name, if_not_compressed => true);
END;
$$
LANGUAGE PLPGSQL
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;

CREATE OR REPLACE PROCEDURE _prom_catalog.compress_old_chunks(metric_table TEXT, compress_before TIMESTAMPTZ)
AS $$
DECLARE
    chunk_schema_name name;
    chunk_table_name name;
    chunk_range_end timestamptz;
    chunk_num INT;
BEGIN
    FOR chunk_schema_name, chunk_table_name, chunk_range_end, chunk_num IN
        SELECT
            chunk_schema,
            chunk_name,
            range_end,
            row_number() OVER (ORDER BY range_end DESC)
        FROM timescaledb_information.chunks
        WHERE hypertable_schema = 'prom_data'
            AND hypertable_name = metric_table
            AND NOT is_compressed
        ORDER BY range_end ASC
    LOOP
        CONTINUE WHEN chunk_num <= 1 OR chunk_range_end > compress_before;
        PERFORM _prom_catalog.compress_chunk_for_metric(metric_table, chunk_schema_name, chunk_table_name);
        COMMIT;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;
