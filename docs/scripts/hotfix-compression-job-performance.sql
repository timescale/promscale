CREATE OR REPLACE FUNCTION _prom_catalog.compress_chunk_for_metric(metric_table TEXT, chunk_schema_name name, chunk_table_name name) RETURNS VOID
AS $$
DECLARE
    chunk_full_name text;
BEGIN
    SELECT
        format('%I.%I', ch.schema_name, ch.table_name)
    INTO chunk_full_name
    FROM _timescaledb_catalog.chunk ch
        JOIN _timescaledb_catalog.hypertable ht ON ht.id = ch.hypertable_id
    WHERE ht.schema_name = 'prom_data'
      AND ht.table_name = metric_table
      AND ch.schema_name = chunk_schema_name
      AND ch.table_name = chunk_table_name;

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
            ch.schema_name as chunk_schema,
            ch.table_name AS chunk_name,
            _timescaledb_internal.to_timestamp(dimsl.range_end) as range_end,
            row_number() OVER (ORDER BY dimsl.range_end DESC)
        FROM _timescaledb_catalog.chunk ch
            JOIN _timescaledb_catalog.hypertable ht ON ht.id = ch.hypertable_id
            JOIN _timescaledb_catalog.chunk_constraint chcons ON ch.id = chcons.chunk_id
            JOIN _timescaledb_catalog.dimension dim ON ch.hypertable_id = dim.hypertable_id
            JOIN _timescaledb_catalog.dimension_slice dimsl ON dim.id = dimsl.dimension_id AND chcons.dimension_slice_id = dimsl.id
        WHERE ch.dropped IS FALSE
            AND (ch.status & 1) != 1 -- only check for uncompressed chunks
            AND ht.schema_name = 'prom_data'
            AND ht.table_name = metric_table
        ORDER BY 3 ASC
    LOOP
        CONTINUE WHEN chunk_num <= 1 OR chunk_range_end > compress_before;
        PERFORM _prom_catalog.compress_chunk_for_metric(metric_table, chunk_schema_name, chunk_table_name);
        COMMIT;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;
