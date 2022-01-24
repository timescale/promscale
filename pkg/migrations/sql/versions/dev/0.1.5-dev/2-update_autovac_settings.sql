DO $doit$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT *
        FROM _prom_catalog.metric
        WHERE default_chunk_interval
    LOOP
        EXECUTE FORMAT($$
            ALTER TABLE prom_data.%I SET (autovacuum_vacuum_threshold = 50000, autovacuum_analyze_threshold = 50000)
        $$, r.table_name);
        EXECUTE FORMAT($$
            ALTER TABLE prom_data_series.%I SET (autovacuum_vacuum_threshold = 100, autovacuum_analyze_threshold = 100)
        $$, r.table_name);
    END LOOP;
END
$doit$;
