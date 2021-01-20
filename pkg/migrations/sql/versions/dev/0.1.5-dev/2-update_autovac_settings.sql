DO $doit$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT *
        FROM SCHEMA_CATALOG.metric
        WHERE default_chunk_interval
    LOOP
        EXECUTE FORMAT($$
            ALTER TABLE SCHEMA_DATA.%I SET (autovacuum_vacuum_threshold = 50000, autovacuum_analyze_threshold = 50000)
        $$, r.table_name);
        EXECUTE FORMAT($$
            ALTER TABLE SCHEMA_DATA_SERIES.%I SET (autovacuum_vacuum_threshold = 100, autovacuum_analyze_threshold = 100)
        $$, r.table_name);
    END LOOP;
END
$doit$;
