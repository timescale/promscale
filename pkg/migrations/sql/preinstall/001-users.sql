CALL SCHEMA_CATALOG.execute_everywhere('create_prom_reader', $ee$
    DO $$
        BEGIN
            CREATE ROLE prom_reader;
        EXCEPTION WHEN duplicate_object THEN
            RAISE NOTICE 'role prom_reader already exists, skipping create';
            RETURN;
        END
    $$;
$ee$);

CALL SCHEMA_CATALOG.execute_everywhere('create_prom_writer', $ee$
    DO $$
        BEGIN
            CREATE ROLE prom_writer;
        EXCEPTION WHEN duplicate_object THEN
            RAISE NOTICE 'role prom_writer already exists, skipping create';
            RETURN;
        END
    $$;
$ee$);

CALL SCHEMA_CATALOG.execute_everywhere('grant_prom_reader_prom_writer',$ee$
    GRANT prom_reader TO prom_writer;
$ee$);
