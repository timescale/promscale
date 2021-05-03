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

CALL SCHEMA_CATALOG.execute_everywhere('create_prom_modifier', $ee$
    DO $$
        BEGIN
            CREATE ROLE prom_modifier;
        EXCEPTION WHEN duplicate_object THEN
            RAISE NOTICE 'role prom_modifier already exists, skipping create';
            RETURN;
        END
    $$;
$ee$);

CALL SCHEMA_CATALOG.execute_everywhere('create_prom_admin', $ee$
    DO $$
        BEGIN
            CREATE ROLE prom_admin;
        EXCEPTION WHEN duplicate_object THEN
            RAISE NOTICE 'role prom_admin already exists, skipping create';
            RETURN;
        END
    $$;
$ee$);

CALL SCHEMA_CATALOG.execute_everywhere('create_prom_maintenance', $ee$
    DO $$
        BEGIN
            CREATE ROLE prom_maintenance;
        EXCEPTION WHEN duplicate_object THEN
            RAISE NOTICE 'role prom_maintenance already exists, skipping create';
            RETURN;
        END
    $$;
$ee$);

CALL SCHEMA_CATALOG.execute_everywhere('grant_prom_reader_prom_writer',$ee$
    DO $$
    BEGIN
        GRANT prom_reader TO prom_writer;
        GRANT prom_reader TO prom_maintenance;
        GRANT prom_writer TO prom_modifier;
        GRANT prom_writer TO prom_admin;
    END
    $$;
$ee$);
