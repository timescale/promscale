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
        GRANT prom_modifier TO prom_admin;
        GRANT prom_maintenance TO prom_admin;
    END
    $$;
$ee$);

--adjust all of the sequence numbers
UPDATE SCHEMA_CATALOG.remote_commands SET seq = 7 WHERE key = 'create_schemas';
UPDATE SCHEMA_CATALOG.remote_commands SET seq = 4 WHERE key = 'create_prom_admin';
UPDATE SCHEMA_CATALOG.remote_commands SET seq = 5 WHERE key = 'create_prom_maintenance';
UPDATE SCHEMA_CATALOG.remote_commands SET seq = 3 WHERE key = 'create_prom_modifier';
UPDATE SCHEMA_CATALOG.remote_commands SET seq = 6 WHERE key = 'grant_prom_reader_prom_writer';

--make sure to create entries for those that may not exist before adjusting seq #s
INSERT INTO SCHEMA_CATALOG.remote_commands(key, command, transactional) VALUES('_prom_catalog.do_decompress_chunks_after', '', true) ON CONFLICT DO NOTHING;
UPDATE SCHEMA_CATALOG.remote_commands SET seq = 8 WHERE key = '_prom_catalog.do_decompress_chunks_after';
INSERT INTO SCHEMA_CATALOG.remote_commands(key, command, transactional) VALUES('_prom_catalog.compress_old_chunks', '', true) ON CONFLICT DO NOTHING;
UPDATE SCHEMA_CATALOG.remote_commands SET seq = 9 WHERE key = '_prom_catalog.compress_old_chunks';

GRANT  SELECT, INSERT, UPDATE, DELETE ON SCHEMA_CATALOG.default TO prom_admin;
REVOKE EXECUTE ON FUNCTION SCHEMA_CATALOG.lock_metric_for_maintenance(int, boolean) FROM prom_writer;

CALL SCHEMA_CATALOG.execute_everywhere(null::text, command =>
$ee$ DO $$ BEGIN
    REVOKE UPDATE, DELETE ON ALL TABLES IN SCHEMA SCHEMA_DATA FROM prom_writer;
    REVOKE UPDATE, DELETE ON ALL TABLES IN SCHEMA SCHEMA_DATA_SERIES FROM prom_writer;
    GRANT  SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SCHEMA_DATA TO prom_modifier;
    GRANT  SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SCHEMA_DATA_SERIES TO prom_modifier;
END $$ $ee$);