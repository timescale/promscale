CALL _prom_catalog.execute_everywhere('create_prom_modifier', $ee$
    DO $$
        BEGIN
            CREATE ROLE prom_modifier;
        EXCEPTION WHEN duplicate_object THEN
            RAISE NOTICE 'role prom_modifier already exists, skipping create';
            RETURN;
        END
    $$;
$ee$);

CALL _prom_catalog.execute_everywhere('create_prom_admin', $ee$
    DO $$
        BEGIN
            CREATE ROLE prom_admin;
        EXCEPTION WHEN duplicate_object THEN
            RAISE NOTICE 'role prom_admin already exists, skipping create';
            RETURN;
        END
    $$;
$ee$);

CALL _prom_catalog.execute_everywhere('create_prom_maintenance', $ee$
    DO $$
        BEGIN
            CREATE ROLE prom_maintenance;
        EXCEPTION WHEN duplicate_object THEN
            RAISE NOTICE 'role prom_maintenance already exists, skipping create';
            RETURN;
        END
    $$;
$ee$);

CALL _prom_catalog.execute_everywhere('grant_prom_reader_prom_writer',$ee$
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
UPDATE _prom_catalog.remote_commands SET seq = 7 WHERE key = 'create_schemas';
UPDATE _prom_catalog.remote_commands SET seq = 4 WHERE key = 'create_prom_admin';
UPDATE _prom_catalog.remote_commands SET seq = 5 WHERE key = 'create_prom_maintenance';
UPDATE _prom_catalog.remote_commands SET seq = 3 WHERE key = 'create_prom_modifier';
UPDATE _prom_catalog.remote_commands SET seq = 6 WHERE key = 'grant_prom_reader_prom_writer';

--make sure to create entries for those that may not exist before adjusting seq #s
INSERT INTO _prom_catalog.remote_commands(key, command, transactional) VALUES('_prom_catalog.do_decompress_chunks_after', '', true) ON CONFLICT DO NOTHING;
UPDATE _prom_catalog.remote_commands SET seq = 8 WHERE key = '_prom_catalog.do_decompress_chunks_after';
INSERT INTO _prom_catalog.remote_commands(key, command, transactional) VALUES('_prom_catalog.compress_old_chunks', '', true) ON CONFLICT DO NOTHING;
UPDATE _prom_catalog.remote_commands SET seq = 9 WHERE key = '_prom_catalog.compress_old_chunks';

GRANT  SELECT, INSERT, UPDATE, DELETE ON _prom_catalog.default TO prom_admin;
REVOKE EXECUTE ON FUNCTION _prom_catalog.lock_metric_for_maintenance(int, boolean) FROM prom_writer;

CALL _prom_catalog.execute_everywhere(null::text, command =>
$ee$ DO $$ BEGIN
    REVOKE UPDATE, DELETE ON ALL TABLES IN SCHEMA prom_data FROM prom_writer;
    REVOKE UPDATE, DELETE ON ALL TABLES IN SCHEMA prom_data_series FROM prom_writer;
    GRANT  SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA prom_data TO prom_modifier;
    GRANT  SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA prom_data_series TO prom_modifier;
END $$ $ee$);