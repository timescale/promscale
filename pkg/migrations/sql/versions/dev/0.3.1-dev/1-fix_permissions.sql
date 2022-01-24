DROP PROCEDURE IF EXISTS execute_everywhere(text, TEXT, BOOLEAN);

CREATE OR REPLACE PROCEDURE _prom_catalog.execute_everywhere(command_key text, command TEXT, transactional BOOLEAN = true)
AS $func$
BEGIN
    IF command_key IS NOT NULL THEN
       INSERT INTO _prom_catalog.remote_commands(key, command, transactional) VALUES(command_key, command, transactional)
       ON CONFLICT (key) DO UPDATE SET command = excluded.command, transactional = excluded.transactional;
    END IF;

    EXECUTE command;
    BEGIN
        CALL distributed_exec(command);
    EXCEPTION
        WHEN undefined_function THEN
            -- we're not on Timescale 2, just return
            RETURN;
        WHEN SQLSTATE '0A000' THEN
            -- we're not the access node, just return
            RETURN;
    END;
END
$func$ LANGUAGE PLPGSQL;
 REVOKE ALL ON PROCEDURE _prom_catalog.execute_everywhere(text, text, boolean) FROM PUBLIC;

CREATE OR REPLACE PROCEDURE _prom_catalog.update_execute_everywhere_entry(command_key text, command TEXT, transactional BOOLEAN = true)
AS $func$
BEGIN
    UPDATE _prom_catalog.remote_commands
    SET
        command=update_execute_everywhere_entry.command,
        transactional=update_execute_everywhere_entry.transactional
    WHERE key = command_key;
END
$func$ LANGUAGE PLPGSQL;
REVOKE ALL ON PROCEDURE _prom_catalog.update_execute_everywhere_entry(text, text, boolean) FROM PUBLIC;


CALL _prom_catalog.execute_everywhere(null::text, command =>
$ee$ DO $$ BEGIN
    REVOKE USAGE ON SCHEMA _prom_catalog FROM prom_writer;
    REVOKE USAGE ON SCHEMA prom_data FROM prom_writer;
    REVOKE USAGE ON SCHEMA prom_data_series FROM prom_writer;

    REVOKE EXECUTE ON FUNCTION _prom_catalog.get_metrics_that_need_compression() FROM prom_reader;

    ALTER DEFAULT PRIVILEGES IN SCHEMA _prom_catalog REVOKE SELECT ON TABLES FROM prom_reader;
    ALTER DEFAULT PRIVILEGES IN SCHEMA _prom_catalog REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM prom_writer;
    ALTER DEFAULT PRIVILEGES IN SCHEMA prom_data REVOKE SELECT ON TABLES FROM prom_reader;
    ALTER DEFAULT PRIVILEGES IN SCHEMA prom_data REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM prom_writer;
    ALTER DEFAULT PRIVILEGES IN SCHEMA prom_data_series REVOKE SELECT ON TABLES FROM prom_reader;
    ALTER DEFAULT PRIVILEGES IN SCHEMA prom_data_series REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM prom_writer;
    ALTER DEFAULT PRIVILEGES IN SCHEMA prom_info REVOKE SELECT ON TABLES FROM prom_reader;
    ALTER DEFAULT PRIVILEGES IN SCHEMA prom_metric REVOKE SELECT ON TABLES FROM prom_reader;
    ALTER DEFAULT PRIVILEGES IN SCHEMA prom_series REVOKE SELECT ON TABLES FROM prom_reader;

    GRANT USAGE ON ALL SEQUENCES IN SCHEMA _prom_catalog TO prom_writer;
    GRANT SELECT ON TABLE public.prom_installation_info TO PUBLIC;
    REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLE _prom_catalog.default FROM prom_writer;
    REVOKE SELECT ON TABLE _prom_catalog.remote_commands FROM prom_reader;
    REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLE _prom_catalog.remote_commands FROM prom_writer;
    REVOKE USAGE ON SEQUENCE _prom_catalog.remote_commands_seq_seq FROM prom_writer;
END $$ $ee$);

CALL _prom_catalog.update_execute_everywhere_entry('create_schemas', $ee$ DO $$ BEGIN
    CREATE SCHEMA IF NOT EXISTS _prom_catalog; -- catalog tables + internal functions
    GRANT USAGE ON SCHEMA _prom_catalog TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_api; -- public functions
    GRANT USAGE ON SCHEMA prom_api TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS _prom_ext; -- optimized versions of functions created by the extension
    GRANT USAGE ON SCHEMA _prom_ext TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_series; -- series views
    GRANT USAGE ON SCHEMA prom_series TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_metric; -- metric views
    GRANT USAGE ON SCHEMA prom_metric TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_data;
    GRANT USAGE ON SCHEMA prom_data TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_data_series;
    GRANT USAGE ON SCHEMA prom_data_series TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_info;
    GRANT USAGE ON SCHEMA prom_info TO prom_reader;
END $$ $ee$)
