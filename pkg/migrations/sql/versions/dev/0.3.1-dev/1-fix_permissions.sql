CREATE OR REPLACE PROCEDURE execute_everywhere(command_key text, command TEXT, transactional BOOLEAN = true)
AS $func$
BEGIN
    IF command_key IS NOT NULL THEN
       INSERT INTO SCHEMA_CATALOG.remote_commands(key, command, transactional) VALUES(command_key, command, transactional)
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

CREATE OR REPLACE PROCEDURE update_execute_everywhere_entry(command_key text, command TEXT, transactional BOOLEAN = true)
AS $func$
BEGIN
    UPDATE SCHEMA_CATALOG.remote_commands
    SET
        command=update_execute_everywhere_entry.command,
        transactional=update_execute_everywhere_entry.transactional
    WHERE key = command_key;
END
$func$ LANGUAGE PLPGSQL;


CALL execute_everywhere(null::text, command=>$ee$
REVOKE USAGE ON SCHEMA SCHEMA_CATALOG FROM prom_writer;
REVOKE USAGE ON SCHEMA SCHEMA_DATA FROM prom_writer;
REVOKE USAGE ON SCHEMA SCHEMA_DATA_SERIES FROM prom_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_CATALOG REVOKE SELECT ON TABLES FROM prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_CATALOG REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM prom_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA REVOKE SELECT ON TABLES FROM prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM prom_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA_SERIES REVOKE SELECT ON TABLES FROM prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA_SERIES REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM prom_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_INFO REVOKE SELECT ON TABLES FROM prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_METRIC REVOKE SELECT ON TABLES FROM prom_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_SERIES REVOKE SELECT ON TABLES FROM prom_reader;

GRANT USAGE ON ALL SEQUENCES IN SCHEMA SCHEMA_CATALOG TO prom_writer;
$ee$);

CALL update_execute_everywhere_entry('create_schemas', $ee$ DO $$ BEGIN
    CREATE SCHEMA IF NOT EXISTS SCHEMA_CATALOG; -- catalog tables + internal functions
    GRANT USAGE ON SCHEMA SCHEMA_CATALOG TO prom_reader;
    --remove default exec privileges by default
    REVOKE ALL ON ALL FUNCTIONS IN SCHEMA SCHEMA_CATALOG FROM PUBLIC;
    ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_CATALOG REVOKE ALL ON FUNCTIONS FROM PUBLIC;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_PROM; -- public functions
    GRANT USAGE ON SCHEMA SCHEMA_PROM TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_EXT; -- optimized versions of functions created by the extension
    GRANT USAGE ON SCHEMA SCHEMA_EXT TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_SERIES; -- series views
    GRANT USAGE ON SCHEMA SCHEMA_SERIES TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_METRIC; -- metric views
    GRANT USAGE ON SCHEMA SCHEMA_METRIC TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_DATA;
    GRANT USAGE ON SCHEMA SCHEMA_DATA TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_DATA_SERIES;
    GRANT USAGE ON SCHEMA SCHEMA_DATA_SERIES TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_INFO;
    GRANT USAGE ON SCHEMA SCHEMA_INFO TO prom_reader;
END $$ $ee$)
