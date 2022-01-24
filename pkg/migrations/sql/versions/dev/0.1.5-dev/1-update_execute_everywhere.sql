--table to save commands so they can be run when adding new nodes
 CREATE TABLE _prom_catalog.remote_commands(
    key TEXT PRIMARY KEY,
    seq SERIAL,
    transactional BOOLEAN,
    command TEXT
);

DROP PROCEDURE execute_everywhere(text, BOOLEAN);


--stub to allow us to fill in table by copy-paste
CREATE OR REPLACE PROCEDURE execute_everywhere(command_key text, command TEXT, transactional BOOLEAN = true)
AS $func$
BEGIN
        INSERT INTO _prom_catalog.remote_commands(key, command, transactional) VALUES(command_key, command, transactional)
        ON CONFLICT (key) DO UPDATE SET command = excluded.command, transactional = excluded.transactional;
END
$func$ LANGUAGE PLPGSQL;


CALL execute_everywhere('create_prom_reader', $ee$
    DO $$
        BEGIN
            CREATE ROLE prom_reader;
        EXCEPTION WHEN duplicate_object THEN
            RAISE NOTICE 'role prom_reader already exists, skipping create';
            RETURN;
        END
    $$;
$ee$);

CALL execute_everywhere('create_prom_writer', $ee$
    DO $$
        BEGIN
            CREATE ROLE prom_writer;
        EXCEPTION WHEN duplicate_object THEN
            RAISE NOTICE 'role prom_writer already exists, skipping create';
            RETURN;
        END
    $$;
$ee$);

CALL execute_everywhere('grant_prom_reader_prom_writer',$ee$
    GRANT prom_reader TO prom_writer;
$ee$);

CALL execute_everywhere('create_schemas', $ee$ DO $$ BEGIN
    CREATE SCHEMA IF NOT EXISTS _prom_catalog; -- catalog tables + internal functions
    GRANT USAGE ON SCHEMA _prom_catalog TO prom_reader;
    GRANT SELECT ON ALL TABLES IN SCHEMA _prom_catalog TO prom_reader;
    ALTER DEFAULT PRIVILEGES IN SCHEMA _prom_catalog GRANT SELECT ON TABLES TO prom_reader;
    GRANT USAGE ON SCHEMA _prom_catalog TO prom_writer;
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA _prom_catalog TO prom_writer;
    ALTER DEFAULT PRIVILEGES IN SCHEMA _prom_catalog GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO prom_writer;

    CREATE SCHEMA IF NOT EXISTS prom_api; -- public functions
    GRANT USAGE ON SCHEMA prom_api TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS _prom_ext; -- optimized versions of functions created by the extension
    GRANT USAGE ON SCHEMA _prom_ext TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_series; -- series views
    GRANT USAGE ON SCHEMA prom_series TO prom_reader;
    GRANT SELECT ON ALL TABLES IN SCHEMA prom_series TO prom_reader;
    ALTER DEFAULT PRIVILEGES IN SCHEMA prom_series GRANT SELECT ON TABLES TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_metric; -- metric views
    GRANT USAGE ON SCHEMA prom_metric TO prom_reader;
    GRANT SELECT ON ALL TABLES IN SCHEMA prom_metric TO prom_reader;
    ALTER DEFAULT PRIVILEGES IN SCHEMA prom_metric GRANT SELECT ON TABLES TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_data;
    GRANT USAGE ON SCHEMA prom_data TO prom_reader;
    GRANT SELECT ON ALL TABLES IN SCHEMA prom_data TO prom_reader;
    ALTER DEFAULT PRIVILEGES IN SCHEMA prom_data GRANT SELECT ON TABLES TO prom_reader;
    GRANT USAGE ON SCHEMA prom_data TO prom_writer;
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA prom_data TO prom_writer;
    ALTER DEFAULT PRIVILEGES IN SCHEMA prom_data GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO prom_writer;

    CREATE SCHEMA IF NOT EXISTS prom_data_series;
    GRANT USAGE ON SCHEMA prom_data_series TO prom_reader;
    GRANT SELECT ON ALL TABLES IN SCHEMA prom_data_series TO prom_reader;
    ALTER DEFAULT PRIVILEGES IN SCHEMA prom_data_series GRANT SELECT ON TABLES TO prom_reader;
    GRANT USAGE ON SCHEMA prom_data_series TO prom_writer;
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA prom_data_series TO prom_writer;
    ALTER DEFAULT PRIVILEGES IN SCHEMA prom_data_series GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO prom_writer;


    CREATE SCHEMA IF NOT EXISTS prom_info;
    GRANT USAGE ON SCHEMA prom_info TO prom_reader;
    GRANT SELECT ON ALL TABLES IN SCHEMA prom_info TO prom_reader;
    ALTER DEFAULT PRIVILEGES IN SCHEMA prom_info GRANT SELECT ON TABLES TO prom_reader;
END $$ $ee$);

--real function
CREATE OR REPLACE PROCEDURE execute_everywhere(command_key text, command TEXT, transactional BOOLEAN = true)
AS $func$
BEGIN
    EXECUTE command;

    INSERT INTO _prom_catalog.remote_commands(key, command, transactional) VALUES(command_key, command, transactional)
    ON CONFLICT (key) DO UPDATE SET command = excluded.command, transactional = excluded.transactional;

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
