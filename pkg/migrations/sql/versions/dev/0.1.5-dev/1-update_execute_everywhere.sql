--table to save commands so they can be run when adding new nodes
 CREATE TABLE SCHEMA_CATALOG.remote_commands(
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
        INSERT INTO SCHEMA_CATALOG.remote_commands(key, command, transactional) VALUES(command_key, command, transactional)
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
    CREATE SCHEMA IF NOT EXISTS SCHEMA_CATALOG; -- catalog tables + internal functions
    GRANT USAGE ON SCHEMA SCHEMA_CATALOG TO prom_reader;
    GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_CATALOG TO prom_reader;
    ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_CATALOG GRANT SELECT ON TABLES TO prom_reader;
    GRANT USAGE ON SCHEMA SCHEMA_CATALOG TO prom_writer;
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SCHEMA_CATALOG TO prom_writer;
    ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_CATALOG GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO prom_writer;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_PROM; -- public functions
    GRANT USAGE ON SCHEMA SCHEMA_PROM TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_EXT; -- optimized versions of functions created by the extension
    GRANT USAGE ON SCHEMA SCHEMA_EXT TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_SERIES; -- series views
    GRANT USAGE ON SCHEMA SCHEMA_SERIES TO prom_reader;
    GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_SERIES TO prom_reader;
    ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_SERIES GRANT SELECT ON TABLES TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_METRIC; -- metric views
    GRANT USAGE ON SCHEMA SCHEMA_METRIC TO prom_reader;
    GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_METRIC TO prom_reader;
    ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_METRIC GRANT SELECT ON TABLES TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_DATA;
    GRANT USAGE ON SCHEMA SCHEMA_DATA TO prom_reader;
    GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_DATA TO prom_reader;
    ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA GRANT SELECT ON TABLES TO prom_reader;
    GRANT USAGE ON SCHEMA SCHEMA_DATA TO prom_writer;
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SCHEMA_DATA TO prom_writer;
    ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO prom_writer;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_DATA_SERIES;
    GRANT USAGE ON SCHEMA SCHEMA_DATA_SERIES TO prom_reader;
    GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_DATA_SERIES TO prom_reader;
    ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA_SERIES GRANT SELECT ON TABLES TO prom_reader;
    GRANT USAGE ON SCHEMA SCHEMA_DATA_SERIES TO prom_writer;
    GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SCHEMA_DATA_SERIES TO prom_writer;
    ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_DATA_SERIES GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO prom_writer;


    CREATE SCHEMA IF NOT EXISTS SCHEMA_INFO;
    GRANT USAGE ON SCHEMA SCHEMA_INFO TO prom_reader;
    GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_INFO TO prom_reader;
    ALTER DEFAULT PRIVILEGES IN SCHEMA SCHEMA_INFO GRANT SELECT ON TABLES TO prom_reader;
END $$ $ee$);

--real function
CREATE OR REPLACE PROCEDURE execute_everywhere(command_key text, command TEXT, transactional BOOLEAN = true)
AS $func$
BEGIN
    EXECUTE command;

    INSERT INTO SCHEMA_CATALOG.remote_commands(key, command, transactional) VALUES(command_key, command, transactional)
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
