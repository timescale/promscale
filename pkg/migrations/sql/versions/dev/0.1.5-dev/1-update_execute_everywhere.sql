--table to save commands so they can be run when adding new nodes
 CREATE TABLE SCHEMA_CATALOG.remote_commands(
    key TEXT PRIMARY KEY,
    seq SERIAL,
    transactional BOOLEAN,
    command TEXT
);

DROP PROCEDURE execute_everywhere(text, BOOLEAN);

CREATE OR REPLACE PROCEDURE execute_everywhere(command_key text, command TEXT, transactional BOOLEAN = true)
AS $func$
BEGIN
    EXECUTE command;

    BEGIN
        INSERT INTO SCHEMA_CATALOG.remote_commands(key, command, transactional) VALUES(command_key, command, transactional)
        ON CONFLICT (key) DO UPDATE SET command = excluded.command, transactional = excluded.transactional;

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
