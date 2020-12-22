CREATE OR REPLACE PROCEDURE execute_everywhere(command TEXT, transactional BOOLEAN = true)
AS $func$
BEGIN
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
