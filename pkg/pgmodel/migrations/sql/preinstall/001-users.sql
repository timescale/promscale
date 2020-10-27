DO $$
    BEGIN
        CREATE ROLE prom_reader;
    EXCEPTION WHEN duplicate_object THEN
        RAISE NOTICE 'role prom_reader already exists, skipping create';
        RETURN;
    END
$$;

-- mirror on distributed if we're distributed
DO $$
    BEGIN
        CALL distributed_exec($dist$ DO
            $inner$
                BEGIN
                    CREATE ROLE prom_reader;
                EXCEPTION WHEN duplicate_object THEN
                    RETURN;
                END
            $inner$;
        $dist$);
    EXCEPTION WHEN SQLSTATE '0A000' THEN
        -- we're not the acess node, just return
        RETURN;
    END
$$;

DO $$
    BEGIN
        CREATE ROLE prom_writer;
    EXCEPTION WHEN duplicate_object THEN
        RAISE NOTICE 'role prom_writer already exists, skipping create';
        RETURN;
    END
$$;
GRANT prom_reader TO prom_writer;

-- mirror on distributed if we're distributed
DO $$
    BEGIN
        CALL distributed_exec($dist$ DO
            $inner$
                BEGIN
                    CREATE ROLE prom_writer;
                EXCEPTION WHEN duplicate_object THEN
                    RETURN;
                END
            $inner$;
        $dist$);
    EXCEPTION WHEN SQLSTATE '0A000' THEN
        -- we're not the acess node, just return
        RETURN;
    END
$$;

DO $$
    BEGIN
        CALL distributed_exec($dist$ GRANT prom_reader TO prom_writer; $dist$);
    EXCEPTION WHEN SQLSTATE '0A000' THEN
        -- we're not the acess node, just return
        RETURN;
    END
$$;
