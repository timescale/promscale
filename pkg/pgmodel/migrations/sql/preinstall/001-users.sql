DO $$
    BEGIN
        CREATE ROLE prom_reader;
    EXCEPTION WHEN duplicate_object THEN
        RAISE NOTICE 'role prom_reader already exists, skipping create';
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