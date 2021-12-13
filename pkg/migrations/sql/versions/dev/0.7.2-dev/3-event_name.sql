/*
    Remove the event_name_check check constraint on SCHEMA_TRACING.event if it exists
    You can't drop the constraint if compression is turned on for the table.
    If compression is on, turn it off.
*/
DO $block$
DECLARE
    _is_timescaledb_installed boolean = false;
    _is_timescaledb_oss boolean = true;
    _timescaledb_version_text text;
    _timescaledb_major_version int;
    _timescaledb_minor_version int;
    _is_compression_available boolean = false;
    _compression_enabled boolean = false;
    _chunk_name text;
BEGIN

    -- determine whether the constraint event exists
    -- if it doesn't, bail early
    IF
    (
        SELECT count(*) = 0
        FROM pg_constraint c
        INNER JOIN pg_class t on (c.conrelid = t.oid)
        INNER JOIN pg_namespace n on (c.connamespace = n.oid and t.relnamespace = n.oid)
        WHERE n.nspname = 'SCHEMA_TRACING'
        AND t.relname = 'event'
        AND c.conname = 'event_name_check'
        AND c.contype = 'c'
    )
    THEN
        RETURN;
    END IF;

    /*
        These functions do not exist until the
        idempotent scripts are executed, so we have
        to deal with it "manually"
        SCHEMA_CATALOG.get_timescale_major_version()
        SCHEMA_CATALOG.is_timescaledb_oss()
        SCHEMA_CATALOG.is_timescaledb_installed()
    */
    SELECT count(*) > 0
    INTO STRICT _is_timescaledb_installed
    FROM pg_extension
    WHERE extname='timescaledb';

    IF _is_timescaledb_installed THEN
        SELECT extversion INTO STRICT _timescaledb_version_text
        FROM pg_catalog.pg_extension
        WHERE extname='timescaledb'
        LIMIT 1;

        _timescaledb_major_version = split_part(_timescaledb_version_text, '.', 1)::INT;
        _timescaledb_minor_version = split_part(_timescaledb_version_text, '.', 2)::INT;

        _is_compression_available = CASE
            WHEN _timescaledb_major_version >= 2 THEN true
            WHEN _timescaledb_major_version = 1 and _timescaledb_minor_version >= 5 THEN true
            ELSE false
        END;

        IF _timescaledb_major_version >= 2 THEN
            _is_timescaledb_oss = (current_setting('timescaledb.license') = 'apache');
        ELSE
            _is_timescaledb_oss = (SELECT edition = 'apache' FROM timescaledb_information.license);
        END IF;
    END IF;

    IF _is_timescaledb_installed AND (NOT _is_timescaledb_oss) AND _is_compression_available THEN
        -- is compression enabled for the table?
        SELECT compression_enabled INTO _compression_enabled
        FROM timescaledb_information.hypertables
        WHERE hypertable_schema = 'SCHEMA_TRACING'
        AND hypertable_name = 'event'
        ;

        IF _compression_enabled THEN
            -- make a temp table listing any chunks of the event
            -- hypertable that are currently compressed
            DROP TABLE IF EXISTS _event_compressed_chunks;
            CREATE TEMPORARY TABLE _event_compressed_chunks (chunk_name text)
            ON COMMIT DROP;

            INSERT INTO _event_compressed_chunks (chunk_name)
            SELECT format('%I.%I',
                chunk_schema,
                chunk_name
            ) as chunk_name
            FROM timescaledb_information.chunks
            WHERE hypertable_schema = 'SCHEMA_TRACING'
            AND hypertable_name = 'event'
            AND is_compressed
            ;

            -- we have to decompress the chunks before we can drop the constraint
            FOR _chunk_name IN (SELECT * FROM _event_compressed_chunks)
            LOOP
                PERFORM decompress_chunk(_chunk_name, true);
            END LOOP;

            -- turn off compression
            ALTER TABLE SCHEMA_TRACING.event SET (timescaledb.compress=false);

            IF _timescaledb_major_version < 2 THEN
                BEGIN
                    PERFORM SCHEMA_TIMESCALE.remove_compression_policy('SCHEMA_TRACING.event', true);
                EXCEPTION
                    WHEN undefined_function THEN
                        RAISE NOTICE 'remove_compression_policy does not exist';
                END;
            END IF;
        END IF;
    END IF;

    -- drop the constraint
    ALTER TABLE SCHEMA_TRACING.event DROP CONSTRAINT IF EXISTS event_name_check;

    IF _compression_enabled THEN
        -- turn compression back on
        ALTER TABLE SCHEMA_TRACING.event SET (timescaledb.compress, timescaledb.compress_segmentby='trace_id,span_id');

        IF _timescaledb_major_version < 2 THEN
            BEGIN
                PERFORM SCHEMA_TIMESCALE.add_compression_policy('SCHEMA_TRACING.event', INTERVAL '1 hour');
            EXCEPTION
                WHEN undefined_function THEN
                    RAISE NOTICE 'add_compression_policy does not exist';
            END;
        END IF;

        -- recompress the chunks that were previously compressed
        FOR _chunk_name IN (SELECT * FROM _event_compressed_chunks)
        LOOP
            PERFORM compress_chunk(_chunk_name, true);
        END LOOP;
    END IF;
END;
$block$
;
