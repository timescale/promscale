/*
    If "vanilla" postgres is installed, do nothing.
    If timescaledb is installed, turn on compression for tracing tables.
*/
DO $block$
DECLARE
    _is_timescaledb_installed boolean = false;
    _is_timescaledb_oss boolean = true;
    _timescaledb_version_text text;
    _timescaledb_major_version int;
    _timescaledb_minor_version int;
    _is_compression_available boolean = false;
BEGIN
    /*
        These functions do not exist until the
        idempotent scripts are executed, so we have
        to deal with it "manually"
        _prom_catalog.get_timescale_major_version()
        _prom_catalog.is_timescaledb_oss()
        _prom_catalog.is_timescaledb_installed()
        _prom_catalog.is_multinode()
        _prom_catalog.get_default_chunk_interval()
        _prom_catalog.get_staggered_chunk_interval(...)
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

    IF _is_timescaledb_installed
        AND (NOT _is_timescaledb_oss)
        AND _is_compression_available
        AND _timescaledb_major_version < 2 THEN
        BEGIN
            PERFORM public.add_compression_policy('_ps_trace.span', INTERVAL '1 hour');
            PERFORM public.add_compression_policy('_ps_trace.event', INTERVAL '1 hour');
            PERFORM public.add_compression_policy('_ps_trace.link', INTERVAL '1 hour');
        EXCEPTION
            WHEN undefined_function THEN
                RAISE NOTICE 'add_compression_policy does not exist';
        END;
    END IF;

    ALTER TABLE _ps_trace.span
        ADD CONSTRAINT span_span_id_check CHECK (span_id != 0),
        ADD CONSTRAINT span_parent_span_id_check CHECK (parent_span_id != 0);

    ALTER TABLE _ps_trace.event
        ADD CONSTRAINT event_span_id_check CHECK (span_id != 0);

    ALTER TABLE _ps_trace.link
        ADD CONSTRAINT link_span_id_check CHECK (span_id != 0),
        ADD CONSTRAINT link_linked_span_id_check CHECK (linked_span_id != 0);
END;
$block$
;

DROP FUNCTION IF EXISTS ps_trace.put_tag_key(ps_trace.tag_k, ps_trace.tag_type);
DROP FUNCTION IF EXISTS ps_trace.put_tag(ps_trace.tag_k, ps_trace.tag_v, ps_trace.tag_type);
