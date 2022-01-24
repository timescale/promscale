ALTER TABLE _prom_catalog.metric
    ADD COLUMN delay_compression_until TIMESTAMPTZ DEFAULT NULL;

    DROP PROCEDURE IF EXISTS _prom_catalog.decompress_chunks_after(NAME, TIMESTAMPTZ);
    DROP PROCEDURE IF EXISTS _prom_catalog.do_decompress_chunks_after(NAME, TIMESTAMPTZ);
    DROP PROCEDURE IF EXISTS _prom_catalog.compression_job(INT, JSONB);
