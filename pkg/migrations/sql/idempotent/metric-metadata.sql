
-- TODO: mov this to update files script.
CREATE TABLE IF NOT EXISTS SCHEMA_CATALOG.metadata
(
    id SERIAL NOT NULL PRIMARY KEY,
    last_seen TIMESTAMPTZ NOT NULL,
    metric_family TEXT NOT NULL,
    type TEXT DEFAULT NULL,
    unit TEXT DEFAULT NULL,
    help TEXT DEFAULT NULL,
    UNIQUE (metric_family, type, unit, help)
);

-- TODO: mov this to update files script.
CREATE UNIQUE INDEX IF NOT EXISTS metadata_index ON SCHEMA_CATALOG.metadata
(
    last_seen, metric_family
);

CREATE OR REPLACE FUNCTION prom_api.insert_metric_metadata(t TIMESTAMPTZ, metric_family_name TEXT, metric_type TEXT, metric_unit TEXT, metric_help TEXT)
RETURNS BIGINT
AS
$$
    DECLARE
        num_rows BIGINT;
    BEGIN
        INSERT INTO _prom_catalog.metadata (last_seen, metric_family, type, unit, help)  VALUES (t, metric_family_name, metric_type, metric_unit, metric_help)
            ON CONFLICT (metric_family, type, unit, help) DO
                UPDATE SET last_seen = t;
        GET DIAGNOSTICS num_rows = ROW_COUNT;
        RETURN num_rows;
    END;
$$ LANGUAGE plpgsql;
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.insert_metric_metadata(TIMESTAMPTZ, TEXT, TEXT, TEXT, TEXT) TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.get_metric_metadata(metric_family_name TEXT)
RETURNS TABLE (metric_family TEXT, type TEXT, unit TEXT, help TEXT)
AS
$$
    SELECT metric_family, type, unit, help FROM SCHEMA_CATALOG.metadata WHERE metric_family = metric_family_name ORDER BY last_seen LIMIT 1
$$ LANGUAGE SQL;
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.get_metric_metadata(TEXT) TO prom_writer;

-- metric_families should have unique elements, otherwise there will be duplicate rows in the returned table.
CREATE OR REPLACE FUNCTION SCHEMA_PROM.get_multiple_metric_metadata(metric_families TEXT[])
RETURNS TABLE (metric_family TEXT, type TEXT, unit TEXT, help TEXT)
AS
$$
    DECLARE
        m TEXT;
BEGIN
    create temporary table result (mf TEXT, t TEXT, u TEXT, h TEXT) ON COMMIT DROP;
    FOREACH m IN ARRAY metric_families
    LOOP
        INSERT INTO result
            SELECT d.metric_family, d.type, d.unit, d.help FROM SCHEMA_CATALOG.metadata d WHERE d.metric_family = m ORDER BY d.last_seen LIMIT 1;
    END LOOP;
    RETURN QUERY SELECT mf, t, u, h from result;
END;
$$ LANGUAGE plpgsql;
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.get_multiple_metric_metadata(TEXT[]) TO prom_writer;
