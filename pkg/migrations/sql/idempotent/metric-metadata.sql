CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.insert_metric_metadatas(t TIMESTAMPTZ[], metric_family_name TEXT[], metric_type TEXT[], metric_unit TEXT[], metric_help TEXT[])
RETURNS BIGINT
AS
$$
    DECLARE
        num_rows BIGINT;
    BEGIN
        INSERT INTO SCHEMA_CATALOG.metadata (last_seen, metric_family, type, unit, help)
            SELECT * FROM UNNEST($1, $2, $3, $4, $5) res(last_seen, metric_family, type, unit, help)
                ORDER BY res.metric_family, res.type, res.unit, res.help
        ON CONFLICT (metric_family, type, unit, help) DO
            UPDATE SET last_seen = EXCLUDED.last_seen;
        GET DIAGNOSTICS num_rows = ROW_COUNT;
        RETURN num_rows;
    END;
$$ LANGUAGE plpgsql;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.insert_metric_metadatas(TIMESTAMPTZ[], TEXT[], TEXT[], TEXT[], TEXT[]) TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.get_metric_metadata(metric_family_name TEXT)
RETURNS TABLE (metric_family TEXT, type TEXT, unit TEXT, help TEXT)
AS
$$
    SELECT metric_family, type, unit, help FROM SCHEMA_CATALOG.metadata WHERE metric_family = metric_family_name ORDER BY last_seen DESC
$$ LANGUAGE SQL;
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.get_metric_metadata(TEXT) TO prom_reader;

-- metric_families should have unique elements, otherwise there will be duplicate rows in the returned table.
CREATE OR REPLACE FUNCTION SCHEMA_PROM.get_multiple_metric_metadata(metric_families TEXT[])
RETURNS TABLE (metric_family TEXT, type TEXT, unit TEXT, help TEXT)
AS
$$
    SELECT info.*
        FROM unnest(metric_families) AS family(name)
    INNER JOIN LATERAL (
        SELECT metric_family, type, unit, help FROM SCHEMA_CATALOG.metadata WHERE metric_family = family.name ORDER BY last_seen DESC LIMIT 1
    ) AS info ON (true)
$$ LANGUAGE SQL;
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.get_multiple_metric_metadata(TEXT[]) TO prom_reader;
