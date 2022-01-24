INSERT INTO _prom_catalog.default(key, value)
VALUES ('trace_retention_period', (30 * INTERVAL '1 days')::text);
