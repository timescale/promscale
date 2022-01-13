INSERT INTO SCHEMA_CATALOG.default(key, value)
VALUES ('trace_retention_period', (30 * INTERVAL '1 days')::text);
