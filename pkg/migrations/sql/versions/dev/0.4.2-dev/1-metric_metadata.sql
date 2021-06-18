CREATE TABLE IF NOT EXISTS SCHEMA_CATALOG.metadata
(
    last_seen TIMESTAMPTZ NOT NULL,
    metric_family TEXT NOT NULL,
    type TEXT DEFAULT NULL,
    unit TEXT DEFAULT NULL,
    help TEXT DEFAULT NULL,
    PRIMARY KEY (metric_family, type, unit, help)
);
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_CATALOG.metadata TO prom_writer;
GRANT SELECT ON TABLE SCHEMA_CATALOG.metadata TO prom_reader;

CREATE INDEX IF NOT EXISTS metadata_index ON SCHEMA_CATALOG.metadata
(
    metric_family, last_seen
);
