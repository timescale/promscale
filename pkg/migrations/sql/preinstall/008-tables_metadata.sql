CREATE TABLE IF NOT EXISTS _prom_catalog.metadata
(
    last_seen TIMESTAMPTZ NOT NULL,
    metric_family TEXT NOT NULL,
    type TEXT DEFAULT NULL,
    unit TEXT DEFAULT NULL,
    help TEXT DEFAULT NULL,
    PRIMARY KEY (metric_family, type, unit, help)
);
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _prom_catalog.metadata TO prom_writer;
GRANT SELECT ON TABLE _prom_catalog.metadata TO prom_reader;

CREATE INDEX IF NOT EXISTS metadata_index ON _prom_catalog.metadata
(
    metric_family, last_seen
);
