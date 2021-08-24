CREATE TABLE IF NOT EXISTS SCHEMA_CATALOG.exemplar_label_key_position (
    metric_name TEXT NOT NULL,
    key         TEXT NOT NULL,
    pos         INTEGER NOT NULL,
    PRIMARY KEY (metric_name, key) INCLUDE (pos)
);
GRANT SELECT ON TABLE SCHEMA_CATALOG.exemplar_label_key_position TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_CATALOG.exemplar_label_key_position TO prom_writer;

CREATE TABLE IF NOT EXISTS SCHEMA_CATALOG.exemplar (
    id          SERIAL PRIMARY KEY,
    metric_name TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    UNIQUE (metric_name) INCLUDE (table_name, id)
);
GRANT SELECT ON TABLE SCHEMA_CATALOG.exemplar TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_CATALOG.exemplar TO prom_writer;

GRANT USAGE, SELECT ON SEQUENCE exemplar_id_seq TO prom_writer;
