CREATE TABLE IF NOT EXISTS SCHEMA_CATALOG.exemplar_label_key_position (
    metric_name TEXT NOT NULL,
    key         TEXT NOT NULL,
    pos         INTEGER NOT NULL,
    PRIMARY KEY (metric_name, key)
);
GRANT SELECT ON TABLE SCHEMA_CATALOG.exemplar_label_key_position TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_CATALOG.exemplar_label_key_position TO prom_writer;

CREATE INDEX IF NOT EXISTS SCHEMA_CATALOG.exemplar_label_key_position_index ON SCHEMA_CATALOG.exemplar_label_key_position
(
    metric_name, key
);
GRANT SELECT ON TABLE SCHEMA_CATALOG.exemplar_label_key_position_index TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_CATALOG.exemplar_label_key_position_index TO prom_writer;

CREATE TABLE IF NOT EXISTS SCHEMA_CATALOG.exemplar (
    id          SERIAL,
    metric_name TEXT NOT NULL,
    table_name  TEXT NOT NULL
);
GRANT SELECT ON TABLE SCHEMA_CATALOG.exemplar TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_CATALOG.exemplar TO prom_writer;

CREATE INDEX IF NOT EXISTS SCHEMA_CATALOG.exemplar_index ON SCHEMA_CATALOG.exemplar
(
    metric_name, table_name
);
GRANT SELECT ON TABLE SCHEMA_CATALOG.exemplar_index TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_CATALOG.exemplar_index TO prom_writer;
