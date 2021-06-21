CREATE TABLE IF NOT EXISTS SCHEMA_CATALOG.exemplar_label_key_position (
    metric_name TEXT NOT NULL,
    key         TEXT NOT NULL,
    pos         INTEGER NOT NULL,
    PRIMARY KEY (metric_name, key)
);
-- todo: create indexes.
GRANT SELECT ON TABLE SCHEMA_CATALOG.exemplar_label_key_position TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_CATALOG.exemplar_label_key_position TO prom_writer;

CREATE TABLE IF NOT EXISTS SCHEMA_CATALOG.exemplar (
    id          SERIAL,
    metric_id   BIGINT NOT NULL,
    table_name  TEXT NOT NULL
);
-- todo: create indexes
