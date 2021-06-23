CREATE DOMAIN SCHEMA_PROM.label_value_array AS TEXT[];

CREATE SCHEMA IF NOT EXISTS SCHEMA_EXEMPLAR_DATA;
GRANT USAGE TO SCHEMA SCHEMA_EXEMPLAR_DATA TO prom_reader;
GRANT USAGE TO SCHEMA SCHEMA_EXEMPLAR_DATA TO prom_writer;

CREATE IF NOT EXISTS SCHEMA_CATALOG.exemplar_label_key_position (
    metric_name TEXT
    key         TEXT
    pos         INTEGER
);
GRANT SELECT ON TABLE SCHEMA_CATALOG.exemplar_label_key_position TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_CATALOG.exemplar_label_key_position TO prom_writer;

CREATE TABLE IF NOT EXISTS SCHEMA_CATALOG.exemplar (
    id          SERIAL,
    metric_name TEXT NOT NULL,
    table_name  TEXT NOT NULL
);
GRANT SELECT ON TABLE SCHEMA_CATALOG.ha_leases_logs TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_CATALOG.ha_leases_logs TO prom_writer;
-- todo: create indexes

-- todo: add indexes across all files related to this.