CREATE TABLE IF NOT EXISTS SCHEMA_PS_CATALOG.promscale_instance_information (
    uuid                        TEXT NOT NULL PRIMARY KEY,
    last_updated                TIMESTAMPTZ NOT NULL,
    telemetry_executed_queries  BIGINT DEFAULT 0,
    telemetry_queries_timed_out BIGINT DEFAULT 0,
    telemetry_queries_failed    BIGINT DEFAULT 0,
    telemetry_ingested_samples  BIGINT DEFAULT 0
);
GRANT SELECT ON TABLE SCHEMA_PS_CATALOG.promscale_instance_information TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_PS_CATALOG.promscale_instance_information TO prom_writer;