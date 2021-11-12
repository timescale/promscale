CREATE SCHEMA IF NOT EXISTS SCHEMA_PS_CATALOG;
GRANT USAGE ON SCHEMA SCHEMA_PS_CATALOG TO prom_reader;
GRANT ALL ON SCHEMA SCHEMA_PS_CATALOG TO prom_writer;

CREATE TABLE IF NOT EXISTS SCHEMA_PS_CATALOG.promscale_instance_information (
    uuid                        UUID NOT NULL PRIMARY KEY,
    last_updated                TIMESTAMPTZ NOT NULL,
    telemetry_ingested_samples  BIGINT NOT NULL,
    telemetry_queries_executed  BIGINT NOT NULL,
    telemetry_queries_timed_out BIGINT NOT NULL,
    telemetry_queries_failed    BIGINT NOT NULL
);
GRANT SELECT ON TABLE SCHEMA_PS_CATALOG.promscale_instance_information TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_PS_CATALOG.promscale_instance_information TO prom_writer;