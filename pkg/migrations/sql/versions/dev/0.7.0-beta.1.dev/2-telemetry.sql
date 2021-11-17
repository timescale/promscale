GRANT SELECT, INSERT, UPDATE ON TABLE TS_CATALOG.metadata TO prom_writer;

CREATE SCHEMA IF NOT EXISTS SCHEMA_PS_CATALOG;
GRANT USAGE ON SCHEMA SCHEMA_PS_CATALOG TO prom_reader;
GRANT ALL ON SCHEMA SCHEMA_PS_CATALOG TO prom_writer;

CREATE TABLE IF NOT EXISTS SCHEMA_PS_CATALOG.promscale_instance_information (
    uuid                                            UUID NOT NULL PRIMARY KEY,
    last_updated                                    TIMESTAMPTZ NOT NULL,
    telemetry_metrics_ingested_samples              BIGINT NOT NULL,
    telemetry_metrics_queries_executed              BIGINT NOT NULL,
    telemetry_metrics_queries_timed_out             BIGINT NOT NULL,
    telemetry_metrics_queries_failed                BIGINT NOT NULL,
    telemetry_traces_queries_executed               BIGINT NOT NULL,
    telemetry_traces_dependency_queries_executed    BIGINT NOT NULL,
    deletable                                       BOOLEAN DEFAULT TRUE
);
GRANT SELECT ON TABLE SCHEMA_PS_CATALOG.promscale_instance_information TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_PS_CATALOG.promscale_instance_information TO prom_writer;

-- See the purpose of the below in sql/preinstall/013-telemetry.sql file.
INSERT INTO SCHEMA_PS_CATALOG.promscale_instance_information
    VALUES ('00000000-0000-0000-0000-000000000000', current_timestamp, 0, 0, 0, 0, 0, 0, FALSE);