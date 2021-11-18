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

-- Write a counter reset row, i.e., the first row in the table. Purpose:
-- The above telemetry_.* rows logically behave as counter. They get deleted by
-- telemetry-housekeeper promscale when last_updated is too old to be stale. Since
-- counters are always increasing, if these rows get deleted, it will result in data-loss.
-- To avoid this loss of data, we treat the first row as immutable, and use it for incrementing
-- the attributes of this row, with the values of the stale rows before they are deleted.
INSERT INTO SCHEMA_PS_CATALOG.promscale_instance_information
    VALUES ('00000000-0000-0000-0000-000000000000', current_timestamp, 0, 0, 0, 0, 0, 0, FALSE);
