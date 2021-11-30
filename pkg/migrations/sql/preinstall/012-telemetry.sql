CREATE TABLE IF NOT EXISTS SCHEMA_PS_CATALOG.promscale_instance_information (
    uuid                                                UUID NOT NULL PRIMARY KEY,
    last_updated                                        TIMESTAMPTZ NOT NULL,
    promscale_ingested_samples_total                    BIGINT DEFAULT 0,
    promscale_metrics_queries_executed_total            BIGINT DEFAULT 0,
    promscale_metrics_queries_timedout_total            BIGINT DEFAULT 0,
    promscale_metrics_queries_failed_total              BIGINT DEFAULT 0,
    promscale_trace_query_requests_executed_total       BIGINT DEFAULT 0,
    promscale_trace_dependency_requests_executed_total  BIGINT DEFAULT 0,
    is_counter_reset_row                                BOOLEAN DEFAULT FALSE
);
GRANT SELECT ON TABLE SCHEMA_PS_CATALOG.promscale_instance_information TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_PS_CATALOG.promscale_instance_information TO prom_writer;
