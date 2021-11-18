CALL SCHEMA_CATALOG.execute_everywhere('create_schemas', $ee$ DO $$ BEGIN
    CREATE SCHEMA IF NOT EXISTS SCHEMA_CATALOG; -- this will be limited to metric and probably renamed in future
    GRANT USAGE ON SCHEMA SCHEMA_CATALOG TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_PROM; -- public functions
    GRANT USAGE ON SCHEMA SCHEMA_PROM TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_EXT; -- optimized versions of functions created by the extension
    GRANT USAGE ON SCHEMA SCHEMA_EXT TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_SERIES; -- series views
    GRANT USAGE ON SCHEMA SCHEMA_SERIES TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_METRIC; -- metric views
    GRANT USAGE ON SCHEMA SCHEMA_METRIC TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_DATA;
    GRANT USAGE ON SCHEMA SCHEMA_DATA TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_DATA_SERIES;
    GRANT USAGE ON SCHEMA SCHEMA_DATA_SERIES TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_INFO;
    GRANT USAGE ON SCHEMA SCHEMA_INFO TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_DATA_EXEMPLAR;
    GRANT USAGE ON SCHEMA SCHEMA_DATA_EXEMPLAR TO prom_reader;
    GRANT ALL ON SCHEMA SCHEMA_DATA_EXEMPLAR TO prom_writer;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_TAG;
    GRANT USAGE ON SCHEMA SCHEMA_TAG TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_TRACING;
    GRANT USAGE ON SCHEMA SCHEMA_TRACING TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_TRACING_PUBLIC;
    GRANT USAGE ON SCHEMA SCHEMA_TRACING_PUBLIC TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS SCHEMA_PS_CATALOG;
    GRANT USAGE ON SCHEMA SCHEMA_PS_CATALOG TO prom_reader;
END $$ $ee$);

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
