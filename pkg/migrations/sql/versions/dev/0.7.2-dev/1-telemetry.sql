CALL _prom_catalog.execute_everywhere('create_schemas', $ee$ DO $$ BEGIN
    CREATE SCHEMA IF NOT EXISTS _prom_catalog; -- this will be limited to metric and probably renamed in future
    GRANT USAGE ON SCHEMA _prom_catalog TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_api; -- public functions
    GRANT USAGE ON SCHEMA prom_api TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS _prom_ext; -- optimized versions of functions created by the extension
    GRANT USAGE ON SCHEMA _prom_ext TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_series; -- series views
    GRANT USAGE ON SCHEMA prom_series TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_metric; -- metric views
    GRANT USAGE ON SCHEMA prom_metric TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_data;
    GRANT USAGE ON SCHEMA prom_data TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_data_series;
    GRANT USAGE ON SCHEMA prom_data_series TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_info;
    GRANT USAGE ON SCHEMA prom_info TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS prom_data_exemplar;
    GRANT USAGE ON SCHEMA prom_data_exemplar TO prom_reader;
    GRANT ALL ON SCHEMA prom_data_exemplar TO prom_writer;

    CREATE SCHEMA IF NOT EXISTS ps_tag;
    GRANT USAGE ON SCHEMA ps_tag TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS _ps_trace;
    GRANT USAGE ON SCHEMA _ps_trace TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS ps_trace;
    GRANT USAGE ON SCHEMA ps_trace TO prom_reader;

    CREATE SCHEMA IF NOT EXISTS _ps_catalog;
    GRANT USAGE ON SCHEMA _ps_catalog TO prom_reader;
END $$ $ee$);

CREATE TABLE IF NOT EXISTS _ps_catalog.promscale_instance_information (
    uuid                                                UUID NOT NULL PRIMARY KEY,
    last_updated                                        TIMESTAMPTZ NOT NULL,
    promscale_ingested_samples_total                    BIGINT DEFAULT 0 NOT NULL,
    promscale_metrics_queries_executed_total            BIGINT DEFAULT 0 NOT NULL,
    promscale_metrics_queries_timedout_total            BIGINT DEFAULT 0 NOT NULL,
    promscale_metrics_queries_failed_total              BIGINT DEFAULT 0 NOT NULL,
    promscale_trace_query_requests_executed_total       BIGINT DEFAULT 0 NOT NULL,
    promscale_trace_dependency_requests_executed_total  BIGINT DEFAULT 0 NOT NULL,
    is_counter_reset_row                                BOOLEAN DEFAULT FALSE NOT NULL, -- counter reset row has '00000000-0000-0000-0000-000000000000' uuid
    CHECK((uuid = '00000000-0000-0000-0000-000000000000' OR NOT is_counter_reset_row) AND (uuid != '00000000-0000-0000-0000-000000000000' OR is_counter_reset_row))
);
GRANT SELECT ON TABLE _ps_catalog.promscale_instance_information TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _ps_catalog.promscale_instance_information TO prom_writer;
