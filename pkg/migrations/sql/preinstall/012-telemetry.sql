CREATE TABLE IF NOT EXISTS _ps_catalog.promscale_instance_information (
    uuid                                                UUID NOT NULL PRIMARY KEY,
    last_updated                                        TIMESTAMPTZ NOT NULL,
    promscale_ingested_samples_total                    BIGINT DEFAULT 0 NOT NULL,
    promscale_metrics_queries_success_total             BIGINT DEFAULT 0 NOT NULL,
    promscale_metrics_queries_timedout_total            BIGINT DEFAULT 0 NOT NULL,
    promscale_metrics_queries_failed_total              BIGINT DEFAULT 0 NOT NULL,
    promscale_trace_query_requests_executed_total       BIGINT DEFAULT 0 NOT NULL,
    promscale_trace_dependency_requests_executed_total  BIGINT DEFAULT 0 NOT NULL,
    is_counter_reset_row                                BOOLEAN DEFAULT FALSE NOT NULL, -- counter reset row has '00000000-0000-0000-0000-000000000000' uuid
    promscale_ingested_spans_total                      BIGINT DEFAULT 0 NOT NULL
    CHECK((uuid = '00000000-0000-0000-0000-000000000000' OR NOT is_counter_reset_row) AND (uuid != '00000000-0000-0000-0000-000000000000' OR is_counter_reset_row))
);
GRANT SELECT ON TABLE _ps_catalog.promscale_instance_information TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _ps_catalog.promscale_instance_information TO prom_writer;

-- Write a counter reset row, i.e., the first row in the table. Purpose:
-- The above promscale_.* rows logically behave as counter. They get deleted by
-- telemetry-housekeeper promscale when last_updated is too old to be stale. Since
-- counters are always increasing, if these rows get deleted, it will result in data-loss.
-- To avoid this loss of data, we treat the first row as immutable, and use it for incrementing
-- the attributes of this row, with the values of the stale rows before they are deleted.
INSERT INTO _ps_catalog.promscale_instance_information (uuid, last_updated, is_counter_reset_row)
    VALUES ('00000000-0000-0000-0000-000000000000', '2021-12-09 00:00:00'::TIMESTAMPTZ, TRUE);