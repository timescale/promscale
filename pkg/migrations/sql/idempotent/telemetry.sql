-- promscale_telemetry_housekeeping() does telemetry housekeeping stuff, which includes
-- searching the table for telemetry_sync_duration, looking for stale promscales, if found,
-- adding their values into the counter_reset row, and then cleaning up the stale
-- promscale instances.
-- It is concurrency safe, since it takes lock on the promscale_instance_information table,
-- making sure that at one time, only one housekeeping is being done.
-- 
-- It returns TRUE if last run was beyond telemetry_sync_duration, otherwise FALSE.
CREATE OR REPLACE FUNCTION SCHEMA_PS_CATALOG.promscale_telemetry_housekeeping(telemetry_sync_duration INTERVAL DEFAULT INTERVAL '1 HOUR')
RETURNS BOOLEAN AS
$$
    DECLARE
        should_update_telemetry BOOLEAN;
    BEGIN
        BEGIN
            LOCK TABLE SCHEMA_PS_CATALOG.promscale_instance_information IN ACCESS EXCLUSIVE MODE NOWAIT; -- Do not wait for the lock as some promscale is already cleaning up the stuff.
        EXCEPTION
            WHEN SQLSTATE '55P03' THEN
                RETURN FALSE;
        END;

        -- This guarantees that we update our telemetry once every telemetry_sync_duration.
        SELECT count(*) = 0 INTO should_update_telemetry FROM SCHEMA_PS_CATALOG.promscale_instance_information
            WHERE is_counter_reset_row = TRUE AND current_timestamp - last_updated < telemetry_sync_duration;

        IF NOT should_update_telemetry THEN
            -- Some Promscale did the housekeeping work within the expected interval. Hence, nothing to do, so exit.
            RETURN FALSE;
        END IF;


        WITH deleted_rows AS (
            DELETE FROM SCHEMA_PS_CATALOG.promscale_instance_information 
            WHERE is_counter_reset_row = FALSE AND current_timestamp - last_updated > (telemetry_sync_duration * 2) -- consider adding stats of deleted rows to persist counter reset behaviour.
            RETURNING *
        )
        UPDATE SCHEMA_PS_CATALOG.promscale_instance_information SET
            promscale_ingested_samples_total                    = promscale_ingested_samples_total + COALESCE(x.del_promscale_ingested_samples_total, 0),
            promscale_metrics_queries_executed_total            = promscale_metrics_queries_executed_total + COALESCE(x.del_promscale_metrics_queries_executed_total, 0),
            promscale_metrics_queries_timedout_total            = promscale_metrics_queries_timedout_total + COALESCE(x.del_promscale_metrics_queries_timedout_total, 0),
            promscale_metrics_queries_failed_total              = promscale_metrics_queries_failed_total + COALESCE(x.del_promscale_metrics_queries_failed_total, 0),
            promscale_trace_query_requests_executed_total       = promscale_trace_query_requests_executed_total + COALESCE(x.del_promscale_trace_query_requests_executed_total, 0),
            promscale_trace_dependency_requests_executed_total  = promscale_trace_dependency_requests_executed_total + COALESCE(x.del_promscale_trace_dependency_requests_executed_total, 0),
            last_updated = current_timestamp
        FROM
        (
            SELECT
                sum(promscale_ingested_samples_total)  			        as del_promscale_ingested_samples_total,
                sum(promscale_metrics_queries_executed_total)  		    as del_promscale_metrics_queries_executed_total,
                sum(promscale_metrics_queries_timedout_total)  		    as del_promscale_metrics_queries_timedout_total,
                sum(promscale_metrics_queries_failed_total)  		    as del_promscale_metrics_queries_failed_total,
                sum(promscale_trace_query_requests_executed_total)      as del_promscale_trace_query_requests_executed_total,
                sum(promscale_trace_dependency_requests_executed_total) as del_promscale_trace_dependency_requests_executed_total
            FROM
                deleted_rows
        ) x
        WHERE is_counter_reset_row = TRUE;

        RETURN TRUE;
    END;
$$
LANGUAGE PLPGSQL;