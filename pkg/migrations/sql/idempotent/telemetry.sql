CREATE OR REPLACE FUNCTION SCHEMA_PS_CATALOG.clean_stale_promscales_after_counter_reset()
RETURNS VOID AS
$$
    BEGIN
        -- Check if any stale row is present.
        IF (SELECT count(*) = 0 FROM SCHEMA_PS_CATALOG.promscale_instance_information
            WHERE
                deletable = TRUE
              AND
                current_timestamp - last_updated > INTERVAL '1 HOUR') THEN
            RETURN;
        END IF;

        -- Stale rows are present. Update the counter reset row.
        UPDATE SCHEMA_PS_CATALOG.promscale_instance_information SET
            telemetry_metrics_ingested_samples = subquery.counter_reset_telemetry_metrics_ingested_samples + subquery.stale_telemetry_metrics_ingested_samples,
            telemetry_metrics_queries_executed = subquery.counter_reset_telemetry_metrics_queries_executed + subquery.stale_telemetry_metrics_queries_executed,
            telemetry_metrics_queries_timed_out = subquery.counter_reset_telemetry_metrics_queries_timed_out + subquery.stale_telemetry_metrics_queries_timed_out,
            telemetry_metrics_queries_failed = subquery.counter_reset_telemetry_metrics_queries_failed + subquery.stale_telemetry_metrics_queries_failed,
            telemetry_traces_queries_executed = subquery.counter_reset_telemetry_traces_queries_executed + subquery.stale_telemetry_traces_queries_executed,
            telemetry_traces_dependency_queries_executed = subquery.counter_reset_telemetry_traces_dependency_queries_executed + subquery.stale_telemetry_traces_dependency_queries_executed,
            last_updated = current_timestamp
        FROM
            (
                SELECT
                    sum(stale.telemetry_metrics_ingested_samples)  				stale_telemetry_metrics_ingested_samples,
                    sum(stale.telemetry_metrics_queries_executed)  				stale_telemetry_metrics_queries_executed,
                    sum(stale.telemetry_metrics_queries_timed_out)  			stale_telemetry_metrics_queries_timed_out,
                    sum(stale.telemetry_metrics_queries_failed)  				stale_telemetry_metrics_queries_failed,
                    sum(stale.telemetry_traces_queries_executed)  				stale_telemetry_traces_queries_executed,
                    sum(stale.telemetry_traces_dependency_queries_executed)  	stale_telemetry_traces_dependency_queries_executed,

                    counter_reset.telemetry_metrics_ingested_samples  			counter_reset_telemetry_metrics_ingested_samples,
                    counter_reset.telemetry_metrics_queries_executed 			counter_reset_telemetry_metrics_queries_executed,
                    counter_reset.telemetry_metrics_queries_timed_out  			counter_reset_telemetry_metrics_queries_timed_out,
                    counter_reset.telemetry_metrics_queries_failed  			counter_reset_telemetry_metrics_queries_failed,
                    counter_reset.telemetry_traces_queries_executed  			counter_reset_telemetry_traces_queries_executed,
                    counter_reset.telemetry_traces_dependency_queries_executed  counter_reset_telemetry_traces_dependency_queries_executed
                FROM
                    SCHEMA_PS_CATALOG.promscale_instance_information stale INNER JOIN SCHEMA_PS_CATALOG.promscale_instance_information counter_reset ON true
                WHERE
                    stale.deletable = true AND counter_reset.deletable = false
                GROUP BY
                    counter_reset.telemetry_metrics_ingested_samples,
                    counter_reset.telemetry_metrics_queries_executed,
                    counter_reset.telemetry_metrics_queries_timed_out,
                    counter_reset.telemetry_metrics_queries_failed,
                    counter_reset.telemetry_traces_queries_executed,
                    counter_reset.telemetry_traces_dependency_queries_executed
            ) AS subquery
        WHERE deletable = false AND current_timestamp - last_updated > interval '1 HOUR';

        -- Delete the stale rows.
        DELETE FROM SCHEMA_PS_CATALOG.promscale_instance_information WHERE deletable = TRUE AND current_timestamp - last_updated > interval '1 HOUR';
    END;
$$
LANGUAGE PLPGSQL;