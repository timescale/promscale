CREATE OR REPLACE FUNCTION SCHEMA_PS_CATALOG.clean_stale_promscales_after_counter_reset()
RETURNS VOID AS
$$
    BEGIN
        -- Check if any stale row is present.
        IF (SELECT count(*) = 0 FROM SCHEMA_PS_CATALOG.promscale_instance_information
                WHERE
                      is_counter_reset_row = FALSE
                  AND
                      current_timestamp - last_updated > INTERVAL '1 HOUR') THEN
            RETURN;
        END IF;

        -- Stale rows are present. Update the counter reset row.
        UPDATE SCHEMA_PS_CATALOG.promscale_instance_information SET
            promscale_ingested_samples_total                    = subquery.counter_reset_metrics_ingested_samples + subquery.stale_metrics_ingested_samples,
            promscale_metrics_queries_executed_total            = subquery.counter_reset_metrics_queries_executed + subquery.stale_metrics_queries_executed,
            promscale_metrics_queries_timedout_total            = subquery.counter_reset_metrics_queries_timed_out + subquery.stale_metrics_queries_timed_out,
            promscale_metrics_queries_failed_total              = subquery.counter_reset_metrics_queries_failed + subquery.stale_metrics_queries_failed,
            promscale_trace_query_requests_executed_total       = subquery.counter_reset_traces_queries_executed + subquery.stale_traces_queries_executed,
            promscale_trace_dependency_requests_executed_total  = subquery.counter_reset_traces_dependency_queries_executed + subquery.stale_traces_dependency_queries_executed,
            last_updated = current_timestamp
        FROM
            (
                -- Basically, we are collecting all stale rows under `stale.` and the actual counter-reset row under `counter_reset.`
                -- and the are aggregated in the above SET step. We can aggregate it here, but its more cleaner to do in SET.
                SELECT
                    sum(stale.promscale_ingested_samples_total)  			        stale_metrics_ingested_samples,
                    sum(stale.promscale_metrics_queries_executed_total)  		    stale_metrics_queries_executed,
                    sum(stale.promscale_metrics_queries_timedout_total)  			stale_metrics_queries_timed_out,
                    sum(stale.promscale_metrics_queries_failed_total)  				stale_metrics_queries_failed,
                    sum(stale.promscale_trace_query_requests_executed_total)        stale_traces_queries_executed,
                    sum(stale.promscale_trace_dependency_requests_executed_total)  	stale_traces_dependency_queries_executed,

                    counter_reset.promscale_ingested_samples_total  			        counter_reset_metrics_ingested_samples,
                    counter_reset.promscale_metrics_queries_executed_total 			    counter_reset_metrics_queries_executed,
                    counter_reset.promscale_metrics_queries_timedout_total  			counter_reset_metrics_queries_timed_out,
                    counter_reset.promscale_metrics_queries_failed_total  			    counter_reset_metrics_queries_failed,
                    counter_reset.promscale_trace_query_requests_executed_total  	    counter_reset_traces_queries_executed,
                    counter_reset.promscale_trace_dependency_requests_executed_total    counter_reset_traces_dependency_queries_executed
                FROM
                    SCHEMA_PS_CATALOG.promscale_instance_information stale INNER JOIN SCHEMA_PS_CATALOG.promscale_instance_information counter_reset ON true
                WHERE 
                    counter_reset.is_counter_reset_row = TRUE AND (stale.is_counter_reset_row = FALSE AND current_timestamp - stale.last_updated > interval '1 HOUR') -- consider only those rows as stale who were last updated beyond an hour.
                GROUP BY
                    counter_reset.promscale_ingested_samples_total,
                    counter_reset.promscale_metrics_queries_executed_total,
                    counter_reset.promscale_metrics_queries_timedout_total,
                    counter_reset.promscale_metrics_queries_failed_total,
                    counter_reset.promscale_trace_query_requests_executed_total,
                    counter_reset.promscale_trace_dependency_requests_executed_total
            ) AS subquery
        WHERE is_counter_reset_row = TRUE;

        -- Delete the stale rows.
        DELETE FROM SCHEMA_PS_CATALOG.promscale_instance_information WHERE is_counter_reset_row = FALSE AND current_timestamp - last_updated > interval '1 HOUR';
    END;
$$
LANGUAGE PLPGSQL;