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
GRANT EXECUTE ON FUNCTION SCHEMA_PS_CATALOG.promscale_telemetry_housekeeping(INTERVAL) TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_PS_CATALOG.promscale_sql_telemetry() RETURNS VOID AS
$$
    DECLARE result TEXT;
    BEGIN
        -- Metrics telemetry.
        SELECT count(*)::TEXT INTO result FROM SCHEMA_CATALOG.metric;
        PERFORM SCHEMA_PS_CATALOG.apply_telemetry('metrics_total', result);

        SELECT sum(hypertable_size(format('SCHEMA_DATA.%I', table_name)))::TEXT INTO result FROM SCHEMA_CATALOG.metric;
        PERFORM SCHEMA_PS_CATALOG.apply_telemetry('metrics_bytes_total', result);

        SELECT approximate_row_count('SCHEMA_CATALOG.series')::TEXT INTO result;
        PERFORM SCHEMA_PS_CATALOG.apply_telemetry('metrics_series_total_approx', result);

        SELECT count(*)::TEXT INTO result FROM SCHEMA_CATALOG.label WHERE key = '__tenant__';
        PERFORM SCHEMA_PS_CATALOG.apply_telemetry('metrics_multi_tenancy_tenant_count', result);

        SELECT count(*)::TEXT INTO result FROM SCHEMA_CATALOG.label_key WHERE key = '__cluster__';
        PERFORM SCHEMA_PS_CATALOG.apply_telemetry('metrics_ha_cluster_count', result);

        SELECT count(*)::TEXT INTO result FROM SCHEMA_CATALOG.metric WHERE is_view IS true;
        PERFORM SCHEMA_PS_CATALOG.apply_telemetry('metrics_registered_views', result);

        SELECT count(*)::TEXT INTO result FROM SCHEMA_CATALOG.exemplar;
        PERFORM SCHEMA_PS_CATALOG.apply_telemetry('metrics_exemplar_total', result);

        SELECT count(*)::TEXT INTO result FROM SCHEMA_CATALOG.metadata;
        PERFORM SCHEMA_PS_CATALOG.apply_telemetry('metrics_metadata_total', result);

        SELECT value INTO result FROM SCHEMA_CATALOG.default WHERE key = 'retention_period';
        PERFORM SCHEMA_PS_CATALOG.apply_telemetry('metrics_default_retention', result);

        SELECT value INTO result FROM SCHEMA_CATALOG.default WHERE key = 'chunk_interval';
        PERFORM SCHEMA_PS_CATALOG.apply_telemetry('metrics_default_chunk_interval', result);

        -- Traces telemetry.
        SELECT (CASE
                    WHEN n_distinct >= 0 THEN
                        n_distinct
                    ELSE
                        -n_distinct * approximate_row_count('SCHEMA_TRACING.span')
                END)::TEXT INTO result
        FROM pg_stats
        WHERE schemaname='SCHEMA_TRACING' AND tablename='span' AND attname='trace_id' AND inherited;
        PERFORM SCHEMA_PS_CATALOG.apply_telemetry('traces_total_approx', result);

        SELECT approximate_row_count('SCHEMA_TRACING.span')::TEXT INTO result;
        PERFORM SCHEMA_PS_CATALOG.apply_telemetry('traces_spans_total_approx', result);

        SELECT hypertable_size('SCHEMA_TRACING.span')::TEXT INTO result;
        PERFORM SCHEMA_PS_CATALOG.apply_telemetry('traces_spans_bytes_total', result);

        -- Others.
        -- The -1 is to ignore the row summing deleted rows i.e., the counter reset row. 
        SELECT (count(*) - 1)::TEXT INTO result FROM SCHEMA_PS_CATALOG.promscale_instance_information;
        PERFORM SCHEMA_PS_CATALOG.apply_telemetry('connector_instance_total', result);

        SELECT count(*)::TEXT INTO result FROM timescaledb_information.data_nodes;
        PERFORM SCHEMA_PS_CATALOG.apply_telemetry('db_node_count', result);
    END;
$$
LANGUAGE PLPGSQL;
GRANT EXECUTE ON FUNCTION SCHEMA_PS_CATALOG.promscale_sql_telemetry() TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_PS_CATALOG.apply_telemetry(telemetry_name TEXT, telemetry_value TEXT) RETURNS VOID AS
$$
    BEGIN
        IF telemetry_value IS NULL THEN
            telemetry_value := '0';
        END IF;

        -- First try to use promscale_extension to fill the metadata table.
        PERFORM update_tsprom_metadata(telemetry_name, telemetry_value, TRUE);

        -- If promscale_extension is not installed, the above line will fail. Hence, catch the exception and try the manual way.
        EXCEPTION WHEN OTHERS THEN
            -- If this fails, throw an error so that the connector can log (or not) as appropriate.
            INSERT INTO _timescaledb_catalog.metadata(key, value, include_in_telemetry) VALUES ('promscale_' || telemetry_name, telemetry_value, TRUE) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, include_in_telemetry = EXCLUDED.include_in_telemetry;
    END;
$$
LANGUAGE PLPGSQL;
GRANT EXECUTE ON FUNCTION SCHEMA_PS_CATALOG.apply_telemetry(TEXT, TEXT) TO prom_writer;
