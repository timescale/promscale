--Order by random with stable marking gives us same order in a statement and different
-- orderings in different statements
CREATE OR REPLACE FUNCTION _prom_catalog.get_metrics_that_need_drop_chunk()
RETURNS SETOF _prom_catalog.metric
AS $$
BEGIN
        IF NOT _prom_catalog.is_timescaledb_installed() THEN
                    -- no real shortcut to figure out if deletion needed, return all
                    RETURN QUERY
                    SELECT m.*
                    FROM _prom_catalog.metric m
                    WHERE is_view = FALSE
                    ORDER BY random();
                    RETURN;
        END IF;

        RETURN QUERY
        SELECT m.*
        FROM _prom_catalog.metric m
        WHERE EXISTS (
            SELECT 1 FROM
            _prom_catalog.get_storage_hypertable_info(m.table_schema, m.table_name, m.is_view) hi
            INNER JOIN public.show_chunks(hi.hypertable_relation,
                         older_than=>NOW() - _prom_catalog.get_metric_retention_period(m.table_schema, m.metric_name)) sc ON TRUE)
        --random order also to prevent starvation
        ORDER BY random();
        RETURN;
END
$$
LANGUAGE PLPGSQL STABLE;
GRANT EXECUTE ON FUNCTION _prom_catalog.get_metrics_that_need_drop_chunk() TO prom_reader;

--drop chunks from metrics tables and delete the appropriate series.
CREATE OR REPLACE FUNCTION _prom_catalog.drop_metric_chunk_data(
    schema_name TEXT, metric_name TEXT, older_than TIMESTAMPTZ
) RETURNS VOID AS $func$
DECLARE
    metric_schema NAME;
    metric_table NAME;
    metric_view BOOLEAN;
    _is_cagg BOOLEAN;
BEGIN
    SELECT table_schema, table_name, is_view
    INTO STRICT metric_schema, metric_table, metric_view
    FROM _prom_catalog.get_metric_table_name_if_exists(schema_name, metric_name);

    -- need to check for caggs when dealing with metric views
    IF metric_view THEN
        SELECT is_cagg, cagg_schema, cagg_name
        INTO _is_cagg, metric_schema, metric_table
        FROM _prom_catalog.get_cagg_info(schema_name, metric_name);
        IF NOT _is_cagg THEN
          RETURN;
        END IF;
    END IF;

    IF _prom_catalog.is_timescaledb_installed() THEN
        IF _prom_catalog.get_timescale_major_version() >= 2 THEN
            PERFORM public.drop_chunks(
                relation=>format('%I.%I', metric_schema, metric_table),
                older_than=>older_than
            );
        ELSE
            PERFORM public.drop_chunks(
                table_name=>metric_table,
                schema_name=> metric_schema,
                older_than=>older_than,
                cascade_to_materializations=>FALSE
            );
        END IF;
    ELSE
        EXECUTE format($$ DELETE FROM %I.%I WHERE time < %L $$, metric_schema, metric_table, older_than);
    END IF;
END
$func$
LANGUAGE PLPGSQL VOLATILE
--security definer to add jobs as the logged-in user
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON FUNCTION _prom_catalog.drop_metric_chunk_data(text, text, timestamptz) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION _prom_catalog.drop_metric_chunk_data(text, text, timestamptz) TO prom_maintenance;

--drop chunks from metrics tables and delete the appropriate series.
CREATE OR REPLACE PROCEDURE _prom_catalog.drop_metric_chunks(
    schema_name TEXT, metric_name TEXT, older_than TIMESTAMPTZ, ran_at TIMESTAMPTZ = now(), log_verbose BOOLEAN = FALSE
) AS $func$
DECLARE
    metric_id int;
    metric_schema NAME;
    metric_table NAME;
    metric_series_table NAME;
    is_metric_view BOOLEAN;
    check_time TIMESTAMPTZ;
    time_dimension_id INT;
    last_updated TIMESTAMPTZ;
    present_epoch BIGINT;
    lastT TIMESTAMPTZ;
    startT TIMESTAMPTZ;
BEGIN
    SELECT id, table_schema, table_name, series_table, is_view
    INTO STRICT metric_id, metric_schema, metric_table, metric_series_table, is_metric_view
    FROM _prom_catalog.get_metric_table_name_if_exists(schema_name, metric_name);

    SELECT older_than + INTERVAL '1 hour'
    INTO check_time;

    startT := clock_timestamp();

    PERFORM _prom_catalog.set_app_name(format('promscale maintenance: data retention: metric %s', metric_name));
    IF log_verbose THEN
        RAISE LOG 'promscale maintenance: data retention: metric %: starting', metric_name;
    END IF;

    -- transaction 1
        IF _prom_catalog.is_timescaledb_installed() THEN
            --Get the time dimension id for the time dimension
            SELECT d.id
            INTO STRICT time_dimension_id
            FROM _timescaledb_catalog.dimension d
            INNER JOIN _prom_catalog.get_storage_hypertable_info(metric_schema, metric_table, is_metric_view) hi ON (hi.id = d.hypertable_id)
            ORDER BY d.id ASC
            LIMIT 1;

            --Get a tight older_than (EXCLUSIVE) because we want to know the
            --exact cut-off where things will be dropped
            SELECT _timescaledb_internal.to_timestamp(range_end)
            INTO older_than
            FROM _timescaledb_catalog.chunk c
            INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (c.id = cc.chunk_id)
            INNER JOIN _timescaledb_catalog.dimension_slice ds ON (ds.id = cc.dimension_slice_id)
            --range_end is exclusive so this is everything < older_than (which is also exclusive)
            WHERE ds.dimension_id = time_dimension_id AND ds.range_end <= _timescaledb_internal.to_unix_microseconds(older_than)
            ORDER BY range_end DESC
            LIMIT 1;
        END IF;
        -- end this txn so we're not holding any locks on the catalog

        SELECT current_epoch, last_update_time INTO present_epoch, last_updated FROM
            _prom_catalog.ids_epoch LIMIT 1;
    COMMIT;

    IF older_than IS NULL THEN
        -- even though there are no new Ids in need of deletion,
        -- we may still have old ones to delete
        lastT := clock_timestamp();
        PERFORM _prom_catalog.set_app_name(format('promscale maintenance: data retention: metric %s: delete expired series', metric_name));
        PERFORM _prom_catalog.delete_expired_series(metric_schema, metric_table, metric_series_table, ran_at, present_epoch, last_updated);
        IF log_verbose THEN
            RAISE LOG 'promscale maintenance: data retention: metric %: done deleting expired series as only action in %', metric_name, clock_timestamp()-lastT;
            RAISE LOG 'promscale maintenance: data retention: metric %: finished in %', metric_name, clock_timestamp()-startT;
        END IF;
        RETURN;
    END IF;

    -- transaction 2
        lastT := clock_timestamp();
        PERFORM _prom_catalog.set_app_name(format('promscale maintenance: data retention: metric %s: mark unused series', metric_name));
        PERFORM _prom_catalog.mark_unused_series(metric_schema, metric_table, metric_series_table, older_than, check_time);
        IF log_verbose THEN
            RAISE LOG 'promscale maintenance: data retention: metric %: done marking unused series in %', metric_name, clock_timestamp()-lastT;
        END IF;
    COMMIT;

    -- transaction 3
        lastT := clock_timestamp();
        PERFORM _prom_catalog.set_app_name( format('promscale maintenance: data retention: metric %s: drop chunks', metric_name));
        PERFORM _prom_catalog.drop_metric_chunk_data(metric_schema, metric_name, older_than);
        IF log_verbose THEN
            RAISE LOG 'promscale maintenance: data retention: metric %: done dropping chunks in %', metric_name, clock_timestamp()-lastT;
        END IF;
        SELECT current_epoch, last_update_time INTO present_epoch, last_updated FROM
            _prom_catalog.ids_epoch LIMIT 1;
    COMMIT;


    -- transaction 4
        lastT := clock_timestamp();
        PERFORM _prom_catalog.set_app_name( format('promscale maintenance: data retention: metric %s: delete expired series', metric_name));
        PERFORM _prom_catalog.delete_expired_series(metric_schema, metric_table, metric_series_table, ran_at, present_epoch, last_updated);
        IF log_verbose THEN
            RAISE LOG 'promscale maintenance: data retention: metric %: done deleting expired series in %', metric_name, clock_timestamp()-lastT;
            RAISE LOG 'promscale maintenance: data retention: metric %: finished in %', metric_name, clock_timestamp()-startT;
        END IF;
    RETURN;
END
$func$
LANGUAGE PLPGSQL;
GRANT EXECUTE ON PROCEDURE _prom_catalog.drop_metric_chunks(text, text, timestamptz, timestamptz, boolean) TO prom_maintenance;

CREATE OR REPLACE PROCEDURE _ps_trace.drop_span_chunks(_older_than timestamptz)
AS $func$
BEGIN
    IF _prom_catalog.is_timescaledb_installed() THEN
        IF _prom_catalog.get_timescale_major_version() >= 2 THEN
            PERFORM public.drop_chunks(
                relation=>'_ps_trace.span',
                older_than=>_older_than
            );
        ELSE
            PERFORM public.drop_chunks(
                table_name=>'span',
                schema_name=> '_ps_trace',
                older_than=>_older_than,
                cascade_to_materializations=>FALSE
            );
        END IF;
    ELSE
        DELETE FROM _ps_trace.span WHERE start_time < _older_than;
    END IF;
END
$func$
LANGUAGE PLPGSQL
--security definer to add jobs as the logged-in user
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON PROCEDURE _ps_trace.drop_span_chunks(timestamptz) FROM PUBLIC;
GRANT EXECUTE ON PROCEDURE _ps_trace.drop_span_chunks(timestamptz) TO prom_maintenance;

CREATE OR REPLACE PROCEDURE _ps_trace.drop_link_chunks(_older_than timestamptz)
AS $func$
BEGIN
    IF _prom_catalog.is_timescaledb_installed() THEN
        IF _prom_catalog.get_timescale_major_version() >= 2 THEN
            PERFORM public.drop_chunks(
                relation=>'_ps_trace.link',
                older_than=>_older_than
            );
        ELSE
            PERFORM public.drop_chunks(
                table_name=>'link',
                schema_name=> '_ps_trace',
                older_than=>_older_than,
                cascade_to_materializations=>FALSE
            );
        END IF;
    ELSE
        DELETE FROM _ps_trace.link WHERE span_start_time < _older_than;
    END IF;
END
$func$
LANGUAGE PLPGSQL
--security definer to add jobs as the logged-in user
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON PROCEDURE _ps_trace.drop_link_chunks(timestamptz) FROM PUBLIC;
GRANT EXECUTE ON PROCEDURE _ps_trace.drop_link_chunks(timestamptz) TO prom_maintenance;

CREATE OR REPLACE PROCEDURE _ps_trace.drop_event_chunks(_older_than timestamptz)
AS $func$
BEGIN
    IF _prom_catalog.is_timescaledb_installed() THEN
        IF _prom_catalog.get_timescale_major_version() >= 2 THEN
            PERFORM public.drop_chunks(
                relation=>'_ps_trace.event',
                older_than=>_older_than
            );
        ELSE
            PERFORM public.drop_chunks(
                table_name=>'event',
                schema_name=> '_ps_trace',
                older_than=>_older_than,
                cascade_to_materializations=>FALSE
            );
        END IF;
    ELSE
        DELETE FROM _ps_trace.event WHERE time < _older_than;
    END IF;
END
$func$
LANGUAGE PLPGSQL
--security definer to add jobs as the logged-in user
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON PROCEDURE _ps_trace.drop_event_chunks(timestamptz) FROM PUBLIC;
GRANT EXECUTE ON PROCEDURE _ps_trace.drop_event_chunks(timestamptz) TO prom_maintenance;

CREATE OR REPLACE FUNCTION ps_trace.set_trace_retention_period(_trace_retention_period INTERVAL)
RETURNS BOOLEAN
AS $$
    INSERT INTO _prom_catalog.default(key, value) VALUES ('trace_retention_period', _trace_retention_period::text)
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
    SELECT true;
$$
LANGUAGE SQL VOLATILE;
COMMENT ON FUNCTION ps_trace.set_trace_retention_period(INTERVAL)
IS 'set the retention period for trace data';
GRANT EXECUTE ON FUNCTION ps_trace.set_trace_retention_period(INTERVAL) TO prom_admin;

CREATE OR REPLACE FUNCTION ps_trace.get_trace_retention_period()
RETURNS INTERVAL
AS $$
    SELECT value::interval
    FROM _prom_catalog.default
    WHERE key = 'trace_retention_period'
$$
LANGUAGE SQL STABLE;
COMMENT ON FUNCTION ps_trace.get_trace_retention_period()
IS 'get the retention period for trace data';
GRANT EXECUTE ON FUNCTION ps_trace.get_trace_retention_period() TO prom_reader;

CREATE OR REPLACE PROCEDURE _ps_trace.execute_data_retention_policy(log_verbose boolean)
AS $$
DECLARE
    _trace_retention_period interval;
    _older_than timestamptz;
    _last timestamptz;
    _start timestamptz;
    _message_text text;
    _pg_exception_detail text;
    _pg_exception_hint text;
BEGIN
    _start := clock_timestamp();

    PERFORM _prom_catalog.set_app_name('promscale maintenance: data retention: tracing');
    IF log_verbose THEN
        RAISE LOG 'promscale maintenance: data retention: tracing: starting';
    END IF;

    _trace_retention_period = ps_trace.get_trace_retention_period();
    IF _trace_retention_period is null THEN
        RAISE EXCEPTION 'promscale maintenance: data retention: tracing: trace_retention_period is null.';
    END IF;

    _older_than = now() - _trace_retention_period;
    IF _older_than >= now() THEN -- bail early. no need to continue
        RAISE WARNING 'promscale maintenance: data retention: tracing: aborting. trace_retention_period set to zero or negative interval';
        IF log_verbose THEN
            RAISE LOG 'promscale maintenance: data retention: tracing: finished in %', clock_timestamp()-_start;
        END IF;
        RETURN;
    END IF;

    IF log_verbose THEN
        RAISE LOG 'promscale maintenance: data retention: tracing: dropping trace chunks older than %s', _older_than;
    END IF;

    _last := clock_timestamp();
    PERFORM _prom_catalog.set_app_name('promscale maintenance: data retention: tracing: deleting link data');
    BEGIN
        CALL _ps_trace.drop_link_chunks(_older_than);
        IF log_verbose THEN
            RAISE LOG 'promscale maintenance: data retention: tracing: done deleting link data in %', clock_timestamp()-_last;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            _message_text = MESSAGE_TEXT,
            _pg_exception_detail = PG_EXCEPTION_DETAIL,
            _pg_exception_hint = PG_EXCEPTION_HINT;
        RAISE WARNING 'promscale maintenance: data retention: tracing: failed to delete link data. % % % %',
            _message_text, _pg_exception_detail, _pg_exception_hint, clock_timestamp()-_last;
    END;
    COMMIT;

    _last := clock_timestamp();
    PERFORM _prom_catalog.set_app_name('promscale maintenance: data retention: tracing: deleting event data');
    BEGIN
        CALL _ps_trace.drop_event_chunks(_older_than);
        IF log_verbose THEN
            RAISE LOG 'promscale maintenance: data retention: tracing: done deleting event data in %', clock_timestamp()-_last;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            _message_text = MESSAGE_TEXT,
            _pg_exception_detail = PG_EXCEPTION_DETAIL,
            _pg_exception_hint = PG_EXCEPTION_HINT;
        RAISE WARNING 'promscale maintenance: data retention: tracing: failed to delete event data. % % % %',
            _message_text, _pg_exception_detail, _pg_exception_hint, clock_timestamp()-_last;
    END;
    COMMIT;

    _last := clock_timestamp();
    PERFORM _prom_catalog.set_app_name('promscale maintenance: data retention: tracing: deleting span data');
    BEGIN
        CALL _ps_trace.drop_span_chunks(_older_than);
        IF log_verbose THEN
            RAISE LOG 'promscale maintenance: data retention: tracing: done deleting span data in %', clock_timestamp()-_last;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            _message_text = MESSAGE_TEXT,
            _pg_exception_detail = PG_EXCEPTION_DETAIL,
            _pg_exception_hint = PG_EXCEPTION_HINT;
        RAISE WARNING 'promscale maintenance: data retention: tracing: failed to delete span data. % % % %',
            _message_text, _pg_exception_detail, _pg_exception_hint, clock_timestamp()-_last;
    END;
    COMMIT;

    IF log_verbose THEN
        RAISE LOG 'promscale maintenance: data retention: tracing: finished in %', clock_timestamp()-_start;
    END IF;
END;
$$ LANGUAGE PLPGSQL;
COMMENT ON PROCEDURE _ps_trace.execute_data_retention_policy(boolean)
IS 'drops old data according to the data retention policy. This procedure should be run regularly in a cron job';
GRANT EXECUTE ON PROCEDURE _ps_trace.execute_data_retention_policy(boolean) TO prom_maintenance;

CREATE OR REPLACE PROCEDURE _prom_catalog.execute_data_retention_policy(log_verbose boolean)
AS $$
DECLARE
    r _prom_catalog.metric;
    remaining_metrics _prom_catalog.metric[] DEFAULT '{}';
BEGIN
    --Do one loop with metric that could be locked without waiting.
    --This allows you to do everything you can while avoiding lock contention.
    --Then come back for the metrics that would have needed to wait on the lock.
    --Hopefully, that lock is now freed. The second loop waits for the lock
    --to prevent starvation.
    FOR r IN
        SELECT *
        FROM _prom_catalog.get_metrics_that_need_drop_chunk()
    LOOP
        IF NOT _prom_catalog.lock_metric_for_maintenance(r.id, wait=>false) THEN
            remaining_metrics := remaining_metrics || r;
            CONTINUE;
        END IF;
        CALL _prom_catalog.drop_metric_chunks(r.table_schema, r.metric_name, NOW() - _prom_catalog.get_metric_retention_period(r.table_schema, r.metric_name), log_verbose=>log_verbose);
        PERFORM _prom_catalog.unlock_metric_for_maintenance(r.id);

        COMMIT;
    END LOOP;

    IF log_verbose AND array_length(remaining_metrics, 1) > 0 THEN
        RAISE LOG 'promscale maintenance: data retention: need to wait to grab locks on % metrics', array_length(remaining_metrics, 1);
    END IF;

    FOR r IN
        SELECT *
        FROM unnest(remaining_metrics)
    LOOP
        PERFORM _prom_catalog.set_app_name( format('promscale maintenance: data retention: metric %s: wait for lock', r.metric_name));
        PERFORM _prom_catalog.lock_metric_for_maintenance(r.id);
        CALL _prom_catalog.drop_metric_chunks(r.table_schema, r.metric_name, NOW() - _prom_catalog.get_metric_retention_period(r.table_schema, r.metric_name), log_verbose=>log_verbose);
        PERFORM _prom_catalog.unlock_metric_for_maintenance(r.id);

        COMMIT;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;
COMMENT ON PROCEDURE _prom_catalog.execute_data_retention_policy(boolean)
IS 'drops old data according to the data retention policy. This procedure should be run regularly in a cron job';
GRANT EXECUTE ON PROCEDURE _prom_catalog.execute_data_retention_policy(boolean) TO prom_maintenance;

--public procedure to be called by cron
--right now just does data retention but name is generic so that
--we can add stuff later without needing people to change their cron scripts
--should be the last thing run in a session so that all session locks
--are guaranteed released on error.
CREATE OR REPLACE PROCEDURE prom_api.execute_maintenance(log_verbose boolean = false)
AS $$
DECLARE
   startT TIMESTAMPTZ;
BEGIN
    startT := clock_timestamp();
    IF log_verbose THEN
        RAISE LOG 'promscale maintenance: data retention: starting';
    END IF;

    PERFORM _prom_catalog.set_app_name( format('promscale maintenance: data retention'));
    CALL _prom_catalog.execute_data_retention_policy(log_verbose=>log_verbose);
    CALL _ps_trace.execute_data_retention_policy(log_verbose=>log_verbose);

    IF NOT _prom_catalog.is_timescaledb_oss() AND _prom_catalog.get_timescale_major_version() >= 2 THEN
        IF log_verbose THEN
            RAISE LOG 'promscale maintenance: compression: starting';
        END IF;

        PERFORM _prom_catalog.set_app_name( format('promscale maintenance: compression'));
        CALL _prom_catalog.execute_compression_policy(log_verbose=>log_verbose);
    END IF;

    IF log_verbose THEN
        RAISE LOG 'promscale maintenance: finished in %', clock_timestamp()-startT;
    END IF;

    IF clock_timestamp()-startT > INTERVAL '12 hours' THEN
        RAISE WARNING 'promscale maintenance jobs are taking too long (one run took %)', clock_timestamp()-startT
              USING HINT = 'Please consider increasing the number of maintenance jobs using config_maintenance_jobs()';
    END IF;
END;
$$ LANGUAGE PLPGSQL;
COMMENT ON PROCEDURE prom_api.execute_maintenance(boolean)
IS 'Execute maintenance tasks like dropping data according to retention policy. This procedure should be run regularly in a cron job';
GRANT EXECUTE ON PROCEDURE prom_api.execute_maintenance(boolean) TO prom_maintenance;

CREATE OR REPLACE PROCEDURE _prom_catalog.execute_maintenance_job(job_id int, config jsonb)
AS $$
DECLARE
   log_verbose boolean;
   ae_key text;
   ae_value text;
   ae_load boolean := FALSE;
BEGIN
    log_verbose := coalesce(config->>'log_verbose', 'false')::boolean;

    --if auto_explain enabled in config, turn it on in a best-effort way
    --i.e. if it fails (most likely due to lack of superuser priviliges) move on anyway.
    BEGIN
        FOR ae_key, ae_value IN
           SELECT * FROM jsonb_each_text(config->'auto_explain')
        LOOP
            IF NOT ae_load THEN
                ae_load := true;
                LOAD 'auto_explain';
            END IF;

            PERFORM set_config('auto_explain.'|| ae_key, ae_value, FALSE);
        END LOOP;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'could not set auto_explain options';
    END;


    CALL prom_api.execute_maintenance(log_verbose=>log_verbose);
END
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION prom_api.config_maintenance_jobs(number_jobs int, new_schedule_interval interval, new_config jsonb = NULL)
RETURNS BOOLEAN
AS $func$
DECLARE
  cnt int;
  log_verbose boolean;
BEGIN
    --check format of config
    log_verbose := coalesce(new_config->>'log_verbose', 'false')::boolean;

    PERFORM public.delete_job(job_id)
    FROM timescaledb_information.jobs
    WHERE proc_schema = '_prom_catalog' AND proc_name = 'execute_maintenance_job' AND (schedule_interval != new_schedule_interval OR new_config IS DISTINCT FROM config) ;


    SELECT count(*) INTO cnt
    FROM timescaledb_information.jobs
    WHERE proc_schema = '_prom_catalog' AND proc_name = 'execute_maintenance_job';

    IF cnt < number_jobs THEN
        PERFORM public.add_job('_prom_catalog.execute_maintenance_job', new_schedule_interval, config=>new_config)
        FROM generate_series(1, number_jobs-cnt);
    END IF;

    IF cnt > number_jobs THEN
        PERFORM public.delete_job(job_id)
        FROM timescaledb_information.jobs
        WHERE proc_schema = '_prom_catalog' AND proc_name = 'execute_maintenance_job'
        LIMIT (cnt-number_jobs);
    END IF;

    RETURN TRUE;
END
$func$
LANGUAGE PLPGSQL VOLATILE
--security definer to add jobs as the logged-in user
SECURITY DEFINER
--search path must be set for security definer
SET search_path = pg_temp;
--redundant given schema settings but extra caution for security definers
REVOKE ALL ON FUNCTION prom_api.config_maintenance_jobs(int, interval, jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION prom_api.config_maintenance_jobs(int, interval, jsonb) TO prom_admin;
COMMENT ON FUNCTION prom_api.config_maintenance_jobs(int, interval, jsonb)
IS 'Configure the number of maintence jobs run by the job scheduler, as well as their scheduled interval';
