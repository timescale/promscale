CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.get_timescale_major_version()
    RETURNS INT
AS $func$
    SELECT split_part(extversion, '.', 1)::INT FROM pg_catalog.pg_extension WHERE extname='timescaledb' LIMIT 1;
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

--just a stub will be replaced in the idempotent scripts
CREATE OR REPLACE PROCEDURE SCHEMA_CATALOG.execute_maintenance_job(job_id int, config jsonb)
AS $$
BEGIN
    RAISE 'calling execute_maintenance_job stub, should have been replaced';
END
$$ LANGUAGE PLPGSQL;


--add 2 jobs executing every 30 min by default for timescaledb 2.0
DO $$
BEGIN
    IF SCHEMA_CATALOG.get_timescale_major_version() >= 2 THEN
       PERFORM add_job('SCHEMA_CATALOG.execute_maintenance_job', '30 min');
       PERFORM add_job('SCHEMA_CATALOG.execute_maintenance_job', '30 min');
    END IF;
END
$$;
