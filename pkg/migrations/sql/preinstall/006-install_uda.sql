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

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.is_timescaledb_installed()
RETURNS BOOLEAN
AS $func$
    SELECT count(*) > 0 FROM pg_extension WHERE extname='timescaledb';
$func$
LANGUAGE SQL STABLE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.is_timescaledb_installed() TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.is_timescaledb_oss()
RETURNS BOOLEAN AS
$$
BEGIN
    IF SCHEMA_CATALOG.is_timescaledb_installed() THEN
        IF SCHEMA_CATALOG.get_timescale_major_version() >= 2 THEN
            -- TimescaleDB 2.x
            RETURN (SELECT current_setting('timescaledb.license') = 'apache');
        ELSE
            -- TimescaleDB 1.x
            -- Note: We cannot use current_setting() in 1.x, otherwise we get permission errors as
            -- we need to be superuser. We should not enforce the use of superuser. Hence, we take
            -- help of a view.
            RETURN (SELECT edition = 'apache' FROM timescaledb_information.license);
        END IF;
    END IF;
RETURN false;
END;
$$
LANGUAGE plpgsql;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.is_timescaledb_oss() TO prom_reader;

--add 2 jobs executing every 30 min by default for timescaledb 2.0
DO $$
BEGIN
    IF NOT SCHEMA_CATALOG.is_timescaledb_oss() AND SCHEMA_CATALOG.get_timescale_major_version() >= 2 THEN
       PERFORM SCHEMA_TIMESCALE.add_job('SCHEMA_CATALOG.execute_maintenance_job', '30 min');
       PERFORM SCHEMA_TIMESCALE.add_job('SCHEMA_CATALOG.execute_maintenance_job', '30 min');
    END IF;
END
$$;
