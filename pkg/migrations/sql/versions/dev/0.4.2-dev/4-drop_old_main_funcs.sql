DROP PROCEDURE IF EXISTS _prom_catalog.drop_metric_chunks(TEXT, TIMESTAMPTZ, TIMESTAMPTZ);
DROP PROCEDURE IF EXISTS _prom_catalog.execute_data_retention_policy();
DROP PROCEDURE IF EXISTS prom_api.execute_maintenance();
DROP FUNCTION IF EXISTS prom_api.config_maintenance_jobs(int, interval);
DROP PROCEDURE IF EXISTS _prom_catalog.execute_compression_policy();