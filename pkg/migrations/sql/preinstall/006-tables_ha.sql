CREATE TABLE SCHEMA_CATALOG.ha_leases
(
    cluster_name TEXT PRIMARY KEY,
    leader_name  TEXT,
    lease_start  TIMESTAMPTZ,
    lease_until  TIMESTAMPTZ
);

CREATE TABLE SCHEMA_CATALOG.ha_leases_logs
(
    cluster_name TEXT        NOT NULL,
    leader_name  TEXT        NOT NULL,
    lease_start  TIMESTAMPTZ NOT NULL, -- inclusive
    lease_until  TIMESTAMPTZ,          -- exclusive
    PRIMARY KEY (cluster_name, leader_name, lease_start)
);


-- STUB for function that trigger to automatically keep the log calls - real implementation in ha.sql
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.ha_leases_audit_fn()
    RETURNS TRIGGER
AS
$func$
BEGIN
    RAISE 'Just a stub, should be overwritten';
    RETURN NEW;
END;
$func$ LANGUAGE plpgsql VOLATILE;

-- trigger to automatically keep the log
CREATE TRIGGER ha_leases_audit
    AFTER INSERT OR UPDATE
    ON SCHEMA_CATALOG.ha_leases
    FOR EACH ROW
EXECUTE PROCEDURE SCHEMA_CATALOG.ha_leases_audit_fn();

-- default values for lease
INSERT INTO SCHEMA_CATALOG.default(key, value)
VALUES ('ha_lease_timeout', '1m'),
       ('ha_lease_refresh', '10s')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

GRANT USAGE ON ALL SEQUENCES IN SCHEMA SCHEMA_CATALOG TO prom_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA SCHEMA_CATALOG TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SCHEMA_CATALOG TO prom_writer