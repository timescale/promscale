CREATE TABLE _prom_catalog.ha_leases
(
    cluster_name TEXT PRIMARY KEY,
    leader_name  TEXT,
    lease_start  TIMESTAMPTZ,
    lease_until  TIMESTAMPTZ
);
GRANT SELECT ON TABLE _prom_catalog.ha_leases TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _prom_catalog.ha_leases TO prom_writer;

CREATE TABLE _prom_catalog.ha_leases_logs
(
    cluster_name TEXT        NOT NULL,
    leader_name  TEXT        NOT NULL,
    lease_start  TIMESTAMPTZ NOT NULL, -- inclusive
    lease_until  TIMESTAMPTZ,          -- exclusive
    PRIMARY KEY (cluster_name, leader_name, lease_start)
);
GRANT SELECT ON TABLE _prom_catalog.ha_leases_logs TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE _prom_catalog.ha_leases_logs TO prom_writer;


-- STUB for function that trigger to automatically keep the log calls - real implementation in ha.sql
CREATE OR REPLACE FUNCTION _prom_catalog.ha_leases_audit_fn()
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
    ON _prom_catalog.ha_leases
    FOR EACH ROW
EXECUTE PROCEDURE _prom_catalog.ha_leases_audit_fn();

-- default values for lease
INSERT INTO _prom_catalog.default(key, value)
VALUES ('ha_lease_timeout', '1m'),
       ('ha_lease_refresh', '10s')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;