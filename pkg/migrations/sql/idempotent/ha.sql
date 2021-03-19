-- function that trigger to automatically keep the log calls
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.ha_leases_audit_fn()
    RETURNS TRIGGER
AS
$func$
BEGIN
    -- update happened, leader didn't change, just lease bounds -> do nothing
    IF OLD IS NOT NULL AND OLD.leader_name = NEW.leader_name THEN
        RETURN NEW;
    END IF;

    -- leader changed, set lease until to existing log line
    IF OLD IS NOT NULL AND OLD.leader_name <> NEW.leader_name THEN
        UPDATE ha_leases_logs
        SET lease_until = OLD.lease_until
        WHERE cluster_name = OLD.cluster_name
          AND leader_name = OLD.leader_name
          AND lease_start = OLD.lease_start
          AND lease_until IS NULL;
    END IF;

    -- insert happened or leader changed and new leader needs to be logged
    INSERT INTO ha_leases_logs (cluster_name, leader_name, lease_start, lease_until)
    VALUES (NEW.cluster_name, NEW.leader_name, NEW.lease_start, null);

    RETURN NEW;
END;
$func$ LANGUAGE plpgsql VOLATILE;

-- ha api functions
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.update_lease(cluster TEXT, writer TEXT, min_time TIMESTAMPTZ,
                                                       max_time TIMESTAMPTZ) RETURNS ha_leases
AS
$func$
DECLARE
    leader            TEXT;
    lease_start       TIMESTAMPTZ;
    lease_until       TIMESTAMPTZ;
    new_lease_timeout TIMESTAMPTZ;
    lease_state       ha_leases%ROWTYPE;
    lease_timeout INTERVAL;
    lease_refresh INTERVAL;
BEGIN

    -- find lease_timeout setting;
    SELECT value::INTERVAL
    INTO lease_timeout
    FROM SCHEMA_CATALOG.default
    WHERE key = 'ha_lease_timeout';

    -- find latest leader and their lease time range;
    SELECT h.leader_name, h.lease_start, h.lease_until
    INTO leader, lease_start, lease_until
    FROM SCHEMA_CATALOG.ha_leases as h
    WHERE cluster_name = cluster;

    --only happens on very first call;
    IF NOT FOUND THEN
        -- no leader yet for cluster insert;
        INSERT INTO SCHEMA_CATALOG.ha_leases
        VALUES (cluster, writer, min_time, max_time + lease_timeout)
        ON CONFLICT DO NOTHING;
        -- needed due to on-conflict clause;
        SELECT h.leader_name, h.lease_start, h.lease_until
        INTO leader, lease_start, lease_until
        FROM SCHEMA_CATALOG.ha_leases as h
        WHERE cluster_name = cluster;
    END IF;

    IF leader <> writer THEN
        RAISE EXCEPTION 'LEADER_HAS_CHANGED' USING ERRCODE = 'PS010';
    END IF;

    -- find lease_refresh setting;
    SELECT value::INTERVAL
    INTO lease_refresh
    FROM SCHEMA_CATALOG.default
    WHERE key = 'ha_lease_refresh';

    new_lease_timeout = max_time + lease_timeout;
    IF new_lease_timeout > lease_until + lease_refresh THEN
        UPDATE SCHEMA_CATALOG.ha_leases h
        SET lease_until = new_lease_timeout
        WHERE h.cluster_name = cluster
          AND h.leader_name = writer
          AND h.lease_until + lease_refresh < new_lease_timeout;
        IF NOT FOUND THEN -- concurrent update
            SELECT h.leader_name, h.lease_start, h.lease_until
            INTO leader, lease_start, lease_until
            FROM SCHEMA_CATALOG.ha_leases as h
            WHERE cluster_name = cluster;
            IF leader <> writer OR lease_until < max_time
            THEN
                RAISE EXCEPTION 'LEADER_HAS_CHANGED' USING ERRCODE = 'PS010';
            END IF;
        END IF;
    END IF;
    SELECT * INTO STRICT lease_state FROM ha_leases WHERE cluster_name = cluster;
    RETURN lease_state;
END;
$func$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.try_change_leader(cluster TEXT, new_leader TEXT,
                                                            max_time TIMESTAMPTZ) RETURNS ha_leases
AS
$func$
DECLARE
    lease_timeout INTERVAL;
    lease_state ha_leases%ROWTYPE;
BEGIN
    -- find lease_timeout setting;
    SELECT value::INTERVAL
    INTO lease_timeout
    FROM SCHEMA_CATALOG.default
    WHERE key = 'ha_lease_timeout';

    UPDATE SCHEMA_CATALOG.ha_leases
    SET leader_name = new_leader,
        lease_start = lease_until,
        lease_until = max_time + lease_timeout
    WHERE cluster_name = cluster
      AND lease_until < max_time;

    SELECT *
    INTO STRICT lease_state
    FROM ha_leases
    WHERE cluster_name = cluster;
    RETURN lease_state;

END;
$func$ LANGUAGE plpgsql VOLATILE;
