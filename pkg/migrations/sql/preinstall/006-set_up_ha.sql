CREATE TABLE SCHEMA_CATALOG.ha_locks
(
    cluster_name TEXT UNIQUE,
    leader       TEXT,
    lease_start  TIMESTAMPTZ,
    lease_until  TIMESTAMPTZ
);

CREATE TABLE SCHEMA_CATALOG.ha_locks_log
(
    cluster_name TEXT        NOT NULL,
    leader       TEXT        NOT NULL,
    lease_start  TIMESTAMPTZ NOT NULL,
    lease_until  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (cluster_name, lease_start),
    UNIQUE (cluster_name, leader, lease_start)
);


-- function that trigger to automatically keep the log calls
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.ha_locks_audit_fn()
    RETURNS TRIGGER
AS
$func$
BEGIN
    INSERT INTO SCHEMA_CATALOG.ha_locks_log (cluster_name, leader, lease_start, lease_until)
    VALUES (NEW.cluster_name, NEW.leader, NEW.lease_start, NEW.lease_until)
           -- if lease is extended, but leader didn't change
    ON CONFLICT (cluster_name, leader, lease_start) DO UPDATE SET lease_until=EXCLUDED.lease_until;
    --no ON CONFLICT (cluster_name, leader) clause as there should never be conflicts
    -- on the PK and if there are it's an error
    RETURN NEW;
END;
$func$ LANGUAGE plpgsql VOLATILE;

-- trigger to automatically keep the log
CREATE TRIGGER ha_locks_audit
    AFTER INSERT OR UPDATE
    ON SCHEMA_CATALOG.ha_locks
    FOR EACH ROW
EXECUTE PROCEDURE SCHEMA_CATALOG.ha_locks_audit_fn();

-- default values for lease
INSERT INTO SCHEMA_CATALOG.default(key, value)
VALUES ('ha_lease_timeout', '1m'),
       ('ha_lease_refresh', '10s')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

-- ha api functions
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.check_insert(cluster TEXT, writer TEXT, min_time TIMESTAMPTZ,
                                                       max_time TIMESTAMPTZ) RETURNS SETOF ha_locks
AS
$func$
DECLARE
    leader            TEXT;
    lease_start       TIMESTAMPTZ;
    lease_until       TIMESTAMPTZ;
    lease_timeout     INTERVAL;
    new_lease_timeout TIMESTAMPTZ;
    lease_refresh     INTERVAL;
BEGIN
    -- find lease_timeout setting;
    SELECT value::INTERVAL
    INTO lease_timeout
    FROM SCHEMA_CATALOG.default
    WHERE key = 'ha_lease_timeout';

    -- find latest leader and their lease time range;
    SELECT h.leader, h.lease_start, h.lease_until
    INTO leader, lease_start, lease_until
    FROM SCHEMA_CATALOG.ha_locks as h
    WHERE cluster_name = cluster;

    --only happens on very first call;
    IF NOT FOUND THEN
        -- no leader yet for cluster insert;
        INSERT INTO SCHEMA_CATALOG.ha_locks
        VALUES (cluster, writer, min_time, max_time + lease_timeout)
        ON CONFLICT DO NOTHING;
        -- needed due to on-conflict clause;
        SELECT h.leader, h.lease_start, h.lease_until
        INTO leader, lease_start, lease_until
        FROM SCHEMA_CATALOG.ha_locks as h
        WHERE cluster_name = cluster;
    END IF;

    IF leader <> writer OR lease_start > min_time THEN
        RAISE EXCEPTION 'LEADER_HAS_CHANGED';
    END IF;

    SELECT value::INTERVAL
    INTO lease_refresh
    FROM SCHEMA_CATALOG.default
    WHERE key = 'ha_lease_refresh';

    new_lease_timeout = max_time + lease_timeout;
    IF new_lease_timeout > lease_until + lease_refresh THEN
        UPDATE SCHEMA_CATALOG.ha_locks h
        SET lease_until = new_lease_timeout
        WHERE h.cluster_name = cluster
          AND h.leader = writer
          AND h.lease_until + lease_refresh < new_lease_timeout;
        IF NOT FOUND THEN -- concurrent update
            SELECT h.leader, h.lease_start, h.lease_until
            INTO leader, lease_start, lease_until
            FROM SCHEMA_CATALOG.ha_locks as h
            WHERE cluster_name = cluster;
            IF leader <> writer OR lease_start > min_time OR lease_until < max_time
            THEN
                RAISE EXCEPTION 'LEADER_HAS_CHANGED';
            END IF;
        END IF;
    END IF;
    RETURN QUERY SELECT cluster, leader, lease_start, lease_until;
END;
$func$ LANGUAGE plpgsql VOLATILE ;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.try_change_leader(cluster TEXT, new_leader TEXT,
                                                            max_time TIMESTAMPTZ) RETURNS SETOF ha_locks
AS
$func$
DECLARE
    lease_timeout INTERVAL;
BEGIN
    -- find lease_timeout setting;
    SELECT value::INTERVAL
    INTO lease_timeout
    FROM SCHEMA_CATALOG.default
    WHERE key = 'ha_lease_timeout';

    UPDATE SCHEMA_CATALOG.ha_locks
    SET leader      = new_leader,
        lease_start = lease_until,
        lease_until = max_time + lease_timeout
    WHERE cluster_name = cluster
      AND lease_until < max_time;

    RETURN QUERY
        SELECT *
        FROM ha_locks
        WHERE cluster_name = cluster;

END;
$func$ LANGUAGE plpgsql VOLATILE;
