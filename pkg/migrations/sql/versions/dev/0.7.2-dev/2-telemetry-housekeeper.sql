-- See the purpose in migrations/sql/preinstall/012-telemetry.sql
INSERT INTO SCHEMA_PS_CATALOG.promscale_instance_information (uuid, last_updated, is_counter_reset_row)
    VALUES ('00000000-0000-0000-0000-000000000000', current_timestamp, TRUE);