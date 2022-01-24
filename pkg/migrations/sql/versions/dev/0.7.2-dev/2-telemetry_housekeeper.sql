-- See the purpose in migrations/sql/preinstall/012-telemetry.sql
INSERT INTO _ps_catalog.promscale_instance_information (uuid, last_updated, is_counter_reset_row)
    VALUES ('00000000-0000-0000-0000-000000000000', '2021-12-09 00:00:00'::TIMESTAMPTZ, TRUE);