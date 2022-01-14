-----------------------
-- Table definitions --
-----------------------

-- a special type we use in our tables so must be created here
CREATE DOMAIN SCHEMA_PROM.label_array AS int[] NOT NULL;

-- special type to store only values of labels
CREATE DOMAIN SCHEMA_PROM.label_value_array AS TEXT[];

CREATE TABLE public.prom_installation_info (
    key TEXT PRIMARY KEY,
    value TEXT
);
GRANT SELECT ON TABLE public.prom_installation_info TO PUBLIC;
--all modifications can only be done by owner

INSERT INTO public.prom_installation_info(key, value) VALUES
    ('tagging schema',          'SCHEMA_TAG'),
    ('catalog schema',          'SCHEMA_CATALOG'),
    ('prometheus API schema',   'SCHEMA_PROM'),
    ('extension schema',        'SCHEMA_EXT'),
    ('series schema',           'SCHEMA_SERIES'),
    ('metric schema',           'SCHEMA_METRIC'),
    ('data schema',             'SCHEMA_DATA'),
    ('exemplar data schema',    'SCHEMA_DATA_EXEMPLAR'),
    ('information schema',      'SCHEMA_INFO'),
    ('tracing schema',          'SCHEMA_TRACING_PUBLIC'),
    ('tracing schema private',  'SCHEMA_TRACING');


CREATE TABLE SCHEMA_CATALOG.series (
    id bigint NOT NULL,
    metric_id int NOT NULL,
    labels SCHEMA_PROM.label_array NOT NULL, --labels are globally unique because of how partitions are defined
    delete_epoch bigint NULL DEFAULT NULL -- epoch after which this row can be deleted
) PARTITION BY LIST(metric_id);
GRANT SELECT ON TABLE SCHEMA_CATALOG.series TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_CATALOG.series TO prom_writer;


CREATE INDEX series_labels_id ON SCHEMA_CATALOG.series USING GIN (labels);
CREATE INDEX series_deleted
    ON SCHEMA_CATALOG.series(delete_epoch, id)
    WHERE delete_epoch IS NOT NULL;

CREATE SEQUENCE SCHEMA_CATALOG.series_id;
GRANT USAGE ON SEQUENCE SCHEMA_CATALOG.series_id TO prom_writer;


CREATE TABLE SCHEMA_CATALOG.label (
    id serial CHECK (id > 0),
    key TEXT,
    value text,
    PRIMARY KEY (id) INCLUDE (key, value),
    UNIQUE (key, value) INCLUDE (id)
);
GRANT SELECT ON TABLE SCHEMA_CATALOG.label TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_CATALOG.label TO prom_writer;
GRANT USAGE ON SEQUENCE SCHEMA_CATALOG.label_id_seq TO prom_writer;

CREATE TABLE SCHEMA_CATALOG.ids_epoch(
    current_epoch BIGINT NOT NULL,
    last_update_time TIMESTAMPTZ NOT NULL,
    -- force there to only be a single row
    is_unique BOOLEAN NOT NULL DEFAULT true CHECK (is_unique = true),
    UNIQUE (is_unique)
);
GRANT SELECT ON TABLE SCHEMA_CATALOG.ids_epoch TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_CATALOG.ids_epoch TO prom_writer;

-- uses an arbitrary start time so pristine and migrated DBs have the same values
INSERT INTO SCHEMA_CATALOG.ids_epoch VALUES (0, '1970-01-01 00:00:00 UTC', true);

--This table creates a unique mapping
--between label keys and their column names across metrics.
--This is done for usability of column name, especially for
-- long keys that get cut off.
CREATE TABLE SCHEMA_CATALOG.label_key(
    id SERIAL,
    key TEXT,
    value_column_name NAME,
    id_column_name NAME,
    PRIMARY KEY (id),
    UNIQUE(key)
);
GRANT SELECT ON TABLE SCHEMA_CATALOG.label_key TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_CATALOG.label_key TO prom_writer;
GRANT USAGE ON SEQUENCE SCHEMA_CATALOG.label_key_id_seq TO prom_writer;

CREATE TABLE SCHEMA_CATALOG.label_key_position (
    metric_name text, --references metric.metric_name NOT metric.id for performance reasons
    key TEXT, --NOT label_key.id for performance reasons.
    pos int,
    UNIQUE (metric_name, key) INCLUDE (pos)
);
GRANT SELECT ON TABLE SCHEMA_CATALOG.label_key_position TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_CATALOG.label_key_position TO prom_writer;

CREATE TABLE SCHEMA_CATALOG.metric (
    id SERIAL PRIMARY KEY,
    metric_name text NOT NULL,
    table_name name NOT NULL,
    creation_completed BOOLEAN NOT NULL DEFAULT false,
    default_chunk_interval BOOLEAN NOT NULL DEFAULT true,
    retention_period INTERVAL DEFAULT NULL, --NULL to use the default retention_period
    default_compression BOOLEAN NOT NULL DEFAULT true,
    delay_compression_until TIMESTAMPTZ DEFAULT NULL,
    table_schema name NOT NULL DEFAULT 'SCHEMA_DATA',
    series_table name NOT NULL, -- series_table specifies the name of table where the series data is stored.
    is_view BOOLEAN NOT NULL DEFAULT false,
    UNIQUE (metric_name, table_schema) INCLUDE (table_name),
    UNIQUE(table_schema, table_name)
);
GRANT SELECT ON TABLE SCHEMA_CATALOG.metric TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_CATALOG.metric TO prom_writer;
GRANT USAGE ON SEQUENCE SCHEMA_CATALOG.metric_id_seq TO prom_writer;

CREATE TABLE SCHEMA_CATALOG.default (
    key TEXT PRIMARY KEY,
    value TEXT
);
GRANT SELECT ON TABLE SCHEMA_CATALOG.default TO prom_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SCHEMA_CATALOG.default TO prom_admin;

INSERT INTO SCHEMA_CATALOG.default(key,value) VALUES
('chunk_interval', (INTERVAL '8 hours')::text),
('retention_period', (90 * INTERVAL '1 day')::text),
('metric_compression', (exists(select * from pg_proc where proname = 'compress_chunk')::text)),
('trace_retention_period', (30 * INTERVAL '1 days')::text);
