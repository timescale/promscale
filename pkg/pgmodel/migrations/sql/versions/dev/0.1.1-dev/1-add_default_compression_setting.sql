ALTER TABLE SCHEMA_CATALOG.metric
ADD COLUMN default_compression BOOLEAN NOT NULL DEFAULT true;

INSERT INTO SCHEMA_CATALOG.default(key,value) VALUES
('metric_compression', (exists(select * from pg_proc where proname = 'compress_chunk')::text));
