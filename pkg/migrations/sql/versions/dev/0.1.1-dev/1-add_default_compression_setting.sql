ALTER TABLE _prom_catalog.metric
ADD COLUMN default_compression BOOLEAN NOT NULL DEFAULT true;

INSERT INTO _prom_catalog.default(key,value) VALUES
('metric_compression', (exists(select * from pg_proc where proname = 'compress_chunk')::text));
