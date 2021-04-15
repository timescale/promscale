\set ECHO all
\set ON_ERROR_STOP 1

SELECT _prom_catalog.get_or_create_metric_table_name('cpu_usage');
SELECT _prom_catalog.get_or_create_metric_table_name('cpu_total');
CALL _prom_catalog.finalize_metric_creation();
INSERT INTO prom_data.cpu_usage
SELECT timestamptz '2000-01-01 02:03:04'+(interval '1s' * g), 100.1 + g, _prom_catalog.get_or_create_series_id('{"__name__": "cpu_usage", "namespace":"dev", "node": "brain"}')
FROM generate_series(1,10) g;
INSERT INTO prom_data.cpu_usage
SELECT timestamptz '2000-01-01 02:03:04'+(interval '1s' * g), 100.1 + g, _prom_catalog.get_or_create_series_id('{"__name__": "cpu_usage", "namespace":"production", "node": "pinky", "new_tag":"foo"}')
FROM generate_series(1,10) g;
INSERT INTO prom_data.cpu_total
SELECT timestamptz '2000-01-01 02:03:04'+(interval '1s' * g), 100.0, _prom_catalog.get_or_create_series_id('{"__name__": "cpu_total", "namespace":"dev", "node": "brain"}')
FROM generate_series(1,10) g;
INSERT INTO prom_data.cpu_total
SELECT timestamptz '2000-01-01 02:03:04'+(interval '1s' * g), 100.0, _prom_catalog.get_or_create_series_id('{"__name__": "cpu_total", "namespace":"production", "node": "pinky", "new_tag_2":"bar"}')
FROM generate_series(1,10) g;

SELECT id , metric_name , table_name, retention_period, chunk_interval > interval '7 hour', compressed_interval > interval '7 hour', before_compression_bytes, after_compression_bytes, label_keys, total_size, total_size_bytes, compression_ratio, total_chunks, compressed_chunks FROM prom_info.metric ORDER BY id;
-- compress chunks
SELECT compress_chunk(show_chunks('prom_data.cpu_usage'));
SELECT compress_chunk(show_chunks('prom_data.cpu_total'));
-- fetch stats with compressed chunks

SET ROLE prom_reader;
SELECT id , metric_name , table_name, retention_period, chunk_interval > interval '7 hour', compressed_interval > interval '7 hour', before_compression_bytes, after_compression_bytes, label_keys, total_size, total_size_bytes, compression_ratio, total_chunks, compressed_chunks FROM prom_info.metric ORDER BY id;
SELECT * FROM prom_info.label ORDER BY key;
SELECT * FROM prom_info.metric_stats ORDER BY num_series_approx;
SELECT * FROM prom_info.system_stats;
SELECT prom_api.label_cardinality(1);
SELECT prom_api.label_cardinality(2);
SELECT prom_api.label_cardinality(1) + prom_api.label_cardinality(2);