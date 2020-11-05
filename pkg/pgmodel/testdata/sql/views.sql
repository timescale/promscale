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

SELECT * FROM prom_info.label ORDER BY key;

\set ON_ERROR_STOP 0
SELECT count(compress_chunk(i)) from show_chunks('prom_data.cpu_usage') i;
\set ON_ERROR_STOP 0

SELECT * FROM cpu_usage ORDER BY time, series_id LIMIT 5;
SELECT time, value, jsonb(labels), val(namespace_id) FROM cpu_usage ORDER BY time, series_id LIMIT 5;
SELECT * FROM prom_series.cpu_usage ORDER BY series_id;
