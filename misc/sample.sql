

set search_path to _ps_trace;

INSERT INTO _ps_trace.schema_url (url)
SELECT format('lib%s.instrumentation.example', generate_series(1, 20))
ON CONFLICT (url) DO NOTHING;

INSERT INTO _ps_trace.inst_lib (name, version, schema_url_id)
SELECT
    format('lib%s', x),
    '1.2.3',
    (SELECT id FROM _ps_trace.schema_url WHERE url = format('lib%s.instrumentation.example', x))
FROM generate_series(1, 20) x
ON CONFLICT (name, version, schema_url_id) DO NOTHING
;

INSERT INTO _ps_trace.schema_url (url)
SELECT format('service%s.resource.example', generate_series(1, 20))
ON CONFLICT (url) DO NOTHING;

INSERT INTO _ps_trace.span_name (name)
SELECT format('span%s', generate_series(1, 100))
ON CONFLICT (name) DO NOTHING;

INSERT INTO _ps_trace.tag_key (tag_type, key)
SELECT
    _ps_trace.span_tag_type() | _ps_trace.resource_tag_type(),
    format('%s%s', t, k)
FROM generate_series(1, 50) k
CROSS JOIN unnest(array['int', 'text', 'date', 'bool']) t
ON CONFLICT (key) DO NOTHING;

INSERT INTO _ps_trace.tag (tag_type, key_id, key, value)
SELECT
    k.tag_type,
    k.id,
    k.key,
    v.value
FROM _ps_trace.tag_key k
INNER JOIN
(
    SELECT 'int' as typ, to_jsonb(v) as value FROM generate_series(1, 100) v
    UNION ALL
    SELECT 'bool' as typ, to_jsonb(v) as value FROM unnest(array[true, false]) v
    UNION ALL
    SELECT 'date' as typ, to_jsonb(v) as value FROM generate_series('2021-01-01'::date, '2021-12-31'::date, interval '1 day') v
    UNION ALL
    SELECT 'text' as typ, to_jsonb(repeat(chr(c), n)) as value FROM generate_series(65, 90) c CROSS JOIN generate_series(1, 5) n
) v ON (starts_with(k.key, v.typ))
ON CONFLICT (key, value) DO NOTHING
;

CREATE EXTENSION IF NOT EXISTS pgcrypto;
DO $block$
DECLARE
    _i bigint;
    _status_code _ps_trace.status_code;
BEGIN
    FOR _i IN 1..10000
    LOOP
        SELECT * INTO _status_code FROM unnest(enum_range(null::_ps_trace.status_code, null)) ORDER BY random() LIMIT 1;
        INSERT INTO _ps_trace.span
        (
            trace_id,
            span_id,
            trace_state,
            parent_span_id,
            name_id,
            span_kind,
            start_time,
            end_time,
            span_tags,
            dropped_tags_count,
            event_time,
            dropped_events_count,
            dropped_link_count,
            status_code,
            status_message,
            inst_lib_id,
            resource_tags,
            resource_dropped_tags_count,
            resource_schema_url_id
        )
        SELECT
            gen_random_uuid(),
            (random() * 9223372036854775807)::bigint,
            'OK',
            null, -- parent_span_id
            (SELECT id FROM span_name ORDER BY random() LIMIT 1),
            (SELECT * FROM unnest(enum_range(null::_ps_trace.span_kind, null)) ORDER BY random() LIMIT 1),
            now(),
            now() + (interval '1 second' * (random() * 5)),
            (
                SELECT jsonb_object_agg(k.id, v.id)
                FROM
                (
                    SELECT *
                    FROM _ps_trace.tag_key
                    ORDER BY random()
                    LIMIT ceil(random() * 10)
                ) k
                CROSS JOIN LATERAL
                (
                    SELECT *
                    FROM _ps_trace.tag t
                    WHERE t.key = k.key
                    ORDER BY random()
                    LIMIT 1
                ) v
            ),
            ceil(random() * 5),
            tstzrange(now(), now() + (interval '1 second' * (random() * 5)), '[)'),
            ceil(random() * 5),
            ceil(random() * 5),
            _status_code,
            _status_code::text,
            (SELECT id FROM inst_lib ORDER BY random() LIMIT 1),
            (
                SELECT jsonb_object_agg(k.id, v.id)
                FROM
                (
                    SELECT *
                    FROM _ps_trace.tag_key
                    ORDER BY random()
                    LIMIT ceil(random() * 10)
                ) k
                CROSS JOIN LATERAL
                (
                    SELECT *
                    FROM _ps_trace.tag t
                    WHERE t.key = k.key
                    ORDER BY random()
                    LIMIT 1
                ) v
            ),
            ceil(random() * 5),
            (SELECT id FROM schema_url WHERE starts_with(url, 'service') ORDER BY random() LIMIT 1)
        ;
        IF _i % 500 = 0 THEN
            COMMIT;
        END IF;
    END LOOP;
END;
$block$;