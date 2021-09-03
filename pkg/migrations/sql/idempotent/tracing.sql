/*
CREATE OR REPLACE FUNCTION _ps_trace.text(_trace_id _ps_trace.trace_id)
RETURNS text
AS $function$
    SELECT replace(_trace_id::text, '-', '')
$function$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.text(_ps_trace.trace_id) TO prom_reader;
*/
CREATE OR REPLACE FUNCTION _ps_trace.span_tag_type()
RETURNS _ps_trace.tag_type
AS $sql$
    SELECT (1<<0)::smallint::_ps_trace.tag_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.span_tag_type() TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.resource_tag_type()
RETURNS _ps_trace.tag_type
AS $sql$
    SELECT (1<<1)::smallint::_ps_trace.tag_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.resource_tag_type() TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.event_tag_type()
RETURNS _ps_trace.tag_type
AS $sql$
    SELECT (1<<2)::smallint::_ps_trace.tag_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.event_tag_type() TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.link_tag_type()
RETURNS _ps_trace.tag_type
AS $sql$
    SELECT (1<<3)::smallint::_ps_trace.tag_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.link_tag_type() TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.is_span_tag_type(_tag_type _ps_trace.tag_type)
RETURNS BOOLEAN
AS $sql$
    SELECT _tag_type & _ps_trace.span_tag_type() = _ps_trace.span_tag_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.is_span_tag_type(_ps_trace.tag_type) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.is_resource_tag_type(_tag_type _ps_trace.tag_type)
RETURNS BOOLEAN
AS $sql$
    SELECT _tag_type & _ps_trace.resource_tag_type() = _ps_trace.resource_tag_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.is_resource_tag_type(_ps_trace.tag_type) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.is_event_tag_type(_tag_type _ps_trace.tag_type)
RETURNS BOOLEAN
AS $sql$
    SELECT _tag_type & _ps_trace.event_tag_type() = _ps_trace.event_tag_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.is_event_tag_type(_ps_trace.tag_type) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.is_link_tag_type(_tag_type _ps_trace.tag_type)
RETURNS BOOLEAN
AS $sql$
    SELECT _tag_type & _ps_trace.link_tag_type() = _ps_trace.link_tag_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.is_link_tag_type(_ps_trace.tag_type) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.put_tag_key(_key _ps_trace.tag_k, _tag_type _ps_trace.tag_type)
RETURNS VOID
AS $sql$
    INSERT INTO _ps_trace.tag_key AS k (key, tag_type)
    VALUES (_key, _tag_type)
    ON CONFLICT (key) DO
    UPDATE SET tag_type = k.tag_type | EXCLUDED.tag_type
    WHERE k.tag_type & EXCLUDED.tag_type = 0
$sql$
LANGUAGE SQL VOLATILE STRICT;
GRANT EXECUTE ON FUNCTION _ps_trace.put_tag_key(_ps_trace.tag_k, _ps_trace.tag_type) TO prom_writer;

CREATE OR REPLACE FUNCTION _ps_trace.put_tag(_key _ps_trace.tag_k, _value jsonb, _tag_type _ps_trace.tag_type)
RETURNS VOID
AS $sql$
    INSERT INTO _ps_trace.tag AS a (tag_type, key_id, key, value)
    SELECT _tag_type, ak.id, _key, _value
    FROM _ps_trace.tag_key ak
    WHERE ak.key = _key
    ON CONFLICT (key, value) DO
    UPDATE SET tag_type = a.tag_type | EXCLUDED.tag_type
    WHERE a.tag_type & EXCLUDED.tag_type = 0
$sql$
LANGUAGE SQL VOLATILE STRICT;
GRANT EXECUTE ON FUNCTION _ps_trace.put_tag(_ps_trace.tag_k, jsonb, _ps_trace.tag_type) TO prom_writer;

CREATE OR REPLACE FUNCTION _ps_trace.has_tag(_tag_map _ps_trace.tag_map, _key _ps_trace.tag_k)
RETURNS boolean
AS $sql$
    SELECT _tag_map ?
    (
        SELECT k.id::text
        FROM _ps_trace.tag_key k
        WHERE k.key = _key
        LIMIT 1
    )
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION _ps_trace.has_tag(_ps_trace.tag_map, _ps_trace.tag_k) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.jsonb(_attr_map _ps_trace.tag_map)
RETURNS jsonb
AS $sql$
    /*
    takes an tag_map which is a map of tag_key.id to tag.id
    and returns a jsonb object containing the key value pairs of tags
    */
    SELECT jsonb_object_agg(a.key, a.value)
    FROM jsonb_each(_attr_map) x -- key is tag_key.id, value is tag.id
    CROSS JOIN LATERAL -- cross join lateral enables partition elimination at execution time
    (
        SELECT
            a.key,
            a.value
        FROM _ps_trace.tag a
        WHERE a.id = x.value::text::bigint
    ) a
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION _ps_trace.jsonb(_ps_trace.tag_map) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.jsonb(_attr_map _ps_trace.tag_map, VARIADIC _keys _ps_trace.tag_k[])
RETURNS jsonb
AS $sql$
    /*
    takes an tag_map which is a map of tag_key.id to tag.id
    and returns a jsonb object containing the key value pairs of tags
    only the key/value pairs with keys passed as arguments are included in the output
    */
    SELECT jsonb_object_agg(a.key, a.value)
    FROM jsonb_each(_attr_map) x -- key is tag_key.id, value is tag.id
    CROSS JOIN LATERAL -- cross join lateral enables partition elimination at execution time
    (
        SELECT
            a.key,
            a.value
        FROM _ps_trace.tag a
        WHERE a.id = x.value::text::bigint
        AND a.key = ANY(_keys) -- ANY works with partition elimination
    ) a
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION _ps_trace.jsonb(_ps_trace.tag_map) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.val(_attr_map _ps_trace.tag_map, _key _ps_trace.tag_k)
RETURNS jsonb
AS $sql$
    SELECT a.value
    FROM _ps_trace.tag a
    WHERE a.key = _key
    AND _attr_map @> jsonb_build_object(a.key_id, a.id)
    LIMIT 1
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION _ps_trace.val(_ps_trace.tag_map, _ps_trace.tag_k) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.val_text(_attr_map _ps_trace.tag_map, _key _ps_trace.tag_k)
RETURNS text
AS $sql$
    SELECT a.value#>>'{}'
    FROM _ps_trace.tag a
    WHERE a.key = _key
    AND _attr_map @> jsonb_build_object(a.key_id, a.id)
    LIMIT 1
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION _ps_trace.val_text(_ps_trace.tag_map, _ps_trace.tag_k) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.get_tag_map(_attrs jsonb)
RETURNS _ps_trace.tag_map
AS $sql$
    SELECT coalesce(jsonb_object_agg(a.key_id, a.id), '{}')::_ps_trace.tag_map
    FROM jsonb_each(_attrs) x
    CROSS JOIN LATERAL (SELECT * FROM _ps_trace.tag a where x.key = a.key AND x.value = a.value) a
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION _ps_trace.get_tag_map(jsonb) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.tag_maps(_key _ps_trace.tag_k, _qry jsonpath, _vars jsonb DEFAULT '{}'::jsonb, _silent boolean DEFAULT false)
RETURNS _ps_trace.tag_maps
AS $sql$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), '{}')::_ps_trace.tag_maps
    FROM _ps_trace.tag a
    WHERE a.key = _key
    AND jsonb_path_exists(a.value, _qry, _vars, _silent)
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION _ps_trace.tag_maps(_ps_trace.tag_k, jsonpath, jsonb, boolean) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.tag_maps_query(_key _ps_trace.tag_k, _path jsonpath)
RETURNS _ps_trace.tag_maps
AS $sql$
    SELECT _ps_trace.tag_maps(_key, _path);
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION _ps_trace.tag_maps_query(_ps_trace.tag_k, jsonpath) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.tag_maps_regex(_key _ps_trace.tag_k, _pattern text)
RETURNS _ps_trace.tag_maps
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), '{}')::_ps_trace.tag_maps
    FROM _ps_trace.tag a
    WHERE a.key = _key AND
    -- if the jsonb value is a string, apply the regex directly
    -- otherwise, convert the value to a text representation, back to a jsonb string, and then apply
    CASE jsonb_typeof(a.value)
        WHEN 'string' THEN jsonb_path_exists(a.value, format('$?(@ like_regex "%s")', _pattern)::jsonpath)
        ELSE jsonb_path_exists(to_jsonb(a.value#>>'{}'), format('$?(@ like_regex "%s")', _pattern)::jsonpath)
    END
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION _ps_trace.tag_maps_regex(_ps_trace.tag_k, text) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.tag_maps_not_regex(_key _ps_trace.tag_k, _pattern text)
RETURNS _ps_trace.tag_maps
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), '{}')::_ps_trace.tag_maps
    FROM _ps_trace.tag a
    WHERE a.key = _key AND
    -- if the jsonb value is a string, apply the regex directly
    -- otherwise, convert the value to a text representation, back to a jsonb string, and then apply
    CASE jsonb_typeof(a.value)
        WHEN 'string' THEN jsonb_path_exists(a.value, format('$?(!(@ like_regex "%s"))', _pattern)::jsonpath)
        ELSE jsonb_path_exists(to_jsonb(a.value#>>'{}'), format('$?(!(@ like_regex "%s"))', _pattern)::jsonpath)
    END
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION _ps_trace.tag_maps_not_regex(_ps_trace.tag_k, text) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.match(_attr_map _ps_trace.tag_map, _maps _ps_trace.tag_maps)
RETURNS boolean
AS $func$
    SELECT _attr_map @> ANY(_maps)
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION _ps_trace.match(_ps_trace.tag_map, _ps_trace.tag_maps) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.tag_id(_key _ps_trace.tag_k)
RETURNS text
AS $func$
    SELECT k.id::text
    FROM _ps_trace.tag_key k
    WHERE k.key = _key
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION _ps_trace.tag_id(_ps_trace.tag_k) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.tag_ids(VARIADIC _keys _ps_trace.tag_k[])
RETURNS text[]
AS $func$
    SELECT array_agg(k.id::text)ƒ
    FROM _ps_trace.tag_key k
    WHERE k.key = ANY(_keys)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION _ps_trace.tag_ids(_ps_trace.tag_k[]) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.tag_maps_equal(_key _ps_trace.tag_k, _val jsonb)
RETURNS _ps_trace.tag_maps
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), '{}')::_ps_trace.tag_maps
    FROM _ps_trace.tag a
    WHERE a.key = _key
    AND a.value = _val
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION _ps_trace.tag_maps_equal(_ps_trace.tag_k, jsonb) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.tag_maps_not_equal(_key _ps_trace.tag_k, _val jsonb)
RETURNS _ps_trace.tag_maps
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), '{}')::_ps_trace.tag_maps
    FROM _ps_trace.tag a
    WHERE a.key = _key
    AND a.value != _val
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION _ps_trace.tag_maps_not_equal(_ps_trace.tag_k, jsonb) TO prom_reader;

DO $do$
DECLARE
    _tpl1 text =
$sql$
CREATE OR REPLACE FUNCTION _ps_trace.tag_maps_%1$s_%2$s(_key _ps_trace.tag_k, _val %3$s)
RETURNS _ps_trace.tag_maps
AS $func$
    SELECT _ps_trace.tag_maps_%2$s(_key,to_jsonb(_val))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
$sql$;
    _tpl2 text =
$sql$
GRANT EXECUTE ON FUNCTION _ps_trace.tag_maps_%1$s_%2$s(_ps_trace.tag_k, %3$s) TO prom_reader;
$sql$;
    _types text[] = ARRAY[
        'text',
        'smallint',
        'int',
        'bigint',
        'bool',
        'real',
        'double precision',
        'numeric',
        'timestamptz',
        'timestamp',
        'time',
        'date'
    ];
    _type text;
BEGIN
    FOREACH _type IN ARRAY _types
    LOOP
        EXECUTE format(_tpl1, replace(_type, ' ', '_'), 'equal', _type);
        EXECUTE format(_tpl2, replace(_type, ' ', '_'), 'equal', _type);
        EXECUTE format(_tpl1, replace(_type, ' ', '_'), 'not_equal', _type);
        EXECUTE format(_tpl2, replace(_type, ' ', '_'), 'equal', _type);
    END LOOP;
END;
$do$;

DO $do$
DECLARE
    _tpl1 text =
$sql$
CREATE OR REPLACE FUNCTION _ps_trace.tag_maps_%1$s_%2$s(_key _ps_trace.tag_k, _val %3$s)
RETURNS _ps_trace.tag_maps
AS $func$
    SELECT _ps_trace.tag_maps(_key, '%4$s', jsonb_build_object('x', to_jsonb(_val)))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
$sql$;
    _tpl2 text =
$sql$
GRANT EXECUTE ON FUNCTION _ps_trace.tag_maps_%1$s_%2$s(_ps_trace.tag_k, %3$s) TO prom_reader;
$sql$;
    _sql record;
BEGIN
    FOR _sql IN
    (
        SELECT
            format
            (
                _tpl1,
                replace(t.type, ' ', '_'),
                f.name,
                t.type,
                format('$?(@ %s $x)', f.jop)
            ) as func,
            format(_tpl2, replace(t.type, ' ', '_'), f.name, t.type) as op
        FROM
        (
            VALUES
            ('smallint'        ),
            ('int'             ),
            ('bigint'          ),
            ('bool'            ),
            ('real'            ),
            ('double precision'),
            ('numeric'         )
        ) t(type)
        CROSS JOIN
        (
            VALUES
            ('less_than'            , '#<'  , '<'),
            ('less_than_equal'      , '#<=' , '<='),
            ('greater_than'         , '#>'  , '>'),
            ('greater_than_equal'   , '#>=' , '>=')
        ) f(name, op, jop)
    )
    LOOP
        EXECUTE _sql.func;
        EXECUTE _sql.op;
    END LOOP;
END;
$do$;


SELECT
    s.trace_id,
    s.span_id,
    s.trace_state,
    s.parent_span_id,
    s.parent_span_id is null as is_root_span,
    n.name,
    s.span_kind,
    s.start_time,
    s.end_time,
    tstzrange(s.start_time, s.end_time, '[]') as time_range,
    s.end_time - s.start_time as duration,
    s.span_tags,
    s.dropped_tags_count,
    s.event_time,
    s.dropped_events_count,
    s.dropped_link_count,
    s.status_code,
    s.status_message,
    il.name as inst_lib_name,
    il.version as inst_lib_version,
    u.url as inst_lib_schema_url,
    s.resource_tags,
    s.resource_dropped_tags_count,
    s.resource_schema_url_id
FROM _ps_trace.span s
INNER JOIN _ps_trace.span_name n ON (s.name_id = n.id)
INNER JOIN _ps_trace.inst_lib il ON (s.inst_lib_id = il.id)
INNER JOIN _ps_trace.schema_url u on (il.schema_url_id = u.id)
;
