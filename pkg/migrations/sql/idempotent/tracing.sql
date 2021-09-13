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

CREATE OR REPLACE FUNCTION _ps_trace.put_tag(_key _ps_trace.tag_k, _value _ps_trace.tag_v, _tag_type _ps_trace.tag_type)
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
GRANT EXECUTE ON FUNCTION _ps_trace.put_tag(_ps_trace.tag_k, _ps_trace.tag_v, _ps_trace.tag_type) TO prom_writer;

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
RETURNS _ps_trace.tag_v
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

CREATE OR REPLACE FUNCTION _ps_trace.tag_maps_equal(_key _ps_trace.tag_k, _val _ps_trace.tag_v)
RETURNS _ps_trace.tag_maps
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), '{}')::_ps_trace.tag_maps
    FROM _ps_trace.tag a
    WHERE a.key = _key
    AND a.value = _val
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION _ps_trace.tag_maps_equal(_ps_trace.tag_k, _ps_trace.tag_v) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.tag_maps_not_equal(_key _ps_trace.tag_k, _val _ps_trace.tag_v)
RETURNS _ps_trace.tag_maps
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), '{}')::_ps_trace.tag_maps
    FROM _ps_trace.tag a
    WHERE a.key = _key
    AND a.value != _val
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION _ps_trace.tag_maps_not_equal(_ps_trace.tag_k, _ps_trace.tag_v) TO prom_reader;

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
        EXECUTE format(_tpl2, replace(_type, ' ', '_'), 'not_equal', _type);
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
            ('less_than'            , '#<'  , '<' ),
            ('less_than_equal'      , '#<=' , '<='),
            ('greater_than'         , '#>'  , '>' ),
            ('greater_than_equal'   , '#>=' , '>=')
        ) f(name, op, jop)
    )
    LOOP
        EXECUTE _sql.func;
        EXECUTE _sql.op;
    END LOOP;
END;
$do$;

CREATE OR REPLACE VIEW ps_trace.span AS
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
    u1.url as inst_lib_schema_url,
    s.resource_tags,
    s.resource_dropped_tags_count,
    u2.url as resource_schema_url
FROM _ps_trace.span s
INNER JOIN _ps_trace.span_name n ON (s.name_id = n.id)
INNER JOIN _ps_trace.inst_lib il ON (s.inst_lib_id = il.id)
INNER JOIN _ps_trace.schema_url u1 on (il.schema_url_id = u1.id)
INNER JOIN _ps_trace.schema_url u2 on (il.schema_url_id = u2.id)
;
GRANT SELECT ON ps_trace.span to prom_reader;

CREATE OR REPLACE VIEW ps_trace.event AS
SELECT
    e.trace_id,
    e.span_id,
    e.time,
    e.event_number,
    e.name as event_name,
    e.tags as event_tags,
    e.dropped_tags_count,
    s.trace_state,
    n.name as span_name,
    s.span_kind,
    s.start_time as span_start_time,
    s.end_time as span_end_time,
    tstzrange(s.start_time, s.end_time, '[]') as time_range,
    s.end_time - s.start_time as duration,
    s.span_tags,
    s.dropped_tags_count as dropped_span_tags_count,
    s.status_code,
    s.status_message
FROM _ps_trace.event e
INNER JOIN _ps_trace.span s on (e.span_id = s.span_id AND e.trace_id = s.trace_id)
INNER JOIN _ps_trace.span_name n ON (s.name_id = n.id)
;
GRANT SELECT ON ps_trace.event to prom_reader;

CREATE OR REPLACE VIEW ps_trace.link AS
SELECT
    s1.trace_id                    ,
    s1.span_id                     ,
    s1.trace_state                 ,
    s1.parent_span_id              ,
    s1.is_root_span                ,
    s1.name                        ,
    s1.span_kind                   ,
    s1.start_time                  ,
    s1.end_time                    ,
    s1.time_range                  ,
    s1.duration                    ,
    s1.span_tags                   ,
    s1.dropped_tags_count          ,
    s1.event_time                  ,
    s1.dropped_events_count        ,
    s1.dropped_link_count          ,
    s1.status_code                 ,
    s1.status_message              ,
    s1.inst_lib_name               ,
    s1.inst_lib_version            ,
    s1.inst_lib_schema_url         ,
    s1.resource_tags               ,
    s1.resource_dropped_tags_count ,
    s1.resource_schema_url         ,
    s2.trace_id                    as linked_trace_id                   ,
    s2.span_id                     as linked_span_id                    ,
    s2.trace_state                 as linked_trace_state                ,
    s2.parent_span_id              as linked_parent_span_id             ,
    s2.is_root_span                as linked_is_root_span               ,
    s2.name                        as linked_name                       ,
    s2.span_kind                   as linked_span_kind                  ,
    s2.start_time                  as linked_start_time                 ,
    s2.end_time                    as linked_end_time                   ,
    s2.time_range                  as linked_time_range                 ,
    s2.duration                    as linked_duration                   ,
    s2.span_tags                   as linked_span_tags                  ,
    s2.dropped_tags_count          as linked_dropped_tags_count         ,
    s2.event_time                  as linked_event_time                 ,
    s2.dropped_events_count        as linked_dropped_events_count       ,
    s2.dropped_link_count          as linked_dropped_link_count         ,
    s2.status_code                 as linked_status_code                ,
    s2.status_message              as linked_status_message             ,
    s2.inst_lib_name               as linked_inst_lib_name              ,
    s2.inst_lib_version            as linked_inst_lib_version           ,
    s2.inst_lib_schema_url         as linked_inst_lib_schema_url        ,
    s2.resource_tags               as linked_resource_tags              ,
    s2.resource_dropped_tags_count as linked_resource_dropped_tags_count,
    s2.resource_schema_url         as linked_resource_schema_url        ,
    k.tags as link_tags,
    k.dropped_tags_count as dropped_link_tags_count
FROM _ps_trace.link k
INNER JOIN ps_trace.span s1 on (k.span_id = s1.span_id and k.trace_id = s1.trace_id)
INNER JOIN ps_trace.span s2 on (k.linked_span_id = s2.span_id and k.linked_trace_id = s2.trace_id)
;
GRANT SELECT ON ps_trace.link to prom_reader;




