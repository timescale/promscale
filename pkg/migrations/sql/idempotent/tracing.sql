
CREATE OR REPLACE FUNCTION prom_trace.text(_trace_id prom_trace.trace_id)
RETURNS text
AS $function$
    SELECT replace(_trace_id::text, '-', '')
$function$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION prom_trace.text(prom_trace.trace_id) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.span_attribute_type()
RETURNS prom_trace.attribute_type
AS $sql$
SELECT (1<<0)::smallint::prom_trace.attribute_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION prom_trace.span_attribute_type() TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.resource_attribute_type()
RETURNS prom_trace.attribute_type
AS $sql$
SELECT (1<<1)::smallint::prom_trace.attribute_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION prom_trace.resource_attribute_type() TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.event_attribute_type()
RETURNS prom_trace.attribute_type
AS $sql$
SELECT (1<<2)::smallint::prom_trace.attribute_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION prom_trace.event_attribute_type() TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.link_attribute_type()
RETURNS prom_trace.attribute_type
AS $sql$
SELECT (1<<3)::smallint::prom_trace.attribute_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION prom_trace.link_attribute_type() TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.is_span_attribute_type(_attribute_type prom_trace.attribute_type)
RETURNS BOOLEAN
AS $sql$
SELECT _attribute_type & prom_trace.span_attribute_type() = prom_trace.span_attribute_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION prom_trace.is_span_attribute_type(prom_trace.attribute_type) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.is_resource_attribute_type(_attribute_type prom_trace.attribute_type)
RETURNS BOOLEAN
AS $sql$
SELECT _attribute_type & prom_trace.resource_attribute_type() = prom_trace.resource_attribute_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION prom_trace.is_resource_attribute_type(prom_trace.attribute_type) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.is_event_attribute_type(_attribute_type prom_trace.attribute_type)
RETURNS BOOLEAN
AS $sql$
SELECT _attribute_type & prom_trace.event_attribute_type() = prom_trace.event_attribute_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION prom_trace.is_event_attribute_type(prom_trace.attribute_type) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.is_link_attribute_type(_attribute_type prom_trace.attribute_type)
RETURNS BOOLEAN
AS $sql$
SELECT _attribute_type & prom_trace.link_attribute_type() = prom_trace.link_attribute_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION prom_trace.is_link_attribute_type(prom_trace.attribute_type) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.put_attribute_key(_key text, _attribute_type prom_trace.attribute_type)
RETURNS VOID
AS $sql$
    INSERT INTO prom_trace.attribute_key AS k (key, attribute_type)
    VALUES (_key, _attribute_type)
    ON CONFLICT (key) DO
    UPDATE SET attribute_type = k.attribute_type | EXCLUDED.attribute_type
    WHERE k.attribute_type & EXCLUDED.attribute_type = 0
$sql$
LANGUAGE SQL VOLATILE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.put_attribute_key(text, prom_trace.attribute_type) TO prom_writer;

CREATE OR REPLACE FUNCTION prom_trace.put_attribute(_key text, _value jsonb, _attribute_type prom_trace.attribute_type)
RETURNS VOID
AS $sql$
    INSERT INTO prom_trace.attribute AS a (attribute_type, key_id, key, value)
    SELECT _attribute_type, ak.id, _key, _value
    FROM prom_trace.attribute_key ak
    WHERE ak.key = _key
    ON CONFLICT (key, value) DO
    UPDATE SET attribute_type = a.attribute_type | EXCLUDED.attribute_type
    WHERE a.attribute_type & EXCLUDED.attribute_type = 0
$sql$
LANGUAGE SQL VOLATILE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.put_attribute(text, jsonb, prom_trace.attribute_type) TO prom_writer;

CREATE OR REPLACE FUNCTION prom_trace.jsonb(_attr_map prom_trace.attribute_map)
RETURNS jsonb
AS $sql$
    /*
    takes an attribute_map which is a map of attribute_key.id to attribute.id
    and returns a jsonb object containing the key value pairs of attributes
    */
    SELECT jsonb_object_agg(a.key, a.value)
    FROM jsonb_each(_attr_map) x -- key is attribute_key.id, value is attribute.id
    CROSS JOIN LATERAL -- cross join lateral enables partition elimination at execution time
    (
        SELECT
            a.key,
            a.value
        FROM prom_trace.attribute a
        WHERE a.id = x.value::text::bigint
    ) a
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.jsonb(prom_trace.attribute_map) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.jsonb(_attr_map prom_trace.attribute_map, VARIADIC _keys text[])
RETURNS jsonb
AS $sql$
    /*
    takes an attribute_map which is a map of attribute_key.id to attribute.id
    and returns a jsonb object containing the key value pairs of attributes
    only the key/value pairs with keys passed as arguments are included in the output
    */
    SELECT jsonb_object_agg(a.key, a.value)
    FROM jsonb_each(_attr_map) x -- key is attribute_key.id, value is attribute.id
    CROSS JOIN LATERAL -- cross join lateral enables partition elimination at execution time
    (
        SELECT
            a.key,
            a.value
        FROM prom_trace.attribute a
        WHERE a.id = x.value::text::bigint
        AND a.key = ANY(_keys) -- ANY works with partition elimination
    ) a
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.jsonb(prom_trace.attribute_map) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.val(_attr_map prom_trace.attribute_map, _key text)
RETURNS jsonb
AS $sql$
    SELECT a.value
    FROM prom_trace.attribute a
    WHERE a.key = _key
    AND _attr_map @> jsonb_build_object(a.key_id, a.id)
    LIMIT 1
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.val(prom_trace.attribute_map, text) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.val_text(_attr_map prom_trace.attribute_map, _key text)
RETURNS text
AS $sql$
    SELECT a.value#>>'{}'
    FROM prom_trace.attribute a
    WHERE a.key = _key
    AND _attr_map @> jsonb_build_object(a.key_id, a.id)
    LIMIT 1
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.val_text(prom_trace.attribute_map, text) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.get_attribute_map(_attrs jsonb)
RETURNS prom_trace.attribute_map
AS $sql$
    SELECT coalesce(jsonb_object_agg(a.key_id, a.id), '{}')::prom_trace.attribute_map
    FROM jsonb_each(_attrs) x
    CROSS JOIN LATERAL (SELECT * FROM prom_trace.attribute a where x.key = a.key AND x.value = a.value) a
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.get_attribute_map(jsonb) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers(_key text, _qry jsonpath, _vars jsonb DEFAULT '{}'::jsonb, _silent boolean DEFAULT false)
RETURNS prom_trace.attribute_matchers
AS $sql$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), '{}')::prom_trace.attribute_matchers
    FROM prom_trace.attribute a
    WHERE a.key = _key
    AND jsonb_path_exists(a.value, _qry, _vars, _silent)
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers(text, jsonpath, jsonb, boolean) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_jsonpath(_key text, _qry jsonpath)
RETURNS prom_trace.attribute_matchers
AS $sql$
    SELECT prom_trace.attribute_matchers(_key, _qry);
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_jsonpath(text, jsonpath) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_equal(_key text, _val jsonb)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), '{}')::prom_trace.attribute_matchers
    FROM prom_trace.attribute a
    WHERE a.key = _key
    AND a.value = _val
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_equal(text, jsonb) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_not_equal(_key text, _val jsonb)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), '{}')::prom_trace.attribute_matchers
    FROM prom_trace.attribute a
    WHERE a.key = _key
    AND a.value != _val
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_not_equal(text, jsonb) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_text_equal(_key text, _val text)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT prom_trace.attribute_matchers_equal(_key,to_jsonb(_val))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_text_equal(text, text) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_text_not_equal(_key text, _val text)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT prom_trace.attribute_matchers_not_equal(_key, to_jsonb(_val))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_text_not_equal(text, text) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_int_equal(_key text, _val int)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT prom_trace.attribute_matchers_equal(_key,to_jsonb(_val))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_int_equal(text, int) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_int_not_equal(_key text, _val int)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT prom_trace.attribute_matchers_not_equal(_key, to_jsonb(_val))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_int_not_equal(text, int) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_bigint_equal(_key text, _val bigint)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT prom_trace.attribute_matchers_equal(_key,to_jsonb(_val))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_bigint_equal(text, bigint) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_bigint_not_equal(_key text, _val bigint)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT prom_trace.attribute_matchers_not_equal(_key, to_jsonb(_val))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_bigint_not_equal(text, bigint) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_numeric_equal(_key text, _val numeric)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT prom_trace.attribute_matchers_equal(_key,to_jsonb(_val))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_numeric_equal(text, numeric) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_numeric_not_equal(_key text, _val numeric)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT prom_trace.attribute_matchers_not_equal(_key, to_jsonb(_val))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_numeric_not_equal(text, numeric) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_real_equal(_key text, _val real)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT prom_trace.attribute_matchers_equal(_key,to_jsonb(_val))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_real_equal(text, real) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_real_not_equal(_key text, _val real)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT prom_trace.attribute_matchers_not_equal(_key, to_jsonb(_val))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_real_not_equal(text, real) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_double_equal(_key text, _val double precision)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT prom_trace.attribute_matchers_equal(_key,to_jsonb(_val))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_double_equal(text, double precision) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_double_not_equal(_key text, _val double precision)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT prom_trace.attribute_matchers_not_equal(_key, to_jsonb(_val))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_double_not_equal(text, double precision) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_bool_equal(_key text, _val bool)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT prom_trace.attribute_matchers_equal(_key,to_jsonb(_val))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_bool_equal(text, bool) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_bool_not_equal(_key text, _val bool)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT prom_trace.attribute_matchers_not_equal(_key, to_jsonb(_val))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_bool_not_equal(text, bool) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_timestamptz_equal(_key text, _val timestamptz)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT prom_trace.attribute_matchers_equal(_key,to_jsonb(_val))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_timestamptz_equal(text, timestamptz) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_timestamptz_not_equal(_key text, _val timestamptz)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT prom_trace.attribute_matchers_not_equal(_key, to_jsonb(_val))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_timestamptz_not_equal(text, timestamptz) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_time_equal(_key text, _val time)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT prom_trace.attribute_matchers_equal(_key,to_jsonb(_val))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_time_equal(text, time) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_time_not_equal(_key text, _val time)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT prom_trace.attribute_matchers_not_equal(_key, to_jsonb(_val))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_time_not_equal(text, time) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_date_equal(_key text, _val date)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT prom_trace.attribute_matchers_equal(_key,to_jsonb(_val))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_date_equal(text, date) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_date_not_equal(_key text, _val date)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT prom_trace.attribute_matchers_not_equal(_key, to_jsonb(_val))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_date_not_equal(text, date) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_regex(_key text, _pattern text)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), '{}')::prom_trace.attribute_matchers
    FROM prom_trace.attribute a
    WHERE a.key = _key AND
    -- if the jsonb value is a string, apply the regex directly
    -- otherwise, convert the value to a text representation, back to a jsonb string, and then apply
    CASE jsonb_typeof(a.value)
        WHEN 'string' THEN jsonb_path_exists(a.value, format('$?(@ like_regex "%s")', _pattern)::jsonpath)
        ELSE jsonb_path_exists(to_jsonb(a.value#>>'{}'), format('$?(@ like_regex "%s")', _pattern)::jsonpath)
    END
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_regex(text, text) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_matchers_not_regex(_key text, _pattern text)
RETURNS prom_trace.attribute_matchers
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), '{}')::prom_trace.attribute_matchers
    FROM prom_trace.attribute a
    WHERE a.key = _key AND
    -- if the jsonb value is a string, apply the regex directly
    -- otherwise, convert the value to a text representation, back to a jsonb string, and then apply
    CASE jsonb_typeof(a.value)
        WHEN 'string' THEN jsonb_path_exists(a.value, format('$?(!(@ like_regex "%s"))', _pattern)::jsonpath)
        ELSE jsonb_path_exists(to_jsonb(a.value#>>'{}'), format('$?(!(@ like_regex "%s"))', _pattern)::jsonpath)
    END
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_matchers_not_regex(text, text) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.match(_attr_map prom_trace.attribute_map, _matchers prom_trace.attribute_matchers)
RETURNS boolean
AS $func$
    SELECT _attr_map @> ANY(_matchers)
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.match(prom_trace.attribute_map, prom_trace.attribute_matchers) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_id(_key text)
RETURNS text
AS $func$
    SELECT k.id::text
    FROM prom_trace.attribute_key k
    WHERE k.key = _key
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_id(text) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.attribute_ids(VARIADIC _keys text[])
RETURNS text[]
AS $func$
    SELECT array_agg(k.id::text)
    FROM prom_trace.attribute_key k
    WHERE k.key = ANY(_keys)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.attribute_ids(text[]) TO prom_reader;

