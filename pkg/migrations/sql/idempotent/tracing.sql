
CREATE OR REPLACE FUNCTION SCHEMA_TRACING.text(_trace_id SCHEMA_TRACING.trace_id)
RETURNS text
AS $function$
    SELECT replace(_trace_id::text, '-', '')
$function$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.text(SCHEMA_TRACING.trace_id) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.span_attribute_type()
RETURNS SCHEMA_TRACING.attribute_type
AS $sql$
SELECT (1<<0)::smallint::SCHEMA_TRACING.attribute_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.span_attribute_type() TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.resource_attribute_type()
RETURNS SCHEMA_TRACING.attribute_type
AS $sql$
SELECT (1<<1)::smallint::SCHEMA_TRACING.attribute_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.resource_attribute_type() TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.event_attribute_type()
RETURNS SCHEMA_TRACING.attribute_type
AS $sql$
SELECT (1<<2)::smallint::SCHEMA_TRACING.attribute_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.event_attribute_type() TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.link_attribute_type()
RETURNS SCHEMA_TRACING.attribute_type
AS $sql$
SELECT (1<<3)::smallint::SCHEMA_TRACING.attribute_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.link_attribute_type() TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.is_span_attribute_type(_attribute_type SCHEMA_TRACING.attribute_type)
RETURNS BOOLEAN
AS $sql$
SELECT _attribute_type & SCHEMA_TRACING.span_attribute_type() = SCHEMA_TRACING.span_attribute_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.is_span_attribute_type(SCHEMA_TRACING.attribute_type) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.is_resource_attribute_type(_attribute_type SCHEMA_TRACING.attribute_type)
RETURNS BOOLEAN
AS $sql$
SELECT _attribute_type & SCHEMA_TRACING.resource_attribute_type() = SCHEMA_TRACING.resource_attribute_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.is_resource_attribute_type(SCHEMA_TRACING.attribute_type) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.is_event_attribute_type(_attribute_type SCHEMA_TRACING.attribute_type)
RETURNS BOOLEAN
AS $sql$
SELECT _attribute_type & SCHEMA_TRACING.event_attribute_type() = SCHEMA_TRACING.event_attribute_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.is_event_attribute_type(SCHEMA_TRACING.attribute_type) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.is_link_attribute_type(_attribute_type SCHEMA_TRACING.attribute_type)
RETURNS BOOLEAN
AS $sql$
SELECT _attribute_type & SCHEMA_TRACING.link_attribute_type() = SCHEMA_TRACING.link_attribute_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.is_link_attribute_type(SCHEMA_TRACING.attribute_type) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.put_attribute_key(_key text, _attribute_type SCHEMA_TRACING.attribute_type)
RETURNS VOID
AS $sql$
    INSERT INTO SCHEMA_TRACING.attribute_key AS k (key, attribute_type)
    VALUES (_key, _attribute_type)
    ON CONFLICT (key) DO
    UPDATE SET attribute_type = k.attribute_type | EXCLUDED.attribute_type
    WHERE k.attribute_type & EXCLUDED.attribute_type = 0
$sql$
LANGUAGE SQL VOLATILE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.put_attribute_key(text, SCHEMA_TRACING.attribute_type) TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.put_attribute(_key text, _value jsonb, _attribute_type SCHEMA_TRACING.attribute_type)
RETURNS VOID
AS $sql$
    INSERT INTO SCHEMA_TRACING.attribute AS a (attribute_type, key_id, key, value)
    SELECT _attribute_type, ak.id, _key, _value
    FROM SCHEMA_TRACING.attribute_key ak
    WHERE ak.key = _key
    ON CONFLICT (key, value) DO
    UPDATE SET attribute_type = a.attribute_type | EXCLUDED.attribute_type
    WHERE a.attribute_type & EXCLUDED.attribute_type = 0
$sql$
LANGUAGE SQL VOLATILE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.put_attribute(text, jsonb, SCHEMA_TRACING.attribute_type) TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.jsonb(_attr_map SCHEMA_TRACING.attribute_map)
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
        FROM SCHEMA_TRACING.attribute a
        WHERE a.id = x.value::text::bigint
    ) a
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
COMMENT ON FUNCTION SCHEMA_TRACING.jsonb(SCHEMA_TRACING.attribute_map) IS
$$takes an attribute_map which is a map of attribute_key.id to attribute.id and returns a jsonb object containing the key value pairs of attributes$$;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.jsonb(SCHEMA_TRACING.attribute_map) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.jsonb(_attr_map SCHEMA_TRACING.attribute_map, VARIADIC _keys text[])
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
        FROM SCHEMA_TRACING.attribute a
        WHERE a.id = x.value::text::bigint
        AND a.key = ANY(_keys) -- ANY works with partition elimination
    ) a
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
COMMENT ON FUNCTION SCHEMA_TRACING.jsonb(SCHEMA_TRACING.attribute_map) IS
$$takes an attribute_map which is a map of attribute_key.id to attribute.id and returns a jsonb object containing the key value pairs of attributes$$;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.jsonb(SCHEMA_TRACING.attribute_map) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.val(_attr_map SCHEMA_TRACING.attribute_map, _key text)
RETURNS jsonb
AS $sql$
    SELECT a.value
    FROM SCHEMA_TRACING.attribute a
    WHERE a.key = _key
    AND _attr_map @> jsonb_build_object(a.key_id, a.id)
    LIMIT 1
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.val(SCHEMA_TRACING.attribute_map, text) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.val_text(_attr_map SCHEMA_TRACING.attribute_map, _key text)
RETURNS text
AS $sql$
    SELECT a.value#>>'{}'
    FROM SCHEMA_TRACING.attribute a
    WHERE a.key = _key
    AND _attr_map @> jsonb_build_object(a.key_id, a.id)
    LIMIT 1
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.val_text(SCHEMA_TRACING.attribute_map, text) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.get_attributes(_attrs jsonb)
RETURNS SCHEMA_TRACING.attribute_map
AS $sql$
    SELECT coalesce(jsonb_object_agg(a.key_id, a.id), '{}')::SCHEMA_TRACING.attribute_map
    FROM jsonb_each(_attrs) x
    CROSS JOIN LATERAL (SELECT * FROM SCHEMA_TRACING.attribute a where x.key = a.key AND x.value = a.value) a
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.get_attributes(jsonb) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.get_attributes(_key text, _qry jsonpath, _vars jsonb DEFAULT '{}'::jsonb, _silent boolean DEFAULT false)
RETURNS SCHEMA_TRACING.attribute_matchers
AS $sql$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), '{}')::SCHEMA_TRACING.attribute_matchers
    FROM SCHEMA_TRACING.attribute a
    WHERE a.key = _key
    AND jsonb_path_exists(a.value, _qry, _vars, _silent)
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.get_attributes(text, jsonpath, jsonb, boolean) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.get_attributes_jsonpath(_key text, _qry jsonpath)
RETURNS SCHEMA_TRACING.attribute_matchers
AS $sql$
    SELECT SCHEMA_TRACING.get_attributes(_key, _qry);
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.get_attributes_jsonpath(text, jsonpath) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.get_attributes_text_equal(_key text, _val text)
RETURNS SCHEMA_TRACING.attribute_matchers
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), '{}')::SCHEMA_TRACING.attribute_matchers
    FROM SCHEMA_TRACING.attribute a
    WHERE a.key = _key
    AND a.value = to_jsonb(_val)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.get_attributes_text_equal(text, text) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.get_attributes_text_not_equal(_key text, _val text)
RETURNS SCHEMA_TRACING.attribute_matchers
AS $func$
    SELECT SCHEMA_TRACING.get_attributes(_key, format('$?(@ != %s)', to_jsonb(_val))::jsonpath)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.get_attributes_text_not_equal(text, text) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.get_attributes_int_equal(_key text, _val int)
RETURNS SCHEMA_TRACING.attribute_matchers
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), '{}')::SCHEMA_TRACING.attribute_matchers
    FROM SCHEMA_TRACING.attribute a
    WHERE a.key = _key
    AND a.value = to_jsonb(_val)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.get_attributes_int_equal(text, int) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.get_attributes_int_not_equal(_key text, _val int)
RETURNS SCHEMA_TRACING.attribute_matchers
AS $func$
    SELECT SCHEMA_TRACING.get_attributes(_key, format('$?(@ != %s)', to_jsonb(_val))::jsonpath)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.get_attributes_int_not_equal(text, int) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.get_attributes_bool_equal(_key text, _val bool)
RETURNS SCHEMA_TRACING.attribute_matchers
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), '{}')::SCHEMA_TRACING.attribute_matchers
    FROM SCHEMA_TRACING.attribute a
    WHERE a.key = _key
    AND a.value = to_jsonb(_val)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.get_attributes_bool_equal(text, bool) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.get_attributes_bool_not_equal(_key text, _val bool)
RETURNS SCHEMA_TRACING.attribute_matchers
AS $func$
    SELECT SCHEMA_TRACING.get_attributes(_key, format('$?(@ != %s)', to_jsonb(_val))::jsonpath)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.get_attributes_bool_not_equal(text, bool) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.get_attributes_regex(_key text, _pattern text)
RETURNS SCHEMA_TRACING.attribute_matchers
AS $func$
    SELECT SCHEMA_TRACING.get_attributes(_key, format('$?(@ like_regex "%s")', _pattern)::jsonpath)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.get_attributes_regex(text, text) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.get_attributes_not_regex(_key text, _pattern text)
RETURNS SCHEMA_TRACING.attribute_matchers
AS $func$
    SELECT SCHEMA_TRACING.get_attributes(_key, format('$?(!(@ like_regex "%s"))', _pattern)::jsonpath)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.get_attributes_not_regex(text, text) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.match_attribute_map(_attr_map SCHEMA_TRACING.attribute_map, _matchers SCHEMA_TRACING.attribute_matchers)
RETURNS boolean
AS $func$
    SELECT _attr_map @> ANY(_matchers)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.match_attribute_map(SCHEMA_TRACING.attribute_map, SCHEMA_TRACING.attribute_matchers) TO prom_reader;

