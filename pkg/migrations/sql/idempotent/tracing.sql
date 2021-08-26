
CREATE OR REPLACE FUNCTION prom_trace.text(_trace_id prom_trace.trace_id) RETURNS text
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE
AS $function$
    SELECT replace(_trace_id::text, '-', '')
$function$;
GRANT EXECUTE ON FUNCTION prom_trace.text(prom_trace.trace_id) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.span_attribute_type() RETURNS prom_trace.attribute_type AS
$sql$
SELECT (1<<0)::smallint::prom_trace.attribute_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION prom_trace.span_attribute_type() TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.resource_attribute_type() RETURNS prom_trace.attribute_type AS
$sql$
SELECT (1<<1)::smallint::prom_trace.attribute_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION prom_trace.resource_attribute_type() TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.event_attribute_type() RETURNS prom_trace.attribute_type AS
$sql$
SELECT (1<<2)::smallint::prom_trace.attribute_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION prom_trace.event_attribute_type() TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.link_attribute_type() RETURNS prom_trace.attribute_type AS
$sql$
SELECT (1<<3)::smallint::prom_trace.attribute_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION prom_trace.link_attribute_type() TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.is_span_attribute_type(_attribute_type prom_trace.attribute_type) RETURNS BOOLEAN AS
$sql$
SELECT _attribute_type & prom_trace.span_attribute_type() = prom_trace.span_attribute_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION prom_trace.is_span_attribute_type(prom_trace.attribute_type) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.is_resource_attribute_type(_attribute_type prom_trace.attribute_type) RETURNS BOOLEAN AS
$sql$
SELECT _attribute_type & prom_trace.resource_attribute_type() = prom_trace.resource_attribute_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION prom_trace.is_resource_attribute_type(prom_trace.attribute_type) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.is_event_attribute_type(_attribute_type prom_trace.attribute_type) RETURNS BOOLEAN AS
$sql$
SELECT _attribute_type & prom_trace.event_attribute_type() = prom_trace.event_attribute_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION prom_trace.is_event_attribute_type(prom_trace.attribute_type) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.is_link_attribute_type(_attribute_type prom_trace.attribute_type) RETURNS BOOLEAN AS
$sql$
SELECT _attribute_type & prom_trace.link_attribute_type() = prom_trace.link_attribute_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION prom_trace.is_link_attribute_type(prom_trace.attribute_type) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.put_attribute_key(_key text, _attribute_type prom_trace.attribute_type) RETURNS VOID
AS $sql$
    INSERT INTO prom_trace.attribute_key AS k (key, attribute_type)
    VALUES (_key, _attribute_type)
    ON CONFLICT (key) DO
    UPDATE SET attribute_type = k.attribute_type | EXCLUDED.attribute_type
    WHERE k.attribute_type & EXCLUDED.attribute_type = 0
$sql$
LANGUAGE SQL VOLATILE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.put_attribute_key(text, prom_trace.attribute_type) TO prom_writer;

CREATE OR REPLACE FUNCTION prom_trace.put_attribute(_key text, _value jsonb, _attribute_type prom_trace.attribute_type) RETURNS VOID
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

CREATE OR REPLACE FUNCTION prom_trace.get_attribute_map(_attrs jsonb) RETURNS prom_trace.attribute_map
AS $sql$
    SELECT coalesce(jsonb_object_agg(a.key_id, a.id), '{}')::prom_trace.attribute_map
    FROM jsonb_each(_attrs) x
    CROSS JOIN LATERAL (SELECT * FROM prom_trace.attribute a where x.key = a.key AND x.value = a.value) a
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.get_attribute_map(jsonb) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.get_attribute_map_like(_key text, _pattern text) RETURNS prom_trace.attribute_map[]
AS $sql$
    SELECT array_agg(jsonb_build_object(a.key_id, a.id))::prom_trace.attribute_map[]
    FROM prom_trace.attribute a
    WHERE a.key = _key AND a.value::text LIKE _pattern
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.get_attribute_map_like(text, text) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.get_attribute_map_not_like(_key text, _pattern text) RETURNS prom_trace.attribute_map[]
AS $sql$
    SELECT array_agg(jsonb_build_object(a.key_id, a.id))::prom_trace.attribute_map[]
    FROM prom_trace.attribute a
    WHERE a.key = _key AND a.value::text NOT LIKE _pattern
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.get_attribute_map_not_like(text, text) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.get_attribute_map_jsonb_path_exists(_key text, _path jsonpath, _vars jsonb DEFAULT '{}'::jsonb, _silent boolean DEFAULT false) RETURNS prom_trace.attribute_map[]
AS $sql$
    SELECT array_agg(jsonb_build_object(a.key_id, a.id))::prom_trace.attribute_map[]
    FROM prom_trace.attribute a
    WHERE a.key = _key AND jsonb_path_exists(a.value, _path, _vars, _silent)
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.get_attribute_map_jsonb_path_exists(text, jsonpath, jsonb, boolean) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.get_attribute_map_jsonb_path_match(_key text, _path jsonpath, _vars jsonb DEFAULT '{}'::jsonb, _silent boolean DEFAULT false) RETURNS prom_trace.attribute_map[]
AS $sql$
    SELECT array_agg(jsonb_build_object(a.key_id, a.id))::prom_trace.attribute_map[]
    FROM prom_trace.attribute a
    WHERE a.key = _key AND jsonb_path_match(a.value, _path, _vars, _silent)
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.get_attribute_map_jsonb_path_match(text, jsonpath, jsonb, boolean) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.get_attribute_key_id(_attr_key text) RETURNS bigint
AS $sql$
    SELECT ak.id FROM prom_trace.attribute_key ak where ak.key = _attr_key
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.get_attribute_key_id(text) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.get_attribute_key_ids(VARIADIC _attr_keys text[]) RETURNS bigint[]
AS $sql$
    SELECT array_agg(ak.id)
    FROM prom_trace.attribute_key ak
    WHERE ak.key = ANY(_attr_keys)
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION prom_trace.get_attribute_key_ids(VARIADIC text[]) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.jsonb(_attr_map prom_trace.attribute_map) RETURNS jsonb
AS $sql$
    /*
    takes an attribute_map which is a map of attribute_key.id to attribute.id
    and returns a jsonb object containing the key value pairs of attributes
    */
    SELECT jsonb_object_agg(a.key, a.value)
    FROM jsonb_each(_attr_map) x -- key is attribute_key.id, value is attribute.id
    CROSS JOIN LATERAL (SELECT * FROM prom_trace.attribute a where a.id = x.value::text::bigint) a
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
COMMENT ON FUNCTION prom_trace.jsonb(prom_trace.attribute_map) IS
$$takes an attribute_map which is a map of attribute_key.id to attribute.id and returns a jsonb object containing the key value pairs of attributes$$;
GRANT EXECUTE ON FUNCTION prom_trace.jsonb(prom_trace.attribute_map) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_trace.val(_attribute_id bigint) RETURNS jsonb
AS $sql$
    /*
    returns the attribute value associated with the attribute.id passed
    */
    SELECT a.value
    from prom_trace.attribute a
    WHERE a.id = _attribute_id
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
COMMENT ON FUNCTION prom_trace.val(bigint) IS
$$returns the attribute value associated with the attribute.id passed$$;
GRANT EXECUTE ON FUNCTION prom_trace.val(bigint) TO prom_reader;

/*
---------------- eq functions ------------------

CREATE OR REPLACE FUNCTION prom_trace.eq(_attr_map prom_trace.attribute_map, _json_attrs jsonb)
RETURNS BOOLEAN
AS $func$
    --assumes no duplicate entries
     SELECT prom_trace.jsonb(_attr_map) = _json_attrs
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
COMMENT ON FUNCTION prom_trace.eq(prom_trace.attribute_map, jsonb)
IS 'returns true if the attribute_map and jsonb object are equal';
GRANT EXECUTE ON FUNCTION prom_trace.eq(prom_trace.attribute_map, jsonb) TO prom_reader;

CREATE OPERATOR prom_trace.== (
    LEFTARG = prom_trace.attribute_map,
    RIGHTARG = jsonb,
    FUNCTION = prom_trace.eq
);

--------------------- op @> ------------------------

CREATE OR REPLACE FUNCTION prom_trace.attribute_map_contains(_attr_map prom_trace.attribute_map, _json_attrs jsonb)
RETURNS BOOLEAN
AS $func$
    SELECT prom_trace.jsonb(_attr_map) @> _json_attrs
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR prom_trace.@> (
    LEFTARG = prom_trace.attribute_map,
    RIGHTARG = jsonb,
    FUNCTION = prom_trace.attribute_map_contains
);
*/