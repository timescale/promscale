
CREATE OR REPLACE FUNCTION SCHEMA_TRACING.text(_trace_id SCHEMA_TRACING.trace_id) RETURNS text
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE
AS $function$
    SELECT replace(_trace_id::text, '-', '')
$function$;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.text(SCHEMA_TRACING.trace_id) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.span_attribute_type() RETURNS SCHEMA_TRACING.attribute_type AS
$sql$
SELECT (1<<0)::smallint::SCHEMA_TRACING.attribute_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.span_attribute_type() TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.resource_attribute_type() RETURNS SCHEMA_TRACING.attribute_type AS
$sql$
SELECT (1<<1)::smallint::SCHEMA_TRACING.attribute_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.resource_attribute_type() TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.event_attribute_type() RETURNS SCHEMA_TRACING.attribute_type AS
$sql$
SELECT (1<<2)::smallint::SCHEMA_TRACING.attribute_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.event_attribute_type() TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.link_attribute_type() RETURNS SCHEMA_TRACING.attribute_type AS
$sql$
SELECT (1<<3)::smallint::SCHEMA_TRACING.attribute_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.link_attribute_type() TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.is_span_attribute_type(_attribute_type SCHEMA_TRACING.attribute_type) RETURNS BOOLEAN AS
$sql$
SELECT _attribute_type & SCHEMA_TRACING.span_attribute_type() = SCHEMA_TRACING.span_attribute_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.is_span_attribute_type(SCHEMA_TRACING.attribute_type) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.is_resource_attribute_type(_attribute_type SCHEMA_TRACING.attribute_type) RETURNS BOOLEAN AS
$sql$
SELECT _attribute_type & SCHEMA_TRACING.resource_attribute_type() = SCHEMA_TRACING.resource_attribute_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.is_resource_attribute_type(SCHEMA_TRACING.attribute_type) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.is_event_attribute_type(_attribute_type SCHEMA_TRACING.attribute_type) RETURNS BOOLEAN AS
$sql$
SELECT _attribute_type & SCHEMA_TRACING.event_attribute_type() = SCHEMA_TRACING.event_attribute_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.is_event_attribute_type(SCHEMA_TRACING.attribute_type) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.is_link_attribute_type(_attribute_type SCHEMA_TRACING.attribute_type) RETURNS BOOLEAN AS
$sql$
SELECT _attribute_type & SCHEMA_TRACING.link_attribute_type() = SCHEMA_TRACING.link_attribute_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.is_link_attribute_type(SCHEMA_TRACING.attribute_type) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.put_attribute_key(_key text, _attribute_type SCHEMA_TRACING.attribute_type) RETURNS VOID
AS $sql$
    INSERT INTO SCHEMA_TRACING.attribute_key AS k (key, attribute_type)
    VALUES (_key, _attribute_type)
    ON CONFLICT (key) DO
    UPDATE SET attribute_type = k.attribute_type | EXCLUDED.attribute_type
    WHERE k.attribute_type & EXCLUDED.attribute_type = 0
$sql$
LANGUAGE SQL VOLATILE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.put_attribute_key(text, SCHEMA_TRACING.attribute_type) TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.put_attribute(_key text, _value jsonb, _attribute_type SCHEMA_TRACING.attribute_type) RETURNS VOID
AS $sql$
    INSERT INTO SCHEMA_TRACING.attribute AS a (attribute_type, key, value)
    VALUES (_attribute_type, _key, _value)
    ON CONFLICT (key, value) DO
    UPDATE SET attribute_type = a.attribute_type | EXCLUDED.attribute_type
    WHERE a.attribute_type & EXCLUDED.attribute_type = 0
$sql$
LANGUAGE SQL VOLATILE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.put_attribute(text, jsonb, SCHEMA_TRACING.attribute_type) TO prom_writer;

/****************** there's probably a better name than "explode" but I can't think of one at the moment */

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.explode(_attr_map SCHEMA_TRACING.attribute_map)
RETURNS TABLE
(
    attribute_key_id bigint,
    attribute_id bigint,
    attribute_type SCHEMA_TRACING.attribute_type,
    key text,
    value jsonb
)
AS $sql$
    SELECT
      ak.id,
      a.id,
      a.attribute_type,
      a.key,
      a.value
    FROM
    (
        SELECT
            x.key::bigint as attribute_key_id,
            x.value::text::bigint as attribute_id
        FROM jsonb_each(_attr_map) x
    ) x
    INNER JOIN SCHEMA_TRACING.attribute_key ak on (x.attribute_key_id = ak.id)
    INNER JOIN SCHEMA_TRACING.attribute a on (x.attribute_id = a.id)
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.explode(SCHEMA_TRACING.attribute_map) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.jsonb(_attr_map SCHEMA_TRACING.attribute_map) RETURNS jsonb
AS $sql$
    SELECT jsonb_object_agg(a.key, a.value)
    FROM jsonb_each(_attr_map) x
    INNER JOIN SCHEMA_TRACING.attribute a on (a.id = x.value::text::bigint)
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.jsonb(SCHEMA_TRACING.attribute_map) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.val(_attribute_id bigint) RETURNS jsonb
AS $sql$
    SELECT a.value
    from SCHEMA_TRACING.attribute a
    WHERE a.id = _attribute_id
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.val(bigint) TO prom_reader;

---------------- eq functions ------------------

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.eq(_attr_map SCHEMA_TRACING.attribute_map, _json_attrs jsonb)
RETURNS BOOLEAN
AS $func$
    --assumes no duplicate entries
     SELECT SCHEMA_TRACING.jsonb(_attr_map) = _json_attrs
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
COMMENT ON FUNCTION SCHEMA_TRACING.eq(SCHEMA_TRACING.attribute_map, jsonb)
IS 'returns true if the attribute_map and jsonb object are equal';
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.eq(SCHEMA_TRACING.attribute_map, jsonb) TO prom_reader;

--------------------- op @> ------------------------

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.attribute_map_contains(_attr_map SCHEMA_TRACING.attribute_map, _json_attrs jsonb)
RETURNS BOOLEAN
AS $func$
    SELECT SCHEMA_TRACING.jsonb(_attr_map) @> _json_attrs
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING.@> (
    LEFTARG = SCHEMA_TRACING.attribute_map,
    RIGHTARG = jsonb,
    FUNCTION = SCHEMA_TRACING.attribute_map_contains
);
