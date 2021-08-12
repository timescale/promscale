
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
