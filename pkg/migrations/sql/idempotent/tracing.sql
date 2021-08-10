
CREATE OR REPLACE FUNCTION text(_trace_id trace_id) RETURNS text
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE
AS $function$
    SELECT replace(_trace_id::text, '-', '')
$function$;

CREATE OR REPLACE FUNCTION span_attribute_type() RETURNS smallint AS
$sql$
SELECT (1<<0)::smallint
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE
;

CREATE OR REPLACE FUNCTION resource_attribute_type() RETURNS smallint AS
$sql$
SELECT (1<<1)::smallint
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE
;

CREATE OR REPLACE FUNCTION event_attribute_type() RETURNS smallint AS
$sql$
SELECT (1<<2)::smallint
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE
;

CREATE OR REPLACE FUNCTION link_attribute_type() RETURNS smallint AS
$sql$
SELECT (1<<3)::smallint
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE
;

CREATE OR REPLACE FUNCTION is_span_attribute_type(_attribute_type attribute_type) RETURNS BOOLEAN AS
$sql$
SELECT _attribute_type & span_attribute_type() = span_attribute_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE
;

CREATE OR REPLACE FUNCTION is_resource_attribute_type(_attribute_type attribute_type) RETURNS BOOLEAN AS
$sql$
SELECT _attribute_type & resource_attribute_type() = resource_attribute_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE
;

CREATE OR REPLACE FUNCTION is_event_attribute_type(_attribute_type attribute_type) RETURNS BOOLEAN AS
$sql$
SELECT _attribute_type & event_attribute_type() = event_attribute_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE
;

CREATE OR REPLACE FUNCTION is_link_attribute_type(_attribute_type attribute_type) RETURNS BOOLEAN AS
$sql$
SELECT _attribute_type & link_attribute_type() = link_attribute_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE
;

CREATE OR REPLACE FUNCTION put_attribute_key(_key text, _attribute_type attribute_type) RETURNS VOID
AS $sql$
    INSERT INTO attribute_key AS k (key, attribute_type)
    VALUES (_key, _attribute_type)
    ON CONFLICT (key) DO
    UPDATE SET attribute_type = k.attribute_type | EXCLUDED.attribute_type
    WHERE k.attribute_type & EXCLUDED.attribute_type = 0
$sql$
LANGUAGE SQL VOLATILE STRICT
;

CREATE OR REPLACE FUNCTION put_attribute(_key text, _value jsonb, _attribute_type attribute_type) RETURNS VOID
AS $sql$
    INSERT INTO attribute AS a (attribute_type, key, value)
    VALUES (_attribute_type, _key, _value)
    ON CONFLICT (key, value) DO
    UPDATE SET attribute_type = a.attribute_type | EXCLUDED.attribute_type
    WHERE a.attribute_type & EXCLUDED.attribute_type = 0
$sql$
LANGUAGE SQL VOLATILE STRICT
;
