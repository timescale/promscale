
SET LOCAL search_path TO DEFAULT;

CREATE OR REPLACE FUNCTION @extschema@.const_support(internal) RETURNS INTERNAL
AS '$libdir/timescale_prometheus_extra', 'const_support'
LANGUAGE C IMMUTABLE STRICT;

--wrapper around jsonb_each_text to give a better row_estimate
--for labels (10 not 100)
CREATE OR REPLACE FUNCTION @extschema@.label_jsonb_each_text(js jsonb,  OUT key text, OUT value text)
 RETURNS SETOF record
 LANGUAGE internal
 IMMUTABLE PARALLEL SAFE STRICT ROWS 10
AS $function$jsonb_each_text$function$;

--wrapper around unnest to give better row estimate (10 not 100)
CREATE OR REPLACE FUNCTION @extschema@.label_unnest(label_array anyarray)
 RETURNS SETOF anyelement
 LANGUAGE internal
 IMMUTABLE PARALLEL SAFE STRICT ROWS 10
AS $function$array_unnest$function$;

---------------------  comparison functions ---------------------

CREATE OR REPLACE FUNCTION @extschema@.label_find_key_equal(label_key label_key, pattern pattern)
RETURNS matcher_positive
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::matcher_positive
    FROM _prom_catalog.label l
    WHERE l.key = label_key and l.value = pattern
$func$
LANGUAGE SQL STABLE PARALLEL SAFE
SUPPORT const_support;

CREATE OR REPLACE FUNCTION @extschema@.label_find_key_not_equal(key label_key, pattern pattern)
RETURNS matcher_negative
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::matcher_negative
    FROM _prom_catalog.label l
    WHERE l.key = key and l.value = pattern
$func$
LANGUAGE SQL STABLE PARALLEL SAFE
SUPPORT const_support;

CREATE OR REPLACE FUNCTION @extschema@.label_find_key_regex(key label_key, pattern pattern)
RETURNS matcher_positive
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::matcher_positive
    FROM _prom_catalog.label l
    WHERE l.key = key and l.value ~ pattern
$func$
LANGUAGE SQL STABLE PARALLEL SAFE
SUPPORT const_support;

CREATE OR REPLACE FUNCTION @extschema@.label_find_key_not_regex(key label_key, pattern pattern)
RETURNS matcher_negative
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::matcher_negative
    FROM _prom_catalog.label l
    WHERE l.key = key and l.value ~ pattern
$func$
LANGUAGE SQL STABLE PARALLEL SAFE
SUPPORT const_support;

CREATE OPERATOR @extschema@.== (
    LEFTARG = label_key,
    RIGHTARG = pattern,
    FUNCTION = @extschema@.label_find_key_equal
);

CREATE OPERATOR @extschema@.!== (
    LEFTARG = label_key,
    RIGHTARG = pattern,
    FUNCTION = @extschema@.label_find_key_not_equal
);

CREATE OPERATOR @extschema@.==~ (
    LEFTARG = label_key,
    RIGHTARG = pattern,
    FUNCTION = @extschema@.label_find_key_regex
);

CREATE OPERATOR @extschema@.!=~ (
    LEFTARG = label_key,
    RIGHTARG = pattern,
    FUNCTION = @extschema@.label_find_key_not_regex
);
