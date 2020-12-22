----------------------------------
-- Label selectors and matchers --
----------------------------------
-- This section contains a few functions that we're going to replace in the idempotent section.
-- They are created here just to allow us to create the necessary operators.

CREATE DOMAIN SCHEMA_PROM.matcher_positive AS int[] NOT NULL;
CREATE DOMAIN SCHEMA_PROM.matcher_negative AS int[] NOT NULL;
CREATE DOMAIN SCHEMA_PROM.label_key AS TEXT NOT NULL;
CREATE DOMAIN SCHEMA_PROM.pattern AS TEXT NOT NULL;

--wrapper around jsonb_each_text to give a better row_estimate
--for labels (10 not 100)
CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_jsonb_each_text(js jsonb, OUT key text, OUT value text)
 RETURNS SETOF record
 LANGUAGE SQL
 IMMUTABLE PARALLEL SAFE STRICT ROWS 10
AS $function$ SELECT (jsonb_each_text(js)).* $function$;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.count_jsonb_keys(j jsonb)
RETURNS INT
AS $func$
    SELECT count(*)::int from (SELECT jsonb_object_keys(j)) v;
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.matcher(labels jsonb)
RETURNS SCHEMA_PROM.matcher_positive
AS $func$
    SELECT ARRAY(
           SELECT coalesce(l.id, -1) -- -1 indicates no such label
           FROM label_jsonb_each_text(labels-'__name__') e
           LEFT JOIN SCHEMA_CATALOG.label l
               ON (l.key = e.key AND l.value = e.value)
        )::SCHEMA_PROM.matcher_positive
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

--------------------- op @> ------------------------

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_contains(labels SCHEMA_PROM.label_array, json_labels jsonb)
RETURNS BOOLEAN
AS $func$
    SELECT labels @> SCHEMA_PROM.matcher(json_labels)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_PROM.@> (
    LEFTARG = SCHEMA_PROM.label_array,
    RIGHTARG = jsonb,
    FUNCTION = SCHEMA_CATALOG.label_contains
);

--------------------- op ? ------------------------

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_match(labels SCHEMA_PROM.label_array, matchers SCHEMA_PROM.matcher_positive)
RETURNS BOOLEAN
AS $func$
    SELECT labels && matchers
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_PROM.? (
    LEFTARG = SCHEMA_PROM.label_array,
    RIGHTARG = SCHEMA_PROM.matcher_positive,
    FUNCTION = SCHEMA_CATALOG.label_match
);

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_match(labels SCHEMA_PROM.label_array, matchers SCHEMA_PROM.matcher_negative)
RETURNS BOOLEAN
AS $func$
    SELECT NOT (labels && matchers)
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_PROM.? (
    LEFTARG = SCHEMA_PROM.label_array,
    RIGHTARG = SCHEMA_PROM.matcher_negative,
    FUNCTION = SCHEMA_CATALOG.label_match
);

--------------------- op == !== ==~ !=~ ------------------------

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_find_key_equal(key_to_match SCHEMA_PROM.label_key, pat SCHEMA_PROM.pattern)
RETURNS SCHEMA_PROM.matcher_positive
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::SCHEMA_PROM.matcher_positive
    FROM SCHEMA_CATALOG.label l
    WHERE l.key = key_to_match and l.value = pat
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_find_key_not_equal(key_to_match SCHEMA_PROM.label_key, pat SCHEMA_PROM.pattern)
RETURNS SCHEMA_PROM.matcher_negative
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::SCHEMA_PROM.matcher_negative
    FROM SCHEMA_CATALOG.label l
    WHERE l.key = key_to_match and l.value = pat
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_find_key_regex(key_to_match SCHEMA_PROM.label_key, pat SCHEMA_PROM.pattern)
RETURNS SCHEMA_PROM.matcher_positive
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::SCHEMA_PROM.matcher_positive
    FROM SCHEMA_CATALOG.label l
    WHERE l.key = key_to_match and l.value ~ pat
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_find_key_not_regex(key_to_match SCHEMA_PROM.label_key, pat SCHEMA_PROM.pattern)
RETURNS SCHEMA_PROM.matcher_negative
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::SCHEMA_PROM.matcher_negative
    FROM SCHEMA_CATALOG.label l
    WHERE l.key = key_to_match and l.value ~ pat
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_PROM.== (
    LEFTARG = SCHEMA_PROM.label_key,
    RIGHTARG = SCHEMA_PROM.pattern,
    FUNCTION = SCHEMA_CATALOG.label_find_key_equal
);

CREATE OPERATOR SCHEMA_PROM.!== (
    LEFTARG = SCHEMA_PROM.label_key,
    RIGHTARG = SCHEMA_PROM.pattern,
    FUNCTION = SCHEMA_CATALOG.label_find_key_not_equal
);

CREATE OPERATOR SCHEMA_PROM.==~ (
    LEFTARG = SCHEMA_PROM.label_key,
    RIGHTARG = SCHEMA_PROM.pattern,
    FUNCTION = SCHEMA_CATALOG.label_find_key_regex
);

CREATE OPERATOR SCHEMA_PROM.!=~ (
    LEFTARG = SCHEMA_PROM.label_key,
    RIGHTARG = SCHEMA_PROM.pattern,
    FUNCTION = SCHEMA_CATALOG.label_find_key_not_regex
);