
--------------------- op == !== ==~ !=~ ------------------------

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.match_equals(labels SCHEMA_PROM.label_array, _op SCHEMA_TAG.tag_op_equals)
RETURNS boolean
AS $func$
    SELECT labels && label_find_key_equal(_op.tag_key, (_op.value#>>'{}'))::int[]
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.match_equals(SCHEMA_PROM.label_array, SCHEMA_TAG.tag_op_equals) TO prom_reader;

CREATE OPERATOR SCHEMA_CATALOG.? (
    LEFTARG = SCHEMA_PROM.label_array,
    RIGHTARG = SCHEMA_TAG.tag_op_equals,
    FUNCTION = SCHEMA_CATALOG.match_equals
);

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.match_not_equals(labels SCHEMA_PROM.label_array, _op SCHEMA_TAG.tag_op_not_equals)
RETURNS boolean
AS $func$
    SELECT NOT (labels && label_find_key_not_equal(_op.tag_key, (_op.value#>>'{}'))::int[])
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.match_not_equals(SCHEMA_PROM.label_array, SCHEMA_TAG.tag_op_not_equals) TO prom_reader;

CREATE OPERATOR SCHEMA_CATALOG.? (
    LEFTARG = SCHEMA_PROM.label_array,
    RIGHTARG = SCHEMA_TAG.tag_op_not_equals,
    FUNCTION = SCHEMA_CATALOG.match_not_equals
);

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.match_regexp_matches(labels SCHEMA_PROM.label_array, _op SCHEMA_TAG.tag_op_regexp_matches)
RETURNS boolean
AS $func$
    SELECT labels && label_find_key_regex(_op.tag_key, _op.value)::int[]
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.match_regexp_matches(SCHEMA_PROM.label_array, SCHEMA_TAG.tag_op_regexp_matches) TO prom_reader;

CREATE OPERATOR SCHEMA_CATALOG.? (
    LEFTARG = SCHEMA_PROM.label_array,
    RIGHTARG = SCHEMA_TAG.tag_op_regexp_matches,
    FUNCTION = SCHEMA_CATALOG.match_regexp_matches
);

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.match_regexp_not_matches(labels SCHEMA_PROM.label_array, _op SCHEMA_TAG.tag_op_regexp_not_matches)
RETURNS boolean
AS $func$
    SELECT NOT (labels && label_find_key_not_regex(_op.tag_key, _op.value)::int[])
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.match_regexp_not_matches(SCHEMA_PROM.label_array, SCHEMA_TAG.tag_op_regexp_not_matches) TO prom_reader;

CREATE OPERATOR SCHEMA_CATALOG.? (
    LEFTARG = SCHEMA_PROM.label_array,
    RIGHTARG = SCHEMA_TAG.tag_op_regexp_not_matches,
    FUNCTION = SCHEMA_CATALOG.match_regexp_not_matches
);
