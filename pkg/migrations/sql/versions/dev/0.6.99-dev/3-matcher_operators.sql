
--------------------- op == !== ==~ !=~ ------------------------

CREATE OR REPLACE FUNCTION _prom_catalog.match_equals(labels prom_api.label_array, _op ps_tag.tag_op_equals)
RETURNS boolean
AS $func$
    SELECT labels && label_find_key_equal(_op.tag_key, (_op.value#>>'{}'))::int[]
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _prom_catalog.match_equals(prom_api.label_array, ps_tag.tag_op_equals) TO prom_reader;

CREATE OPERATOR _prom_catalog.? (
    LEFTARG = prom_api.label_array,
    RIGHTARG = ps_tag.tag_op_equals,
    FUNCTION = _prom_catalog.match_equals
);

CREATE OR REPLACE FUNCTION _prom_catalog.match_not_equals(labels prom_api.label_array, _op ps_tag.tag_op_not_equals)
RETURNS boolean
AS $func$
    SELECT NOT (labels && label_find_key_not_equal(_op.tag_key, (_op.value#>>'{}'))::int[])
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _prom_catalog.match_not_equals(prom_api.label_array, ps_tag.tag_op_not_equals) TO prom_reader;

CREATE OPERATOR _prom_catalog.? (
    LEFTARG = prom_api.label_array,
    RIGHTARG = ps_tag.tag_op_not_equals,
    FUNCTION = _prom_catalog.match_not_equals
);

CREATE OR REPLACE FUNCTION _prom_catalog.match_regexp_matches(labels prom_api.label_array, _op ps_tag.tag_op_regexp_matches)
RETURNS boolean
AS $func$
    SELECT labels && label_find_key_regex(_op.tag_key, _op.value)::int[]
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _prom_catalog.match_regexp_matches(prom_api.label_array, ps_tag.tag_op_regexp_matches) TO prom_reader;

CREATE OPERATOR _prom_catalog.? (
    LEFTARG = prom_api.label_array,
    RIGHTARG = ps_tag.tag_op_regexp_matches,
    FUNCTION = _prom_catalog.match_regexp_matches
);

CREATE OR REPLACE FUNCTION _prom_catalog.match_regexp_not_matches(labels prom_api.label_array, _op ps_tag.tag_op_regexp_not_matches)
RETURNS boolean
AS $func$
    SELECT NOT (labels && label_find_key_not_regex(_op.tag_key, _op.value)::int[])
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _prom_catalog.match_regexp_not_matches(prom_api.label_array, ps_tag.tag_op_regexp_not_matches) TO prom_reader;

CREATE OPERATOR _prom_catalog.? (
    LEFTARG = prom_api.label_array,
    RIGHTARG = ps_tag.tag_op_regexp_not_matches,
    FUNCTION = _prom_catalog.match_regexp_not_matches
);
