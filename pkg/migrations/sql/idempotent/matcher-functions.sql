CREATE OR REPLACE FUNCTION prom_api.matcher(labels jsonb)
RETURNS prom_api.matcher_positive
AS $func$
    SELECT ARRAY(
           SELECT coalesce(l.id, -1) -- -1 indicates no such label
           FROM label_jsonb_each_text(labels-'__name__') e
           LEFT JOIN _prom_catalog.label l
               ON (l.key = e.key AND l.value = e.value)
        )::prom_api.matcher_positive
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
COMMENT ON FUNCTION prom_api.matcher(jsonb)
IS 'returns a matcher for the JSONB, __name__ is ignored. The matcher can be used to match against a label array using @> or ? operators';
GRANT EXECUTE ON FUNCTION prom_api.matcher(jsonb) TO prom_reader;

---------------- eq functions ------------------

CREATE OR REPLACE FUNCTION prom_api.eq(labels1 prom_api.label_array, labels2 prom_api.label_array)
RETURNS BOOLEAN
AS $func$
    --assumes labels have metric name in position 1 and have no duplicate entries
    SELECT array_length(labels1, 1) = array_length(labels2, 1) AND labels1 @> labels2[2:]
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
COMMENT ON FUNCTION prom_api.eq(prom_api.label_array, prom_api.label_array)
IS 'returns true if two label arrays are equal, ignoring the metric name';
GRANT EXECUTE ON FUNCTION prom_api.eq(prom_api.label_array, prom_api.label_array) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_api.eq(labels1 prom_api.label_array, matchers prom_api.matcher_positive)
RETURNS BOOLEAN
AS $func$
    --assumes no duplicate entries
     SELECT array_length(labels1, 1) = (array_length(matchers, 1) + 1)
            AND labels1 @> matchers
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
COMMENT ON FUNCTION prom_api.eq(prom_api.label_array, prom_api.matcher_positive)
IS 'returns true if the label array and matchers are equal, there should not be a matcher for the metric name';
GRANT EXECUTE ON FUNCTION prom_api.eq(prom_api.label_array, prom_api.matcher_positive) TO prom_reader;

CREATE OR REPLACE FUNCTION prom_api.eq(labels prom_api.label_array, json_labels jsonb)
RETURNS BOOLEAN
AS $func$
    --assumes no duplicate entries
    --do not call eq(label_array, matchers) to allow inlining
     SELECT array_length(labels, 1) = (_prom_catalog.count_jsonb_keys(json_labels-'__name__') + 1)
            AND labels @> prom_api.matcher(json_labels)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
COMMENT ON FUNCTION prom_api.eq(prom_api.label_array, jsonb)
IS 'returns true if the labels and jsonb are equal, ignoring the metric name';
GRANT EXECUTE ON FUNCTION prom_api.eq(prom_api.label_array, jsonb) TO prom_reader;

--------------------- op @> ------------------------

CREATE OR REPLACE FUNCTION _prom_catalog.label_contains(labels prom_api.label_array, json_labels jsonb)
RETURNS BOOLEAN
AS $func$
    SELECT labels @> prom_api.matcher(json_labels)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _prom_catalog.label_contains(prom_api.label_array, jsonb) TO prom_reader;


--------------------- op ? ------------------------

CREATE OR REPLACE FUNCTION _prom_catalog.label_match(labels prom_api.label_array, matchers prom_api.matcher_positive)
RETURNS BOOLEAN
AS $func$
    SELECT labels && matchers
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _prom_catalog.label_match(prom_api.label_array, prom_api.matcher_positive) TO prom_reader;

CREATE OR REPLACE FUNCTION _prom_catalog.label_match(labels prom_api.label_array, matchers prom_api.matcher_negative)
RETURNS BOOLEAN
AS $func$
    SELECT NOT (labels && matchers)
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _prom_catalog.label_match(prom_api.label_array, prom_api.matcher_negative) TO prom_reader;

--------------------- op == !== ==~ !=~ ------------------------

CREATE OR REPLACE FUNCTION _prom_catalog.label_find_key_equal(key_to_match prom_api.label_key, pat prom_api.pattern)
RETURNS prom_api.matcher_positive
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::prom_api.matcher_positive
    FROM _prom_catalog.label l
    WHERE l.key = key_to_match and l.value = pat
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _prom_catalog.label_find_key_equal(prom_api.label_key, prom_api.pattern) TO prom_reader;

CREATE OR REPLACE FUNCTION _prom_catalog.label_find_key_not_equal(key_to_match prom_api.label_key, pat prom_api.pattern)
RETURNS prom_api.matcher_negative
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::prom_api.matcher_negative
    FROM _prom_catalog.label l
    WHERE l.key = key_to_match and l.value = pat
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _prom_catalog.label_find_key_not_equal(prom_api.label_key, prom_api.pattern) TO prom_reader;

CREATE OR REPLACE FUNCTION _prom_catalog.label_find_key_regex(key_to_match prom_api.label_key, pat prom_api.pattern)
RETURNS prom_api.matcher_positive
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::prom_api.matcher_positive
    FROM _prom_catalog.label l
    WHERE l.key = key_to_match and l.value ~ pat
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _prom_catalog.label_find_key_regex(prom_api.label_key, prom_api.pattern) TO prom_reader;

CREATE OR REPLACE FUNCTION _prom_catalog.label_find_key_not_regex(key_to_match prom_api.label_key, pat prom_api.pattern)
RETURNS prom_api.matcher_negative
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::prom_api.matcher_negative
    FROM _prom_catalog.label l
    WHERE l.key = key_to_match and l.value ~ pat
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _prom_catalog.label_find_key_not_regex(prom_api.label_key, prom_api.pattern) TO prom_reader;

--------------------- op == !== ==~ !=~ ------------------------

CREATE OR REPLACE FUNCTION _prom_catalog.match_equals(labels prom_api.label_array, _op ps_tag.tag_op_equals)
RETURNS boolean
AS $func$
    SELECT labels && label_find_key_equal(_op.tag_key, (_op.value#>>'{}'))::int[]
$func$
LANGUAGE SQL STABLE PARALLEL SAFE; -- do not make strict. it disables function inlining
GRANT EXECUTE ON FUNCTION _prom_catalog.match_equals(prom_api.label_array, ps_tag.tag_op_equals) TO prom_reader;

CREATE OR REPLACE FUNCTION _prom_catalog.match_not_equals(labels prom_api.label_array, _op ps_tag.tag_op_not_equals)
RETURNS boolean
AS $func$
    SELECT NOT (labels && label_find_key_not_equal(_op.tag_key, (_op.value#>>'{}'))::int[])
$func$
LANGUAGE SQL STABLE PARALLEL SAFE; -- do not make strict. it disables function inlining
GRANT EXECUTE ON FUNCTION _prom_catalog.match_not_equals(prom_api.label_array, ps_tag.tag_op_not_equals) TO prom_reader;

CREATE OR REPLACE FUNCTION _prom_catalog.match_regexp_matches(labels prom_api.label_array, _op ps_tag.tag_op_regexp_matches)
RETURNS boolean
AS $func$
    SELECT labels && label_find_key_regex(_op.tag_key, _op.value)::int[]
$func$
LANGUAGE SQL STABLE PARALLEL SAFE; -- do not make strict. it disables function inlining
GRANT EXECUTE ON FUNCTION _prom_catalog.match_regexp_matches(prom_api.label_array, ps_tag.tag_op_regexp_matches) TO prom_reader;

CREATE OR REPLACE FUNCTION _prom_catalog.match_regexp_not_matches(labels prom_api.label_array, _op ps_tag.tag_op_regexp_not_matches)
RETURNS boolean
AS $func$
    SELECT NOT (labels && label_find_key_not_regex(_op.tag_key, _op.value)::int[])
$func$
LANGUAGE SQL STABLE PARALLEL SAFE; -- do not make strict. it disables function inlining
GRANT EXECUTE ON FUNCTION _prom_catalog.match_regexp_not_matches(prom_api.label_array, ps_tag.tag_op_regexp_not_matches) TO prom_reader;
