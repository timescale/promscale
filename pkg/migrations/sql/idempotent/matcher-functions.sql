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
COMMENT ON FUNCTION SCHEMA_PROM.matcher(jsonb)
IS 'returns a matcher for the JSONB, __name__ is ignored. The matcher can be used to match against a label array using @> or ? operators';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.matcher(jsonb) TO prom_reader;

---------------- eq functions ------------------

CREATE OR REPLACE FUNCTION SCHEMA_PROM.eq(labels1 SCHEMA_PROM.label_array, labels2 SCHEMA_PROM.label_array)
RETURNS BOOLEAN
AS $func$
    --assumes labels have metric name in position 1 and have no duplicate entries
    SELECT array_length(labels1, 1) = array_length(labels2, 1) AND labels1 @> labels2[2:]
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
COMMENT ON FUNCTION SCHEMA_PROM.eq(SCHEMA_PROM.label_array, SCHEMA_PROM.label_array)
IS 'returns true if two label arrays are equal, ignoring the metric name';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.eq(SCHEMA_PROM.label_array, SCHEMA_PROM.label_array) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.eq(labels1 SCHEMA_PROM.label_array, matchers SCHEMA_PROM.matcher_positive)
RETURNS BOOLEAN
AS $func$
    --assumes no duplicate entries
     SELECT array_length(labels1, 1) = (array_length(matchers, 1) + 1)
            AND labels1 @> matchers
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
COMMENT ON FUNCTION SCHEMA_PROM.eq(SCHEMA_PROM.label_array, SCHEMA_PROM.matcher_positive)
IS 'returns true if the label array and matchers are equal, there should not be a matcher for the metric name';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.eq(SCHEMA_PROM.label_array, SCHEMA_PROM.matcher_positive) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_PROM.eq(labels SCHEMA_PROM.label_array, json_labels jsonb)
RETURNS BOOLEAN
AS $func$
    --assumes no duplicate entries
    --do not call eq(label_array, matchers) to allow inlining
     SELECT array_length(labels, 1) = (SCHEMA_CATALOG.count_jsonb_keys(json_labels-'__name__') + 1)
            AND labels @> SCHEMA_PROM.matcher(json_labels)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
COMMENT ON FUNCTION SCHEMA_PROM.eq(SCHEMA_PROM.label_array, jsonb)
IS 'returns true if the labels and jsonb are equal, ignoring the metric name';
GRANT EXECUTE ON FUNCTION SCHEMA_PROM.eq(SCHEMA_PROM.label_array, jsonb) TO prom_reader;

--------------------- op @> ------------------------

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_contains(labels SCHEMA_PROM.label_array, json_labels jsonb)
RETURNS BOOLEAN
AS $func$
    SELECT labels @> SCHEMA_PROM.matcher(json_labels)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.label_contains(SCHEMA_PROM.label_array, jsonb) TO prom_reader;

--------------------- op ? ------------------------

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_match(labels SCHEMA_PROM.label_array, matchers SCHEMA_PROM.matcher_positive)
RETURNS BOOLEAN
AS $func$
    SELECT labels && matchers
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.label_match(SCHEMA_PROM.label_array, SCHEMA_PROM.matcher_positive) TO prom_reader;


CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_match(labels SCHEMA_PROM.label_array, matchers SCHEMA_PROM.matcher_negative)
RETURNS BOOLEAN
AS $func$
    SELECT NOT (labels && matchers)
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.label_match(SCHEMA_PROM.label_array, SCHEMA_PROM.matcher_negative) TO prom_reader;

--------------------- op == !== ==~ !=~ ------------------------

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_find_key_equal(key_to_match SCHEMA_PROM.label_key, pat SCHEMA_PROM.pattern)
RETURNS SCHEMA_PROM.matcher_positive
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::SCHEMA_PROM.matcher_positive
    FROM SCHEMA_CATALOG.label l
    WHERE l.key = key_to_match and l.value = pat
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.label_find_key_equal(SCHEMA_PROM.label_key, SCHEMA_PROM.pattern) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_find_key_not_equal(key_to_match SCHEMA_PROM.label_key, pat SCHEMA_PROM.pattern)
RETURNS SCHEMA_PROM.matcher_negative
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::SCHEMA_PROM.matcher_negative
    FROM SCHEMA_CATALOG.label l
    WHERE l.key = key_to_match and l.value = pat
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.label_find_key_not_equal(SCHEMA_PROM.label_key, SCHEMA_PROM.pattern) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_find_key_regex(key_to_match SCHEMA_PROM.label_key, pat SCHEMA_PROM.pattern)
RETURNS SCHEMA_PROM.matcher_positive
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::SCHEMA_PROM.matcher_positive
    FROM SCHEMA_CATALOG.label l
    WHERE l.key = key_to_match and l.value ~ pat
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.label_find_key_regex(SCHEMA_PROM.label_key, SCHEMA_PROM.pattern) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_CATALOG.label_find_key_not_regex(key_to_match SCHEMA_PROM.label_key, pat SCHEMA_PROM.pattern)
RETURNS SCHEMA_PROM.matcher_negative
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::SCHEMA_PROM.matcher_negative
    FROM SCHEMA_CATALOG.label l
    WHERE l.key = key_to_match and l.value ~ pat
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_CATALOG.label_find_key_not_regex(SCHEMA_PROM.label_key, SCHEMA_PROM.pattern) TO prom_reader;