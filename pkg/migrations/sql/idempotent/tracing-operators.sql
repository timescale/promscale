/*

A tag_map is a domain over jsonb. A tag_map is a jsonb object. The
keys in the object are ids that point to tag_key.id. The values in
the object are ids that point to tag.id.

A tag_maps is a domain over jsonb. A tag_maps is a jsonb array in
which each element is a tag_map.

Example:

select *
from span
where tags ? ('service.name' == 'bob')
;

Each of the ? operators are intended to be used as shown in the
example above. Each ? operator requires two functions to implement
the feature. One "eval" function takes the operation described in
the parentheses -- ('service.name' == 'bob') -- and returns matches
from the tag table. The "match" function then matches the results
against the tags on the left side.

The functionality needs to be divided between the two functions in
order to get PostgreSQL to inline the part the finds the matches from
the tag table. We only want that query to run once per query.

List of operators:
 Name | Left arg type |        Right arg type        | Result type
------+---------------+------------------------------+-------------
 #    | tag_map       | tag_k                        | jsonb
 #    | tag_maps      | tag_k                        | jsonb
 #?   | tag_map       | tag_k                        | boolean
 #?   | tag_maps      | tag_k                        | boolean
 ->   | tag_maps      | integer                      | tag_map
 ?    | tag_map       | tag_op_equals                | boolean
 ?    | tag_map       | tag_op_greater_than          | boolean
 ?    | tag_map       | tag_op_greater_than_or_equal | boolean
 ?    | tag_map       | tag_op_jsonb_path_exists     | boolean
 ?    | tag_map       | tag_op_less_than             | boolean
 ?    | tag_map       | tag_op_less_than_or_equal    | boolean
 ?    | tag_map       | tag_op_not_equals            | boolean
 ?    | tag_map       | tag_op_regexp_matches        | boolean
 ?    | tag_map       | tag_op_regexp_not_matches    | boolean
 ?    | tag_maps      | tag_op_equals                | boolean
 ?    | tag_maps      | tag_op_greater_than          | boolean
 ?    | tag_maps      | tag_op_greater_than_or_equal | boolean
 ?    | tag_maps      | tag_op_jsonb_path_exists     | boolean
 ?    | tag_maps      | tag_op_less_than             | boolean
 ?    | tag_maps      | tag_op_less_than_or_equal    | boolean
 ?    | tag_maps      | tag_op_not_equals            | boolean
 ?    | tag_maps      | tag_op_regexp_matches        | boolean
 ?    | tag_maps      | tag_op_regexp_not_matches    | boolean


List of functions:
                 Name                 | Result data type |                 Argument data types
--------------------------------------+------------------+------------------------------------------------------
 tag_map_eval_equals                  | jsonb            | _op tag_op_equals
 tag_map_eval_greater_than            | jsonb[]          | _op tag_op_greater_than
 tag_map_eval_greater_than_or_equal   | jsonb[]          | _op tag_op_greater_than_or_equal
 tag_map_eval_jsonb_path_exists       | jsonb[]          | _op tag_op_jsonb_path_exists
 tag_map_eval_less_than               | jsonb[]          | _op tag_op_less_than
 tag_map_eval_less_than_or_equal      | jsonb[]          | _op tag_op_less_than_or_equal
 tag_map_eval_not_equals              | jsonb[]          | _op tag_op_not_equals
 tag_map_eval_regexp_matches          | jsonb[]          | _op tag_op_regexp_matches
 tag_map_eval_regexp_not_matches      | jsonb[]          | _op tag_op_regexp_not_matches
 tag_map_eval_tags_by_key             | jsonb[]          | _key tag_k
 tag_map_get_tag_id                   | jsonb            | _tag_map tag_map, _key tag_k
 tag_map_has_tag                      | boolean          | _tag_map tag_map, _key tag_k
 tag_map_match_equals                 | boolean          | _tag_map tag_map, _op tag_op_equals
 tag_map_match_greater_than           | boolean          | _tag_map tag_map, _op tag_op_greater_than
 tag_map_match_greater_than_or_equal  | boolean          | _tag_map tag_map, _op tag_op_greater_than_or_equal
 tag_map_match_jsonb_path_exists      | boolean          | _tag_map tag_map, _op tag_op_jsonb_path_exists
 tag_map_match_less_than              | boolean          | _tag_map tag_map, _op tag_op_less_than
 tag_map_match_less_than_or_equal     | boolean          | _tag_map tag_map, _op tag_op_less_than_or_equal
 tag_map_match_not_equals             | boolean          | _tag_map tag_map, _op tag_op_not_equals
 tag_map_match_regexp_matches         | boolean          | _tag_map tag_map, _op tag_op_regexp_matches
 tag_map_match_regexp_not_matches     | boolean          | _tag_map tag_map, _op tag_op_regexp_not_matches
 tag_maps_element                     | tag_map          | _tag_maps tag_maps, _type integer
 tag_maps_eval_equals                 | jsonb            | _op tag_op_equals
 tag_maps_eval_greater_than           | jsonb[]          | _op tag_op_greater_than
 tag_maps_eval_greater_than_or_equal  | jsonb[]          | _op tag_op_greater_than_or_equal
 tag_maps_eval_jsonb_path_exists      | jsonb[]          | _op tag_op_jsonb_path_exists
 tag_maps_eval_less_than              | jsonb[]          | _op tag_op_less_than
 tag_maps_eval_less_than_or_equal     | jsonb[]          | _op tag_op_less_than_or_equal
 tag_maps_eval_not_equals             | jsonb[]          | _op tag_op_not_equals
 tag_maps_eval_regexp_matches         | jsonb[]          | _op tag_op_regexp_matches
 tag_maps_eval_regexp_not_matches     | jsonb[]          | _op tag_op_regexp_not_matches
 tag_maps_eval_tags_by_key            | jsonb[]          | _key tag_k
 tag_maps_get_tag_id                  | jsonb            | _tag_maps tag_maps, _key tag_k
 tag_maps_has_tag                     | boolean          | _tag_maps tag_maps, _key tag_k
 tag_maps_match_equals                | boolean          | _tag_maps tag_maps, _op tag_op_equals
 tag_maps_match_greater_than          | boolean          | _tag_maps tag_maps, _op tag_op_greater_than
 tag_maps_match_greater_than_or_equal | boolean          | _tag_maps tag_maps, _op tag_op_greater_than_or_equal
 tag_maps_match_jsonb_path_exists     | boolean          | _tag_maps tag_maps, _op tag_op_jsonb_path_exists
 tag_maps_match_less_than             | boolean          | _tag_maps tag_maps, _op tag_op_less_than
 tag_maps_match_less_than_or_equal    | boolean          | _tag_maps tag_maps, _op tag_op_less_than_or_equal
 tag_maps_match_not_equals            | boolean          | _tag_maps tag_maps, _op tag_op_not_equals
 tag_maps_match_regexp_matches        | boolean          | _tag_maps tag_maps, _op tag_op_regexp_matches
 tag_maps_match_regexp_not_matches    | boolean          | _tag_maps tag_maps, _op tag_op_regexp_not_matches

*/


-- tag map get tag id

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_map_get_tag_id(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _key SCHEMA_TRACING_PUBLIC.tag_k)
RETURNS jsonb
AS $func$
    SELECT _tag_map->(SELECT k.id::text from _ps_trace.tag_key k WHERE k.key = _key LIMIT 1)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_map_get_tag_id(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TRACING_PUBLIC.tag_k) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_map_get_tag_id IS $$This function supports the # operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.# (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
        RIGHTARG = SCHEMA_TRACING_PUBLIC.tag_k,
        FUNCTION = SCHEMA_TRACING.tag_map_get_tag_id
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.# (SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TRACING_PUBLIC.tag_k)
IS 'Returns the tag.id from the tag_map corresponding to the tag key provided';

-- tag map has tag

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_map_eval_tags_by_key(_key SCHEMA_TRACING_PUBLIC.tag_k)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), array[]::jsonb[])
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _key
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_map_eval_tags_by_key(SCHEMA_TRACING_PUBLIC.tag_k) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_map_has_tag(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _key SCHEMA_TRACING_PUBLIC.tag_k)
RETURNS boolean
AS $func$
    SELECT _tag_map @> ANY(SCHEMA_TRACING.tag_map_eval_tags_by_key(_key))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_map_has_tag(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TRACING_PUBLIC.tag_k) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_map_has_tag IS $$This function supports the #? operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.#? (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
        RIGHTARG = SCHEMA_TRACING_PUBLIC.tag_k,
        FUNCTION = SCHEMA_TRACING.tag_map_has_tag
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.#? (SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TRACING_PUBLIC.tag_k)
IS 'Returns true if the tag_map contains the tag key provided';

-- tag map json path exists

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_map_eval_jsonb_path_exists(_op SCHEMA_TAG.tag_op_jsonb_path_exists)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), array[]::jsonb[])
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _op.tag_key
    AND jsonb_path_exists(a.value, _op.value)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_map_eval_jsonb_path_exists(SCHEMA_TAG.tag_op_jsonb_path_exists) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_map_match_jsonb_path_exists(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _op SCHEMA_TAG.tag_op_jsonb_path_exists)
RETURNS boolean
AS $func$
    SELECT _tag_map @> ANY(SCHEMA_TRACING.tag_map_eval_jsonb_path_exists(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_map_match_jsonb_path_exists(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_jsonb_path_exists) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_map_match_jsonb_path_exists IS $$This function supports the @? operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
        RIGHTARG = SCHEMA_TAG.tag_op_jsonb_path_exists,
        FUNCTION = SCHEMA_TRACING.tag_map_match_jsonb_path_exists
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.? (SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_jsonb_path_exists)
IS 'Returns true if the tag_map contains a key value pair matching the tag_op_jsonb_path_exists';

-- tag map regexp matches

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_map_eval_regexp_matches(_op SCHEMA_TAG.tag_op_regexp_matches)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), array[]::jsonb[])
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _op.tag_key
    -- if the jsonb value is a string, apply the regex directly
    -- otherwise, convert the value to a text representation, back to a jsonb string, and then apply
    AND CASE jsonb_typeof(a.value)
        WHEN 'string' THEN jsonb_path_exists(a.value, format('$?(@ like_regex "%s")', _op.value)::jsonpath)
        ELSE jsonb_path_exists(to_jsonb(a.value#>>'{}'), format('$?(@ like_regex "%s")', _op.value)::jsonpath)
    END
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_map_eval_regexp_matches(SCHEMA_TAG.tag_op_regexp_matches) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_map_match_regexp_matches(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _op SCHEMA_TAG.tag_op_regexp_matches)
RETURNS boolean
AS $func$
    SELECT _tag_map @> ANY(SCHEMA_TRACING.tag_map_eval_regexp_matches(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_map_match_regexp_matches(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_regexp_matches) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_map_match_regexp_matches IS $$This function supports the ==~ operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
        RIGHTARG = SCHEMA_TAG.tag_op_regexp_matches,
        FUNCTION = SCHEMA_TRACING.tag_map_match_regexp_matches
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.? (SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_regexp_matches)
IS 'Returns true if the tag_map contains a key value pair matching the tag_op_regexp_matches';

-- tag map regexp not matches

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_map_eval_regexp_not_matches(_op SCHEMA_TAG.tag_op_regexp_not_matches)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), array[]::jsonb[])
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _op.tag_key
    -- if the jsonb value is a string, apply the regex directly
    -- otherwise, convert the value to a text representation, back to a jsonb string, and then apply
    AND CASE jsonb_typeof(a.value)
        WHEN 'string' THEN jsonb_path_exists(a.value, format('$?(!(@ like_regex "%s"))', _op.value)::jsonpath)
        ELSE jsonb_path_exists(to_jsonb(a.value#>>'{}'), format('$?(!(@ like_regex "%s"))', _op.value)::jsonpath)
    END
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_map_eval_regexp_not_matches(SCHEMA_TAG.tag_op_regexp_not_matches) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_map_match_regexp_not_matches(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _op SCHEMA_TAG.tag_op_regexp_not_matches)
RETURNS boolean
AS $func$
    SELECT _tag_map @> ANY(SCHEMA_TRACING.tag_map_eval_regexp_not_matches(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_map_match_regexp_not_matches(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_regexp_not_matches) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_map_match_regexp_not_matches IS $$This function supports the !=~ operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
        RIGHTARG = SCHEMA_TAG.tag_op_regexp_not_matches,
        FUNCTION = SCHEMA_TRACING.tag_map_match_regexp_not_matches
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.? (SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_regexp_not_matches)
IS 'Returns true if the tag_map contains a key value pair matching the tag_op_regexp_not_matches';

-- tag map equals

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_map_eval_equals(_op SCHEMA_TAG.tag_op_equals)
RETURNS jsonb
AS $func$
    SELECT jsonb_build_object(a.key_id, a.id)
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _op.tag_key
    AND a.value = _op.value
    LIMIT 1
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_map_eval_equals(SCHEMA_TAG.tag_op_equals) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_map_match_equals(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _op SCHEMA_TAG.tag_op_equals)
RETURNS boolean
AS $func$
    SELECT _tag_map @> (SCHEMA_TRACING.tag_map_eval_equals(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_map_match_equals(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_equals) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_map_match_equals IS $$This function supports the == operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
        RIGHTARG = SCHEMA_TAG.tag_op_equals,
        FUNCTION = SCHEMA_TRACING.tag_map_match_equals
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.? (SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_equals)
IS 'Returns true if the tag_map contains a key value pair matching the tag_op_equals';

-- tag map not equals

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_map_eval_not_equals(_op SCHEMA_TAG.tag_op_not_equals)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), array[]::jsonb[])
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _op.tag_key
    AND a.value != _op.value
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_map_eval_not_equals(SCHEMA_TAG.tag_op_not_equals) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_map_match_not_equals(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _op SCHEMA_TAG.tag_op_not_equals)
RETURNS boolean
AS $func$
    SELECT _tag_map @> ANY(SCHEMA_TRACING.tag_map_eval_not_equals(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_map_match_not_equals(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_not_equals) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_map_match_not_equals IS $$This function supports the !== operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
        RIGHTARG = SCHEMA_TAG.tag_op_not_equals,
        FUNCTION = SCHEMA_TRACING.tag_map_match_not_equals
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.? (SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_not_equals)
IS 'Returns true if the tag_map contains a key value pair matching the tag_op_not_equals';

-- tag map less than

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_map_eval_less_than(_op SCHEMA_TAG.tag_op_less_than)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), array[]::jsonb[])
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _op.tag_key
    AND jsonb_path_exists(a.value, '$?(@ < $x)'::jsonpath, jsonb_build_object('x', _op.value))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_map_eval_less_than(SCHEMA_TAG.tag_op_less_than) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_map_match_less_than(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _op SCHEMA_TAG.tag_op_less_than)
RETURNS boolean
AS $func$
    SELECT _tag_map @> ANY(SCHEMA_TRACING.tag_map_eval_less_than(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_map_match_less_than(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_less_than) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_map_match_less_than IS $$This function supports the #< operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
        RIGHTARG = SCHEMA_TAG.tag_op_less_than,
        FUNCTION = SCHEMA_TRACING.tag_map_match_less_than
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.? (SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_less_than)
IS 'Returns true if the tag_map contains a key value pair matching the tag_op_less_than';

-- tag map less than or equal

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_map_eval_less_than_or_equal(_op SCHEMA_TAG.tag_op_less_than_or_equal)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), array[]::jsonb[])
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _op.tag_key
    AND jsonb_path_exists(a.value, '$?(@ <= $x)'::jsonpath, jsonb_build_object('x', _op.value))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_map_eval_less_than_or_equal(SCHEMA_TAG.tag_op_less_than_or_equal) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_map_match_less_than_or_equal(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _op SCHEMA_TAG.tag_op_less_than_or_equal)
RETURNS boolean
AS $func$
    SELECT _tag_map @> ANY(SCHEMA_TRACING.tag_map_eval_less_than_or_equal(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_map_match_less_than_or_equal(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_less_than_or_equal) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_map_match_less_than_or_equal IS $$This function supports the #<= operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
        RIGHTARG = SCHEMA_TAG.tag_op_less_than_or_equal,
        FUNCTION = SCHEMA_TRACING.tag_map_match_less_than_or_equal
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.? (SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_less_than_or_equal)
IS 'Returns true if the tag_map contains a key value pair matching the tag_op_less_than_or_equal';

-- tag map greater than

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_map_eval_greater_than(_op SCHEMA_TAG.tag_op_greater_than)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), array[]::jsonb[])
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _op.tag_key
    AND jsonb_path_exists(a.value, '$?(@ > $x)'::jsonpath, jsonb_build_object('x', _op.value))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_map_eval_greater_than(SCHEMA_TAG.tag_op_greater_than) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_map_match_greater_than(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _op SCHEMA_TAG.tag_op_greater_than)
RETURNS boolean
AS $func$
    SELECT _tag_map @> ANY(SCHEMA_TRACING.tag_map_eval_greater_than(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_map_match_greater_than(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_greater_than) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_map_match_greater_than IS $$This function supports the #> operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
        RIGHTARG = SCHEMA_TAG.tag_op_greater_than,
        FUNCTION = SCHEMA_TRACING.tag_map_match_greater_than
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.? (SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_greater_than)
IS 'Returns true if the tag_map contains a key value pair matching the tag_op_greater_than';

-- tag map greater than or equal

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_map_eval_greater_than_or_equal(_op SCHEMA_TAG.tag_op_greater_than_or_equal)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), array[]::jsonb[])
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _op.tag_key
    AND jsonb_path_exists(a.value, '$?(@ >= $x)'::jsonpath, jsonb_build_object('x', _op.value))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_map_eval_greater_than_or_equal(SCHEMA_TAG.tag_op_greater_than_or_equal) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_map_match_greater_than_or_equal(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _op SCHEMA_TAG.tag_op_greater_than_or_equal)
RETURNS boolean
AS $func$
    SELECT _tag_map @> ANY(SCHEMA_TRACING.tag_map_eval_greater_than_or_equal(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_map_match_greater_than_or_equal(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_greater_than_or_equal) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_map_match_greater_than_or_equal IS $$This function supports the #>= operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
        RIGHTARG = SCHEMA_TAG.tag_op_greater_than_or_equal,
        FUNCTION = SCHEMA_TRACING.tag_map_match_greater_than_or_equal
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.? (SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_greater_than_or_equal)
IS 'Returns true if the tag_map contains a key value pair matching the tag_op_greater_than_or_equal';


-------------------------------------------------------------------------------


-- tag maps element ->

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_element(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _type int)
RETURNS SCHEMA_TRACING_PUBLIC.tag_map
AS $func$
    SELECT (_tag_maps->_type)::SCHEMA_TRACING_PUBLIC.tag_map
$func$
LANGUAGE sql STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_element(SCHEMA_TRACING_PUBLIC.tag_maps, int) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_maps_element IS $$This function supports the -> operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.-> (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
        RIGHTARG = int,
        FUNCTION = SCHEMA_TRACING.tag_maps_element
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.-> (SCHEMA_TRACING_PUBLIC.tag_maps, int)
IS 'Returns the tag_map from the array at the index specified';

-- tag maps get tag id #

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_get_tag_id(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _key SCHEMA_TRACING_PUBLIC.tag_k)
RETURNS jsonb
AS $func$
    SELECT jsonb_path_query_first
    (   _tag_maps,
        $qry$  $[*].keyvalue() ? (@ == $key) .value  $qry$,
        jsonb_build_object('key', (SELECT k.id::text from _ps_trace.tag_key k WHERE k.key = _key LIMIT 1))
    )
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_get_tag_id(SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TRACING_PUBLIC.tag_k) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_maps_get_tag_id IS $$This function supports the # operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.# (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
        RIGHTARG = SCHEMA_TRACING_PUBLIC.tag_k,
        FUNCTION = SCHEMA_TRACING.tag_maps_get_tag_id
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.# (SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TRACING_PUBLIC.tag_k)
IS 'Returns the first tag.id found (if any) from any of the tag_maps in the tag_maps corresponding to the tag key provided';

-- tag maps has tag #?

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_eval_tags_by_key(_key SCHEMA_TRACING_PUBLIC.tag_k)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_array(jsonb_build_object(a.key_id, a.id))), array[]::jsonb[])
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _key
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_eval_tags_by_key(SCHEMA_TRACING_PUBLIC.tag_k) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_has_tag(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _key SCHEMA_TRACING_PUBLIC.tag_k)
RETURNS boolean
AS $func$
    SELECT _tag_maps @> ANY(SCHEMA_TRACING.tag_maps_eval_tags_by_key(_key))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_has_tag(SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TRACING_PUBLIC.tag_k) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_maps_has_tag IS $$This function supports the #? operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.#? (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
        RIGHTARG = SCHEMA_TRACING_PUBLIC.tag_k,
        FUNCTION = SCHEMA_TRACING.tag_maps_has_tag
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.#? (SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TRACING_PUBLIC.tag_k)
IS 'Returns true if any tag_map in the tag_maps contains the tag key provided';

-- tag maps match jsonb path exists ?

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_eval_jsonb_path_exists(_op SCHEMA_TAG.tag_op_jsonb_path_exists)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_array(jsonb_build_object(a.key_id, a.id))), array[]::jsonb[])
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _op.tag_key
    AND jsonb_path_exists(a.value, _op.value)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_eval_jsonb_path_exists(SCHEMA_TAG.tag_op_jsonb_path_exists) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_match_jsonb_path_exists(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _op SCHEMA_TAG.tag_op_jsonb_path_exists)
RETURNS boolean
AS $func$
    SELECT _tag_maps @> ANY(SCHEMA_TRACING.tag_maps_eval_jsonb_path_exists(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_match_jsonb_path_exists(SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAG.tag_op_jsonb_path_exists) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_maps_match_jsonb_path_exists IS $$This function supports the ? operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
        RIGHTARG = SCHEMA_TAG.tag_op_jsonb_path_exists,
        FUNCTION = SCHEMA_TRACING.tag_maps_match_jsonb_path_exists
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.? (SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAG.tag_op_jsonb_path_exists)
IS 'Returns true if any tag_map in the tag_maps contains a key value pair matching the tag_op_jsonb_path_exists';

-- tag maps match regexp matches ?

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_eval_regexp_matches(_op SCHEMA_TAG.tag_op_regexp_matches)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_array(jsonb_build_object(a.key_id, a.id))), array[]::jsonb[])
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _op.tag_key
    -- if the jsonb value is a string, apply the regex directly
    -- otherwise, convert the value to a text representation, back to a jsonb string, and then apply
    AND CASE jsonb_typeof(a.value)
        WHEN 'string' THEN jsonb_path_exists(a.value, format('$?(@ like_regex "%s")', _op.value)::jsonpath)
        ELSE jsonb_path_exists(to_jsonb(a.value#>>'{}'), format('$?(@ like_regex "%s")', _op.value)::jsonpath)
    END
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_eval_regexp_matches(SCHEMA_TAG.tag_op_regexp_matches) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_match_regexp_matches(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _op SCHEMA_TAG.tag_op_regexp_matches)
RETURNS boolean
AS $func$
    SELECT _tag_maps @> ANY(SCHEMA_TRACING.tag_maps_eval_regexp_matches(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_match_regexp_matches(SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAG.tag_op_regexp_matches) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_maps_match_regexp_matches IS $$This function supports the ==~ operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
        RIGHTARG = SCHEMA_TAG.tag_op_regexp_matches,
        FUNCTION = SCHEMA_TRACING.tag_maps_match_regexp_matches
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.? (SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAG.tag_op_regexp_matches)
IS 'Returns true if any tag_map in the tag_maps contains a key value pair matching the tag_op_regexp_matches';

-- tag maps match regexp not matches ?

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_eval_regexp_not_matches(_op SCHEMA_TAG.tag_op_regexp_not_matches)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_array(jsonb_build_object(a.key_id, a.id))), array[]::jsonb[])
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _op.tag_key
    -- if the jsonb value is a string, apply the regex directly
    -- otherwise, convert the value to a text representation, back to a jsonb string, and then apply
    AND CASE jsonb_typeof(a.value)
        WHEN 'string' THEN jsonb_path_exists(a.value, format('$?(!(@ like_regex "%s"))', _op.value)::jsonpath)
        ELSE jsonb_path_exists(to_jsonb(a.value#>>'{}'), format('$?(!(@ like_regex "%s"))', _op.value)::jsonpath)
    END
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_eval_regexp_not_matches(SCHEMA_TAG.tag_op_regexp_not_matches) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_match_regexp_not_matches(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _op SCHEMA_TAG.tag_op_regexp_not_matches)
RETURNS boolean
AS $func$
    SELECT _tag_maps @> ANY(SCHEMA_TRACING.tag_maps_eval_regexp_not_matches(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_match_regexp_not_matches(SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAG.tag_op_regexp_not_matches) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_maps_match_regexp_not_matches IS $$This function supports the !=~ operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
        RIGHTARG = SCHEMA_TAG.tag_op_regexp_not_matches,
        FUNCTION = SCHEMA_TRACING.tag_maps_match_regexp_not_matches
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.? (SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAG.tag_op_regexp_not_matches)
IS 'Returns true if any tag_map in the tag_maps contains a key value pair matching the tag_op_regexp_not_matches';

-- tag maps match equals ?

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_eval_equals(_op SCHEMA_TAG.tag_op_equals)
RETURNS jsonb
AS $func$
    SELECT jsonb_build_array(jsonb_build_object(a.key_id, a.id))
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _op.tag_key
    AND a.value = _op.value
    LIMIT 1
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_eval_equals(SCHEMA_TAG.tag_op_equals) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_match_equals(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _op SCHEMA_TAG.tag_op_equals)
RETURNS boolean
AS $func$
    SELECT _tag_maps @> (SCHEMA_TRACING.tag_maps_eval_equals(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_match_equals(SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAG.tag_op_equals) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_maps_match_equals IS $$This function supports the == operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
        RIGHTARG = SCHEMA_TAG.tag_op_equals,
        FUNCTION = SCHEMA_TRACING.tag_maps_match_equals
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.? (SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAG.tag_op_equals)
IS 'Returns true if any tag_map in the tag_maps contains a key value pair matching the tag_op_equals';

-- tag maps match not equals ?

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_eval_not_equals(_op SCHEMA_TAG.tag_op_not_equals)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_array(jsonb_build_object(a.key_id, a.id))), array[]::jsonb[])
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _op.tag_key
    AND a.value != _op.value
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_eval_not_equals(SCHEMA_TAG.tag_op_not_equals) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_match_not_equals(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _op SCHEMA_TAG.tag_op_not_equals)
RETURNS boolean
AS $func$
    SELECT _tag_maps @> ANY(SCHEMA_TRACING.tag_maps_eval_not_equals(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_match_not_equals(SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAG.tag_op_not_equals) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_maps_match_not_equals IS $$This function supports the !== operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
        RIGHTARG = SCHEMA_TAG.tag_op_not_equals,
        FUNCTION = SCHEMA_TRACING.tag_maps_match_not_equals
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.? (SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAG.tag_op_not_equals)
IS 'Returns true if any tag_map in the tag_maps contains a key value pair matching the tag_op_not_equals';

-- tag maps match less than ?

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_eval_less_than(_op SCHEMA_TAG.tag_op_less_than)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_array(jsonb_build_object(a.key_id, a.id))), array[]::jsonb[])
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _op.tag_key
    AND jsonb_path_exists(a.value, '$?(@ < $x)'::jsonpath, jsonb_build_object('x', _op.value))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_eval_less_than(SCHEMA_TAG.tag_op_less_than) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_match_less_than(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _op SCHEMA_TAG.tag_op_less_than)
RETURNS boolean
AS $func$
    SELECT _tag_maps @> ANY(SCHEMA_TRACING.tag_maps_eval_less_than(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_match_less_than(SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAG.tag_op_less_than) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_maps_match_less_than IS $$This function supports the #< operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
        RIGHTARG = SCHEMA_TAG.tag_op_less_than,
        FUNCTION = SCHEMA_TRACING.tag_maps_match_less_than
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.? (SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAG.tag_op_less_than)
IS 'Returns true if any tag_map in the tag_maps contains a key value pair matching the tag_op_less_than';

-- tag maps match less than or equal ?

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_eval_less_than_or_equal(_op SCHEMA_TAG.tag_op_less_than_or_equal)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_array(jsonb_build_object(a.key_id, a.id))), array[]::jsonb[])
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _op.tag_key
    AND jsonb_path_exists(a.value, '$?(@ <= $x)'::jsonpath, jsonb_build_object('x', _op.value))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_eval_less_than_or_equal(SCHEMA_TAG.tag_op_less_than_or_equal) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_match_less_than_or_equal(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _op SCHEMA_TAG.tag_op_less_than_or_equal)
RETURNS boolean
AS $func$
    SELECT _tag_maps @> ANY(SCHEMA_TRACING.tag_maps_eval_less_than_or_equal(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_match_less_than_or_equal(SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAG.tag_op_less_than_or_equal) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_maps_match_less_than_or_equal IS $$This function supports the #<= operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
        RIGHTARG = SCHEMA_TAG.tag_op_less_than_or_equal,
        FUNCTION = SCHEMA_TRACING.tag_maps_match_less_than_or_equal
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.? (SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAG.tag_op_less_than_or_equal)
IS 'Returns true if any tag_map in the tag_maps contains a key value pair matching the tag_op_less_than_or_equal';

-- tag maps match greater than ?

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_eval_greater_than(_op SCHEMA_TAG.tag_op_greater_than)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_array(jsonb_build_object(a.key_id, a.id))), array[]::jsonb[])
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _op.tag_key
    AND jsonb_path_exists(a.value, '$?(@ > $x)'::jsonpath, jsonb_build_object('x', _op.value))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_eval_greater_than(SCHEMA_TAG.tag_op_greater_than) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_match_greater_than(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _op SCHEMA_TAG.tag_op_greater_than)
RETURNS boolean
AS $func$
    SELECT _tag_maps @> ANY(SCHEMA_TRACING.tag_maps_eval_greater_than(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_match_greater_than(SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAG.tag_op_greater_than) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_maps_match_greater_than IS $$This function supports the #> operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
        RIGHTARG = SCHEMA_TAG.tag_op_greater_than,
        FUNCTION = SCHEMA_TRACING.tag_maps_match_greater_than
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.? (SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAG.tag_op_greater_than)
IS 'Returns true if any tag_map in the tag_maps contains a key value pair matching the tag_op_greater_than';

-- tag maps match greater than or equal ?

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_eval_greater_than_or_equal(_op SCHEMA_TAG.tag_op_greater_than_or_equal)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_array(jsonb_build_object(a.key_id, a.id))), array[]::jsonb[])
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _op.tag_key
    AND jsonb_path_exists(a.value, '$?(@ >= $x)'::jsonpath, jsonb_build_object('x', _op.value))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_eval_greater_than_or_equal(SCHEMA_TAG.tag_op_greater_than_or_equal) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_match_greater_than_or_equal(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _op SCHEMA_TAG.tag_op_greater_than_or_equal)
RETURNS boolean
AS $func$
    SELECT _tag_maps @> ANY(SCHEMA_TRACING.tag_maps_eval_greater_than_or_equal(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_match_greater_than_or_equal(SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAG.tag_op_greater_than_or_equal) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_maps_match_greater_than_or_equal IS $$This function supports the #>= operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
        RIGHTARG = SCHEMA_TAG.tag_op_greater_than_or_equal,
        FUNCTION = SCHEMA_TRACING.tag_maps_match_greater_than_or_equal
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.? (SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAG.tag_op_greater_than_or_equal)
IS 'Returns true if any tag_map in the tag_maps contains a key value pair matching the tag_op_greater_than_or_equal';

