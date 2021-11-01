

-- tag maps get tag id

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.tag_maps_get_tag_id(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _key text)
RETURNS bigint
AS $func$
    SELECT jsonb_path_query_first(
        _tag_maps,
        $qry$  $[*].keyvalue() ? (@ == $key) .value  $qry$,
        jsonb_build_object('key', (SELECT k.id::text from _ps_trace.tag_key k WHERE k.key = _key LIMIT 1))
    )::bigint
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.tag_maps_get_tag_id(SCHEMA_TRACING_PUBLIC.tag_maps, text) TO prom_reader;
--COMMENT ON FUNCTION SCHEMA_TRACING_PUBLIC.tag_maps_get_tag_id IS $$This function supports the # operator.$$;

/*
DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.# (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
        RIGHTARG = text,
        FUNCTION = SCHEMA_TRACING.tag_maps_get_tag_id
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.# (SCHEMA_TRACING_PUBLIC.tag_maps, text)
IS 'Returns the first tag.id found (if any) from any of the tag_maps in the tag_maps corresponding to the tag key provided';
*/

-- tag maps has tag

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_eval_tags_by_key(_key text)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_array(jsonb_build_object(a.key_id, a.id))), array[]::jsonb[])
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _key
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_eval_tags_by_key(text) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.tag_maps_has_tag(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _key text)
RETURNS boolean
AS $func$
    SELECT _tag_maps @> ANY(SCHEMA_TRACING.tag_maps_eval_tags_by_key(_key))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.tag_maps_has_tag(SCHEMA_TRACING_PUBLIC.tag_maps, text) TO prom_reader;
--COMMENT ON FUNCTION SCHEMA_TRACING_PUBLIC.tag_maps_has_tag IS $$This function supports the #? operator.$$;

/*
DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.#? (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
        RIGHTARG = text,
        FUNCTION = SCHEMA_TRACING.tag_maps_has_tag
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.#? (SCHEMA_TRACING_PUBLIC.tag_maps, text)
IS 'Returns true if any tag_map in the tag_maps contains the tag key provided';
*/

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

-- tag maps equals

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

-- tag maps not equals

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

-- tag maps less than or equal

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

-- tag maps greater than

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

-- tag maps greater than or equal

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

-- tag maps val

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_val(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _key text)
RETURNS SCHEMA_TRACING_PUBLIC.tag_v
AS $func$
    SELECT a.value
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _key -- partition elimination
    AND a.id = (
        jsonb_path_query_first
        (   _tag_maps,
            '$[*].keyvalue() ? (@.key == $key) .value',
            jsonb_build_object('key', (SELECT id::text FROM SCHEMA_TRACING.tag_key WHERE key = _key))
        )
    )::bigint
    LIMIT 1
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_maps_val(SCHEMA_TRACING_PUBLIC.tag_maps, text) TO prom_reader;

/*
DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.-> (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
        RIGHTARG = text,
        FUNCTION = SCHEMA_TRACING.tag_maps_val
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.-> (SCHEMA_TRACING_PUBLIC.tag_maps, text)
IS 'Returns the tag value associated with the tag_key provided';
*/

-- tag maps val text

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.tag_maps_val_text(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _key text)
RETURNS text
AS $func$
    SELECT a.value#>>'{}'
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _key -- partition elimination
    AND a.id = (
        jsonb_path_query_first
        (   _tag_maps,
            '$[*].keyvalue() ? (@.key == $key) .value',
            jsonb_build_object('key', (SELECT id::text FROM SCHEMA_TRACING.tag_key WHERE key = _key))
        )
    )::bigint
    LIMIT 1
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.tag_maps_val_text(SCHEMA_TRACING_PUBLIC.tag_maps, text) TO prom_reader;

/*
DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TRACING_PUBLIC.->> (
        LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
        RIGHTARG = text,
        FUNCTION = SCHEMA_TRACING.tag_maps_val_text
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
COMMENT ON OPERATOR SCHEMA_TRACING_PUBLIC.->> (SCHEMA_TRACING_PUBLIC.tag_maps, text)
IS 'Returns the tag value associated with the tag_key provided as text';
*/

-- tag maps jsonb

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.tag_maps_jsonb(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps)
RETURNS jsonb
AS $func$
    /*
    takes an tag_map which is an array of maps of tag_key.id to tag.id
    and returns a jsonb object containing the key value pairs of tags
    */
    SELECT jsonb_object_agg(a.key, a.value)
    FROM jsonb_path_query(_tag_maps, '$[*].keyvalue()') x -- all the key value pairs
    INNER JOIN LATERAL -- inner join lateral enables partition elimination at execution time
    (
        SELECT
            a.key,
            a.value
        FROM SCHEMA_TRACING.tag a
        WHERE a.id = (x->>'value')::bigint
        -- filter on a.key to eliminate all but one partition of the tag table
        AND a.key = (SELECT k.key from SCHEMA_TRACING.tag_key k WHERE k.id = (x->>'key')::bigint)
        LIMIT 1
    ) a on (true)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.tag_maps_jsonb(SCHEMA_TRACING_PUBLIC.tag_maps) TO prom_reader;

-- tag maps jsonb

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.tag_maps_jsonb(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, VARIADIC _keys SCHEMA_TRACING_PUBLIC.tag_k[])
RETURNS jsonb
AS $func$
    /*
    takes an tag_maps which is an array of maps of tag_key.id to tag.id
    and returns a jsonb object containing the key value pairs of tags
    only the key/value pairs with keys passed as arguments are included in the output
    */
    SELECT jsonb_object_agg(a.key, a.value)
    FROM jsonb_path_query(_tag_maps, '$[*].*') x -- all the values
    INNER JOIN LATERAL -- inner join lateral enables partition elimination at execution time
    (
        SELECT
            a.key,
            a.value
        FROM SCHEMA_TRACING.tag a
        WHERE a.id = (x#>>'{}')::bigint
        AND a.key = ANY(_keys) -- ANY works with partition elimination
    ) a on (true)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.tag_maps_jsonb(SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TRACING_PUBLIC.tag_k[]) TO prom_reader;

-- tag maps jsonb

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.tag_maps_jsonb(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _ix int)
RETURNS jsonb
AS $func$
    SELECT SCHEMA_TRACING_PUBLIC.tag_map_jsonb((_tag_maps->_ix)::SCHEMA_TRACING_PUBLIC.tag_map)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.tag_maps_jsonb(SCHEMA_TRACING_PUBLIC.tag_maps, int) TO prom_reader;

-- tag maps jsonb

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.tag_maps_jsonb(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _ix int, VARIADIC _keys SCHEMA_TRACING_PUBLIC.tag_k[])
RETURNS jsonb
AS $func$
    SELECT SCHEMA_TRACING_PUBLIC.tag_map_jsonb((_tag_maps->_ix)::SCHEMA_TRACING_PUBLIC.tag_map, variadic _keys)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.tag_maps_jsonb(SCHEMA_TRACING_PUBLIC.tag_maps, int, SCHEMA_TRACING_PUBLIC.tag_k[]) TO prom_reader;

-- tag maps val

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.tag_maps_val(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _ix int, _key SCHEMA_TRACING_PUBLIC.tag_k)
RETURNS SCHEMA_TRACING_PUBLIC.tag_v
AS $func$
    SELECT a.value
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _key -- partition elimination
    AND a.id = (
        jsonb_path_query_first(
            _tag_maps,
            format(
                '$[%s]."%s"',
                _ix,
                (SELECT id FROM SCHEMA_TRACING.tag_key WHERE key = _key)
            )::jsonpath
        )
    )::bigint
    LIMIT 1
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.tag_maps_val(SCHEMA_TRACING_PUBLIC.tag_maps, int, SCHEMA_TRACING_PUBLIC.tag_k) TO prom_reader;

-- tag maps val text

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.tag_maps_val_text(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _ix int, _key SCHEMA_TRACING_PUBLIC.tag_k)
RETURNS text
AS $func$
    SELECT a.value#>>'{}'
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _key -- partition elimination
    AND a.id = (
        jsonb_path_query_first(
            _tag_maps,
            format(
                '$[%s]."%s"',
                _ix,
                (SELECT id FROM SCHEMA_TRACING.tag_key WHERE key = _key)
            )::jsonpath
        )
    )::bigint
    LIMIT 1
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.tag_maps_val_text(SCHEMA_TRACING_PUBLIC.tag_maps, int, SCHEMA_TRACING_PUBLIC.tag_k) TO prom_reader;

-- tag maps element ->

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_element(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _ix int)
RETURNS SCHEMA_TRACING_PUBLIC.tag_map
AS $func$
    -- don't use the -> operator because depending on your search_path
    -- it could cause an infinite loop resulting in a stack depth error
    SELECT jsonb_path_query_first(_tag_maps, format('$[%s]', _ix)::jsonpath)::SCHEMA_TRACING_PUBLIC.tag_map
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
