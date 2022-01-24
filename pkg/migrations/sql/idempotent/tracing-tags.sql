
-------------------------------------------------------------------------------
-- get tag id
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION _ps_trace.get_tag_id(_tag_map ps_trace.tag_map, _key ps_trace.tag_k)
RETURNS bigint
AS $func$
    SELECT (_tag_map->(SELECT k.id::text from _ps_trace.tag_key k WHERE k.key = _key LIMIT 1))::bigint
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.get_tag_id(ps_trace.tag_map, ps_trace.tag_k) TO prom_reader;
COMMENT ON FUNCTION _ps_trace.get_tag_id IS $$This function supports the # operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR ps_trace.# (
        LEFTARG = ps_trace.tag_map,
        RIGHTARG = ps_trace.tag_k,
        FUNCTION = _ps_trace.get_tag_id
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

-------------------------------------------------------------------------------
-- has tag
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION _ps_trace.eval_tags_by_key(_key ps_trace.tag_k)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), array[]::jsonb[])
    FROM _ps_trace.tag a
    WHERE a.key = _key
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.eval_tags_by_key(ps_trace.tag_k) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.has_tag(_tag_map ps_trace.tag_map, _key ps_trace.tag_k)
RETURNS boolean
AS $func$
    SELECT _tag_map @> ANY(_ps_trace.eval_tags_by_key(_key))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.has_tag(ps_trace.tag_map, ps_trace.tag_k) TO prom_reader;
COMMENT ON FUNCTION _ps_trace.has_tag IS $$This function supports the #? operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR ps_trace.#? (
        LEFTARG = ps_trace.tag_map,
        RIGHTARG = ps_trace.tag_k,
        FUNCTION = _ps_trace.has_tag
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

-------------------------------------------------------------------------------
-- jsonb path exists
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION _ps_trace.eval_jsonb_path_exists(_op ps_tag.tag_op_jsonb_path_exists)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), array[]::jsonb[])
    FROM _ps_trace.tag a
    WHERE a.key = _op.tag_key
    AND jsonb_path_exists(a.value, _op.value)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.eval_jsonb_path_exists(ps_tag.tag_op_jsonb_path_exists) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.match_jsonb_path_exists(_tag_map ps_trace.tag_map, _op ps_tag.tag_op_jsonb_path_exists)
RETURNS boolean
AS $func$
    SELECT _tag_map @> ANY(_ps_trace.eval_jsonb_path_exists(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.match_jsonb_path_exists(ps_trace.tag_map, ps_tag.tag_op_jsonb_path_exists) TO prom_reader;
COMMENT ON FUNCTION _ps_trace.match_jsonb_path_exists IS $$This function supports the @? operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR ps_trace.? (
        LEFTARG = ps_trace.tag_map,
        RIGHTARG = ps_tag.tag_op_jsonb_path_exists,
        FUNCTION = _ps_trace.match_jsonb_path_exists
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

-------------------------------------------------------------------------------
-- regexp matches
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION _ps_trace.eval_regexp_matches(_op ps_tag.tag_op_regexp_matches)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), array[]::jsonb[])
    FROM _ps_trace.tag a
    WHERE a.key = _op.tag_key
    -- if the jsonb value is a string, apply the regex directly
    -- otherwise, convert the value to a text representation, back to a jsonb string, and then apply
    AND CASE jsonb_typeof(a.value)
        WHEN 'string' THEN jsonb_path_exists(a.value, format('$?(@ like_regex "%s")', _op.value)::jsonpath)
        ELSE jsonb_path_exists(to_jsonb(a.value#>>'{}'), format('$?(@ like_regex "%s")', _op.value)::jsonpath)
    END
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.eval_regexp_matches(ps_tag.tag_op_regexp_matches) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.match_regexp_matches(_tag_map ps_trace.tag_map, _op ps_tag.tag_op_regexp_matches)
RETURNS boolean
AS $func$
    SELECT _tag_map @> ANY(_ps_trace.eval_regexp_matches(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.match_regexp_matches(ps_trace.tag_map, ps_tag.tag_op_regexp_matches) TO prom_reader;
COMMENT ON FUNCTION _ps_trace.match_regexp_matches IS $$This function supports the ==~ operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR ps_trace.? (
        LEFTARG = ps_trace.tag_map,
        RIGHTARG = ps_tag.tag_op_regexp_matches,
        FUNCTION = _ps_trace.match_regexp_matches
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

-------------------------------------------------------------------------------
-- regexp not matches
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION _ps_trace.eval_regexp_not_matches(_op ps_tag.tag_op_regexp_not_matches)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), array[]::jsonb[])
    FROM _ps_trace.tag a
    WHERE a.key = _op.tag_key
    -- if the jsonb value is a string, apply the regex directly
    -- otherwise, convert the value to a text representation, back to a jsonb string, and then apply
    AND CASE jsonb_typeof(a.value)
        WHEN 'string' THEN jsonb_path_exists(a.value, format('$?(!(@ like_regex "%s"))', _op.value)::jsonpath)
        ELSE jsonb_path_exists(to_jsonb(a.value#>>'{}'), format('$?(!(@ like_regex "%s"))', _op.value)::jsonpath)
    END
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.eval_regexp_not_matches(ps_tag.tag_op_regexp_not_matches) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.match_regexp_not_matches(_tag_map ps_trace.tag_map, _op ps_tag.tag_op_regexp_not_matches)
RETURNS boolean
AS $func$
    SELECT _tag_map @> ANY(_ps_trace.eval_regexp_not_matches(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.match_regexp_not_matches(ps_trace.tag_map, ps_tag.tag_op_regexp_not_matches) TO prom_reader;
COMMENT ON FUNCTION _ps_trace.match_regexp_not_matches IS $$This function supports the !=~ operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR ps_trace.? (
        LEFTARG = ps_trace.tag_map,
        RIGHTARG = ps_tag.tag_op_regexp_not_matches,
        FUNCTION = _ps_trace.match_regexp_not_matches
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

-------------------------------------------------------------------------------
-- equals
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION _ps_trace.eval_equals(_op ps_tag.tag_op_equals)
RETURNS jsonb
AS $func$
    SELECT jsonb_build_object(a.key_id, a.id)
    FROM _ps_trace.tag a
    WHERE a.key = _op.tag_key
    AND a.value = _op.value
    LIMIT 1
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.eval_equals(ps_tag.tag_op_equals) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.match_equals(_tag_map ps_trace.tag_map, _op ps_tag.tag_op_equals)
RETURNS boolean
AS $func$
    SELECT _tag_map @> (_ps_trace.eval_equals(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.match_equals(ps_trace.tag_map, ps_tag.tag_op_equals) TO prom_reader;
COMMENT ON FUNCTION _ps_trace.match_equals IS $$This function supports the == operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR ps_trace.? (
        LEFTARG = ps_trace.tag_map,
        RIGHTARG = ps_tag.tag_op_equals,
        FUNCTION = _ps_trace.match_equals
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

-------------------------------------------------------------------------------
-- not equals
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION _ps_trace.eval_not_equals(_op ps_tag.tag_op_not_equals)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), array[]::jsonb[])
    FROM _ps_trace.tag a
    WHERE a.key = _op.tag_key
    AND a.value != _op.value
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.eval_not_equals(ps_tag.tag_op_not_equals) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.match_not_equals(_tag_map ps_trace.tag_map, _op ps_tag.tag_op_not_equals)
RETURNS boolean
AS $func$
    SELECT _tag_map @> ANY(_ps_trace.eval_not_equals(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.match_not_equals(ps_trace.tag_map, ps_tag.tag_op_not_equals) TO prom_reader;
COMMENT ON FUNCTION _ps_trace.match_not_equals IS $$This function supports the !== operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR ps_trace.? (
        LEFTARG = ps_trace.tag_map,
        RIGHTARG = ps_tag.tag_op_not_equals,
        FUNCTION = _ps_trace.match_not_equals
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

-------------------------------------------------------------------------------
-- less than
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION _ps_trace.eval_less_than(_op ps_tag.tag_op_less_than)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), array[]::jsonb[])
    FROM _ps_trace.tag a
    WHERE a.key = _op.tag_key
    AND jsonb_path_exists(a.value, '$?(@ < $x)'::jsonpath, jsonb_build_object('x', _op.value))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.eval_less_than(ps_tag.tag_op_less_than) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.match_less_than(_tag_map ps_trace.tag_map, _op ps_tag.tag_op_less_than)
RETURNS boolean
AS $func$
    SELECT _tag_map @> ANY(_ps_trace.eval_less_than(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.match_less_than(ps_trace.tag_map, ps_tag.tag_op_less_than) TO prom_reader;
COMMENT ON FUNCTION _ps_trace.match_less_than IS $$This function supports the #< operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR ps_trace.? (
        LEFTARG = ps_trace.tag_map,
        RIGHTARG = ps_tag.tag_op_less_than,
        FUNCTION = _ps_trace.match_less_than
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

-------------------------------------------------------------------------------
-- less than or equal
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION _ps_trace.eval_less_than_or_equal(_op ps_tag.tag_op_less_than_or_equal)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), array[]::jsonb[])
    FROM _ps_trace.tag a
    WHERE a.key = _op.tag_key
    AND jsonb_path_exists(a.value, '$?(@ <= $x)'::jsonpath, jsonb_build_object('x', _op.value))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.eval_less_than_or_equal(ps_tag.tag_op_less_than_or_equal) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.match_less_than_or_equal(_tag_map ps_trace.tag_map, _op ps_tag.tag_op_less_than_or_equal)
RETURNS boolean
AS $func$
    SELECT _tag_map @> ANY(_ps_trace.eval_less_than_or_equal(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.match_less_than_or_equal(ps_trace.tag_map, ps_tag.tag_op_less_than_or_equal) TO prom_reader;
COMMENT ON FUNCTION _ps_trace.match_less_than_or_equal IS $$This function supports the #<= operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR ps_trace.? (
        LEFTARG = ps_trace.tag_map,
        RIGHTARG = ps_tag.tag_op_less_than_or_equal,
        FUNCTION = _ps_trace.match_less_than_or_equal
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

-------------------------------------------------------------------------------
-- greater than
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION _ps_trace.eval_greater_than(_op ps_tag.tag_op_greater_than)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), array[]::jsonb[])
    FROM _ps_trace.tag a
    WHERE a.key = _op.tag_key
    AND jsonb_path_exists(a.value, '$?(@ > $x)'::jsonpath, jsonb_build_object('x', _op.value))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.eval_greater_than(ps_tag.tag_op_greater_than) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.match_greater_than(_tag_map ps_trace.tag_map, _op ps_tag.tag_op_greater_than)
RETURNS boolean
AS $func$
    SELECT _tag_map @> ANY(_ps_trace.eval_greater_than(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.match_greater_than(ps_trace.tag_map, ps_tag.tag_op_greater_than) TO prom_reader;
COMMENT ON FUNCTION _ps_trace.match_greater_than IS $$This function supports the #> operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR ps_trace.? (
        LEFTARG = ps_trace.tag_map,
        RIGHTARG = ps_tag.tag_op_greater_than,
        FUNCTION = _ps_trace.match_greater_than
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

-------------------------------------------------------------------------------
-- greater than or equal
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION _ps_trace.eval_greater_than_or_equal(_op ps_tag.tag_op_greater_than_or_equal)
RETURNS jsonb[]
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), array[]::jsonb[])
    FROM _ps_trace.tag a
    WHERE a.key = _op.tag_key
    AND jsonb_path_exists(a.value, '$?(@ >= $x)'::jsonpath, jsonb_build_object('x', _op.value))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.eval_greater_than_or_equal(ps_tag.tag_op_greater_than_or_equal) TO prom_reader;

CREATE OR REPLACE FUNCTION _ps_trace.match_greater_than_or_equal(_tag_map ps_trace.tag_map, _op ps_tag.tag_op_greater_than_or_equal)
RETURNS boolean
AS $func$
    SELECT _tag_map @> ANY(_ps_trace.eval_greater_than_or_equal(_op))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION _ps_trace.match_greater_than_or_equal(ps_trace.tag_map, ps_tag.tag_op_greater_than_or_equal) TO prom_reader;
COMMENT ON FUNCTION _ps_trace.match_greater_than_or_equal IS $$This function supports the #>= operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR ps_trace.? (
        LEFTARG = ps_trace.tag_map,
        RIGHTARG = ps_tag.tag_op_greater_than_or_equal,
        FUNCTION = _ps_trace.match_greater_than_or_equal
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

-------------------------------------------------------------------------------
-- jsonb
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ps_trace.jsonb(_tag_map ps_trace.tag_map)
RETURNS jsonb
AS $func$
    /*
    takes an tag_map which is a map of tag_key.id to tag.id
    and returns a jsonb object containing the key value pairs of tags
    */
    SELECT jsonb_object_agg(a.key, a.value)
    FROM jsonb_each(_tag_map) x -- key is tag_key.id, value is tag.id
    INNER JOIN LATERAL -- inner join lateral enables partition elimination at execution time
    (
        SELECT
            a.key,
            a.value
        FROM _ps_trace.tag a
        WHERE a.id = x.value::text::bigint
        -- filter on a.key to eliminate all but one partition of the tag table
        AND a.key = (SELECT k.key from _ps_trace.tag_key k WHERE k.id = x.key::bigint)
        LIMIT 1
    ) a on (true)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION ps_trace.jsonb(ps_trace.tag_map) TO prom_reader;

-------------------------------------------------------------------------------
-- jsonb
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ps_trace.jsonb(_tag_map ps_trace.tag_map, VARIADIC _keys ps_trace.tag_k[])
RETURNS jsonb
AS $func$
    /*
    takes an tag_map which is a map of tag_key.id to tag.id
    and returns a jsonb object containing the key value pairs of tags
    only the key/value pairs with keys passed as arguments are included in the output
    */
    SELECT jsonb_object_agg(a.key, a.value)
    FROM jsonb_each(_tag_map) x -- key is tag_key.id, value is tag.id
    INNER JOIN LATERAL -- inner join lateral enables partition elimination at execution time
    (
        SELECT
            a.key,
            a.value
        FROM _ps_trace.tag a
        WHERE a.id = x.value::text::bigint
        AND a.key = ANY(_keys) -- ANY works with partition elimination
    ) a on (true)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION ps_trace.jsonb(ps_trace.tag_map) TO prom_reader;

-------------------------------------------------------------------------------
-- val
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ps_trace.val(_tag_map ps_trace.tag_map, _key ps_trace.tag_k)
RETURNS ps_trace.tag_v
AS $func$
    SELECT a.value
    FROM _ps_trace.tag a
    WHERE a.key = _key -- partition elimination
    AND a.id = (_tag_map->>(SELECT id::text FROM _ps_trace.tag_key WHERE key = _key))::bigint
    LIMIT 1
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION ps_trace.val(ps_trace.tag_map, ps_trace.tag_k) TO prom_reader;

-------------------------------------------------------------------------------
-- val_text
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ps_trace.val_text(_tag_map ps_trace.tag_map, _key ps_trace.tag_k)
RETURNS text
AS $func$
    SELECT a.value#>>'{}'
    FROM _ps_trace.tag a
    WHERE a.key = _key -- partition elimination
    AND a.id = (_tag_map->>(SELECT id::text FROM _ps_trace.tag_key WHERE key = _key))::bigint
    LIMIT 1
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION ps_trace.val_text(ps_trace.tag_map, ps_trace.tag_k) TO prom_reader;
