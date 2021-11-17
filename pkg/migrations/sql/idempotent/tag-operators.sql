-------------------------------------------------------------------------------
-- jsonb_path_exists
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION SCHEMA_TAG.tag_op_jsonb_path_exists(_tag_key text, _value jsonpath)
RETURNS SCHEMA_TAG.tag_op_jsonb_path_exists AS $func$
    SELECT ROW(_tag_key, _value)::SCHEMA_TAG.tag_op_jsonb_path_exists
$func$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TAG.tag_op_jsonb_path_exists(text, jsonpath) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TAG.tag_op_jsonb_path_exists IS $$This function supports the @? operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TAG.@? (
        LEFTARG = text,
        RIGHTARG = jsonpath,
        FUNCTION = SCHEMA_TAG.tag_op_jsonb_path_exists
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

-------------------------------------------------------------------------------
-- regexp_matches
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION SCHEMA_TAG.tag_op_regexp_matches(_tag_key text, _value text)
RETURNS SCHEMA_TAG.tag_op_regexp_matches AS $func$
    SELECT ROW(_tag_key, _value)::SCHEMA_TAG.tag_op_regexp_matches
$func$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TAG.tag_op_regexp_matches(text, text) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TAG.tag_op_regexp_matches IS $$This function supports the ==~ operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TAG.==~ (
        LEFTARG = text,
        RIGHTARG = text,
        FUNCTION = SCHEMA_TAG.tag_op_regexp_matches
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

-------------------------------------------------------------------------------
-- regexp_not_matches
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION SCHEMA_TAG.tag_op_regexp_not_matches(_tag_key text, _value text)
RETURNS SCHEMA_TAG.tag_op_regexp_not_matches AS $func$
    SELECT ROW(_tag_key, _value)::SCHEMA_TAG.tag_op_regexp_not_matches
$func$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TAG.tag_op_regexp_not_matches(text, text) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TAG.tag_op_regexp_not_matches IS $$This function supports the !=~ operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TAG.!=~ (
        LEFTARG = text,
        RIGHTARG = text,
        FUNCTION = SCHEMA_TAG.tag_op_regexp_not_matches
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

-------------------------------------------------------------------------------
-- equals
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION SCHEMA_TAG.tag_op_equals_text(_tag_key text, _value text)
RETURNS SCHEMA_TAG.tag_op_equals AS $func$
    SELECT ROW(_tag_key, to_jsonb(_value))::SCHEMA_TAG.tag_op_equals
$func$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TAG.tag_op_equals_text(text, text) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TAG.tag_op_equals_text(text, text) IS $$This function supports the == operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TAG.== (
        LEFTARG = text,
        RIGHTARG = text,
        FUNCTION = SCHEMA_TAG.tag_op_equals_text
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

CREATE OR REPLACE FUNCTION SCHEMA_TAG.tag_op_equals(_tag_key text, _value anyelement)
RETURNS SCHEMA_TAG.tag_op_equals AS $func$
    SELECT ROW(_tag_key, to_jsonb(_value))::SCHEMA_TAG.tag_op_equals
$func$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TAG.tag_op_equals(text, anyelement) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TAG.tag_op_equals(text, anyelement) IS $$This function supports the == operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TAG.== (
        LEFTARG = text,
        RIGHTARG = anyelement,
        FUNCTION = SCHEMA_TAG.tag_op_equals
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

-------------------------------------------------------------------------------
-- not_equals
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION SCHEMA_TAG.tag_op_not_equals_text(_tag_key text, _value text)
RETURNS SCHEMA_TAG.tag_op_not_equals AS $func$
    SELECT ROW(_tag_key, to_jsonb(_value))::SCHEMA_TAG.tag_op_not_equals
$func$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TAG.tag_op_not_equals_text(text, text) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TAG.tag_op_not_equals_text(text, text) IS $$This function supports the !== operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TAG.!== (
        LEFTARG = text,
        RIGHTARG = text,
        FUNCTION = SCHEMA_TAG.tag_op_not_equals_text
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

CREATE OR REPLACE FUNCTION SCHEMA_TAG.tag_op_not_equals(_tag_key text, _value anyelement)
RETURNS SCHEMA_TAG.tag_op_not_equals AS $func$
    SELECT ROW(_tag_key, to_jsonb(_value))::SCHEMA_TAG.tag_op_not_equals
$func$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TAG.tag_op_not_equals(text, anyelement) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TAG.tag_op_not_equals(text, anyelement) IS $$This function supports the !== operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TAG.!== (
        LEFTARG = text,
        RIGHTARG = anyelement,
        FUNCTION = SCHEMA_TAG.tag_op_not_equals
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

-------------------------------------------------------------------------------
-- less_than
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION SCHEMA_TAG.tag_op_less_than_text(_tag_key text, _value text)
RETURNS SCHEMA_TAG.tag_op_less_than AS $func$
    SELECT ROW(_tag_key, to_jsonb(_value))::SCHEMA_TAG.tag_op_less_than
$func$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TAG.tag_op_less_than_text(text, text) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TAG.tag_op_less_than_text(text, text) IS $$This function supports the #< operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TAG.#< (
        LEFTARG = text,
        RIGHTARG = text,
        FUNCTION = SCHEMA_TAG.tag_op_less_than_text
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

CREATE OR REPLACE FUNCTION SCHEMA_TAG.tag_op_less_than(_tag_key text, _value anyelement)
RETURNS SCHEMA_TAG.tag_op_less_than AS $func$
    SELECT ROW(_tag_key, to_jsonb(_value))::SCHEMA_TAG.tag_op_less_than
$func$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TAG.tag_op_less_than(text, anyelement) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TAG.tag_op_less_than(text, anyelement) IS $$This function supports the #< operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TAG.#< (
        LEFTARG = text,
        RIGHTARG = anyelement,
        FUNCTION = SCHEMA_TAG.tag_op_less_than
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

-------------------------------------------------------------------------------
-- less_than_or_equal
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION SCHEMA_TAG.tag_op_less_than_or_equal_text(_tag_key text, _value text)
RETURNS SCHEMA_TAG.tag_op_less_than_or_equal AS $func$
    SELECT ROW(_tag_key, to_jsonb(_value))::SCHEMA_TAG.tag_op_less_than_or_equal
$func$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TAG.tag_op_less_than_or_equal_text(text, text) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TAG.tag_op_less_than_or_equal_text(text, text) IS $$This function supports the #<= operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TAG.#<= (
        LEFTARG = text,
        RIGHTARG = text,
        FUNCTION = SCHEMA_TAG.tag_op_less_than_or_equal_text
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

CREATE OR REPLACE FUNCTION SCHEMA_TAG.tag_op_less_than_or_equal(_tag_key text, _value anyelement)
RETURNS SCHEMA_TAG.tag_op_less_than_or_equal AS $func$
    SELECT ROW(_tag_key, to_jsonb(_value))::SCHEMA_TAG.tag_op_less_than_or_equal
$func$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TAG.tag_op_less_than_or_equal(text, anyelement) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TAG.tag_op_less_than_or_equal IS $$This function supports the #<= operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TAG.#<= (
        LEFTARG = text,
        RIGHTARG = anyelement,
        FUNCTION = SCHEMA_TAG.tag_op_less_than_or_equal
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

-------------------------------------------------------------------------------
-- greater_than
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION SCHEMA_TAG.tag_op_greater_than_text(_tag_key text, _value text)
RETURNS SCHEMA_TAG.tag_op_greater_than AS $func$
    SELECT ROW(_tag_key, to_jsonb(_value))::SCHEMA_TAG.tag_op_greater_than
$func$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TAG.tag_op_greater_than_text(text, text) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TAG.tag_op_greater_than_text(text, text) IS $$This function supports the #> operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TAG.#> (
        LEFTARG = text,
        RIGHTARG = text,
        FUNCTION = SCHEMA_TAG.tag_op_greater_than_text
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

CREATE OR REPLACE FUNCTION SCHEMA_TAG.tag_op_greater_than(_tag_key text, _value anyelement)
RETURNS SCHEMA_TAG.tag_op_greater_than AS $func$
    SELECT ROW(_tag_key, to_jsonb(_value))::SCHEMA_TAG.tag_op_greater_than
$func$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TAG.tag_op_greater_than(text, anyelement) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TAG.tag_op_greater_than(text, anyelement) IS $$This function supports the #> operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TAG.#> (
        LEFTARG = text,
        RIGHTARG = anyelement,
        FUNCTION = SCHEMA_TAG.tag_op_greater_than
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

-------------------------------------------------------------------------------
-- greater_than_or_equal
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION SCHEMA_TAG.tag_op_greater_than_or_equal_text(_tag_key text, _value text)
RETURNS SCHEMA_TAG.tag_op_greater_than_or_equal AS $func$
    SELECT ROW(_tag_key, to_jsonb(_value))::SCHEMA_TAG.tag_op_greater_than_or_equal
$func$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TAG.tag_op_greater_than_or_equal_text(text, text) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TAG.tag_op_greater_than_or_equal_text(text, text) IS $$This function supports the #>= operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TAG.#>= (
        LEFTARG = text,
        RIGHTARG = text,
        FUNCTION = SCHEMA_TAG.tag_op_greater_than_or_equal_text
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;

CREATE OR REPLACE FUNCTION SCHEMA_TAG.tag_op_greater_than_or_equal(_tag_key text, _value anyelement)
RETURNS SCHEMA_TAG.tag_op_greater_than_or_equal AS $func$
    SELECT ROW(_tag_key, to_jsonb(_value))::SCHEMA_TAG.tag_op_greater_than_or_equal
$func$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TAG.tag_op_greater_than_or_equal(text, anyelement) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TAG.tag_op_greater_than_or_equal IS $$This function supports the #>= operator.$$;

DO $do$
BEGIN
    CREATE OPERATOR SCHEMA_TAG.#>= (
        LEFTARG = text,
        RIGHTARG = anyelement,
        FUNCTION = SCHEMA_TAG.tag_op_greater_than_or_equal
    );
EXCEPTION
    WHEN SQLSTATE '42723' THEN -- operator already exists
        null;
END;
$do$;
