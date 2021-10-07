
CREATE OR REPLACE FUNCTION SCHEMA_TRACING.get_tag_id(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _key SCHEMA_TRACING_PUBLIC.tag_k)
RETURNS jsonb
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT null::jsonb
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.# (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
    RIGHTARG = SCHEMA_TRACING_PUBLIC.tag_k,
    FUNCTION = SCHEMA_TRACING.get_tag_id
);

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.has_tag(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _key SCHEMA_TRACING_PUBLIC.tag_k)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.#? (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
    RIGHTARG = SCHEMA_TRACING_PUBLIC.tag_k,
    FUNCTION = SCHEMA_TRACING.has_tag
);

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.match_jsonb_path_exists(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _op SCHEMA_TAG.tag_op_jsonb_path_exists)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
    RIGHTARG = SCHEMA_TAG.tag_op_jsonb_path_exists,
    FUNCTION = SCHEMA_TRACING.match_jsonb_path_exists
);

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.match_regexp_matches(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _op SCHEMA_TAG.tag_op_regexp_matches)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
    RIGHTARG = SCHEMA_TAG.tag_op_regexp_matches,
    FUNCTION = SCHEMA_TRACING.match_regexp_matches
);

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.match_regexp_not_matches(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _op SCHEMA_TAG.tag_op_regexp_not_matches)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
    RIGHTARG = SCHEMA_TAG.tag_op_regexp_not_matches,
    FUNCTION = SCHEMA_TRACING.match_regexp_not_matches
);

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.match_equals(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _op SCHEMA_TAG.tag_op_equals)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
    RIGHTARG = SCHEMA_TAG.tag_op_equals,
    FUNCTION = SCHEMA_TRACING.match_equals
);

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.match_not_equals(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _op SCHEMA_TAG.tag_op_not_equals)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
    RIGHTARG = SCHEMA_TAG.tag_op_not_equals,
    FUNCTION = SCHEMA_TRACING.match_not_equals
);

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.match_less_than(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _op SCHEMA_TAG.tag_op_less_than)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
    RIGHTARG = SCHEMA_TAG.tag_op_less_than,
    FUNCTION = SCHEMA_TRACING.match_less_than
);

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.match_less_than_or_equal(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _op SCHEMA_TAG.tag_op_less_than_or_equal)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
    RIGHTARG = SCHEMA_TAG.tag_op_less_than_or_equal,
    FUNCTION = SCHEMA_TRACING.match_less_than_or_equal
);

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.match_greater_than(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _op SCHEMA_TAG.tag_op_greater_than)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
    RIGHTARG = SCHEMA_TAG.tag_op_greater_than,
    FUNCTION = SCHEMA_TRACING.match_greater_than
);

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.match_greater_than_or_equal(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _op SCHEMA_TAG.tag_op_greater_than_or_equal)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
    RIGHTARG = SCHEMA_TAG.tag_op_greater_than_or_equal,
    FUNCTION = SCHEMA_TRACING.match_greater_than_or_equal
);
