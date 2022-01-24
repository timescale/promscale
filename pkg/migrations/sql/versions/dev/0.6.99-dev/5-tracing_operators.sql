
CREATE OR REPLACE FUNCTION _ps_trace.get_tag_id(_tag_map ps_trace.tag_map, _key ps_trace.tag_k)
RETURNS jsonb
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT null::jsonb
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR ps_trace.# (
    LEFTARG = ps_trace.tag_map,
    RIGHTARG = ps_trace.tag_k,
    FUNCTION = _ps_trace.get_tag_id
);

CREATE OR REPLACE FUNCTION _ps_trace.has_tag(_tag_map ps_trace.tag_map, _key ps_trace.tag_k)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR ps_trace.#? (
    LEFTARG = ps_trace.tag_map,
    RIGHTARG = ps_trace.tag_k,
    FUNCTION = _ps_trace.has_tag
);

CREATE OR REPLACE FUNCTION _ps_trace.match_jsonb_path_exists(_tag_map ps_trace.tag_map, _op ps_tag.tag_op_jsonb_path_exists)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR ps_trace.? (
    LEFTARG = ps_trace.tag_map,
    RIGHTARG = ps_tag.tag_op_jsonb_path_exists,
    FUNCTION = _ps_trace.match_jsonb_path_exists
);

CREATE OR REPLACE FUNCTION _ps_trace.match_regexp_matches(_tag_map ps_trace.tag_map, _op ps_tag.tag_op_regexp_matches)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR ps_trace.? (
    LEFTARG = ps_trace.tag_map,
    RIGHTARG = ps_tag.tag_op_regexp_matches,
    FUNCTION = _ps_trace.match_regexp_matches
);

CREATE OR REPLACE FUNCTION _ps_trace.match_regexp_not_matches(_tag_map ps_trace.tag_map, _op ps_tag.tag_op_regexp_not_matches)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR ps_trace.? (
    LEFTARG = ps_trace.tag_map,
    RIGHTARG = ps_tag.tag_op_regexp_not_matches,
    FUNCTION = _ps_trace.match_regexp_not_matches
);

CREATE OR REPLACE FUNCTION _ps_trace.match_equals(_tag_map ps_trace.tag_map, _op ps_tag.tag_op_equals)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR ps_trace.? (
    LEFTARG = ps_trace.tag_map,
    RIGHTARG = ps_tag.tag_op_equals,
    FUNCTION = _ps_trace.match_equals
);

CREATE OR REPLACE FUNCTION _ps_trace.match_not_equals(_tag_map ps_trace.tag_map, _op ps_tag.tag_op_not_equals)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR ps_trace.? (
    LEFTARG = ps_trace.tag_map,
    RIGHTARG = ps_tag.tag_op_not_equals,
    FUNCTION = _ps_trace.match_not_equals
);

CREATE OR REPLACE FUNCTION _ps_trace.match_less_than(_tag_map ps_trace.tag_map, _op ps_tag.tag_op_less_than)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR ps_trace.? (
    LEFTARG = ps_trace.tag_map,
    RIGHTARG = ps_tag.tag_op_less_than,
    FUNCTION = _ps_trace.match_less_than
);

CREATE OR REPLACE FUNCTION _ps_trace.match_less_than_or_equal(_tag_map ps_trace.tag_map, _op ps_tag.tag_op_less_than_or_equal)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR ps_trace.? (
    LEFTARG = ps_trace.tag_map,
    RIGHTARG = ps_tag.tag_op_less_than_or_equal,
    FUNCTION = _ps_trace.match_less_than_or_equal
);

CREATE OR REPLACE FUNCTION _ps_trace.match_greater_than(_tag_map ps_trace.tag_map, _op ps_tag.tag_op_greater_than)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR ps_trace.? (
    LEFTARG = ps_trace.tag_map,
    RIGHTARG = ps_tag.tag_op_greater_than,
    FUNCTION = _ps_trace.match_greater_than
);

CREATE OR REPLACE FUNCTION _ps_trace.match_greater_than_or_equal(_tag_map ps_trace.tag_map, _op ps_tag.tag_op_greater_than_or_equal)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR ps_trace.? (
    LEFTARG = ps_trace.tag_map,
    RIGHTARG = ps_tag.tag_op_greater_than_or_equal,
    FUNCTION = _ps_trace.match_greater_than_or_equal
);
