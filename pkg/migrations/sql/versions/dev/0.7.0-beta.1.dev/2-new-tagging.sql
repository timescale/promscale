/******************************************************************************
    CREATE NEW DOMAIN FOR TAGS
******************************************************************************/
CALL SCHEMA_CATALOG.execute_everywhere('tracing_types', $ee$ DO $$ BEGIN
    CREATE DOMAIN SCHEMA_TRACING_PUBLIC.tag_maps jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(value) = 'array');
    GRANT USAGE ON DOMAIN SCHEMA_TRACING_PUBLIC.tag_maps TO prom_reader;
END $$ $ee$);

/******************************************************************************
    ALTER THE SPAN TABLE
******************************************************************************/
ALTER TABLE SCHEMA_TRACING.span ADD COLUMN IF NOT EXISTS tags SCHEMA_TRACING_PUBLIC.tag_maps NOT NULL DEFAULT '[{},{},{}]'::jsonb;
UPDATE SCHEMA_TRACING.span u
SET tags = jsonb_build_array(
    '{}'::jsonb, -- reserved for "special" tags
    u.span_tags,
    u.resource_tags
)
WHERE true
;
ALTER TABLE SCHEMA_TRACING.span
    DROP COLUMN IF EXISTS span_tags,
    DROP COLUMN IF EXISTS resource_tags
;
CREATE INDEX CONCURRENTLY IF NOT EXISTS span_tags_idx ON SCHEMA_TRACING.span USING gin (tags jsonb_path_ops);

/******************************************************************************
    ALTER THE EVENT TABLE
******************************************************************************/
ALTER TABLE SCHEMA_TRACING.event RENAME COLUMN tags TO old_tags;
ALTER TABLE SCHEMA_TRACING.event ADD COLUMN IF NOT EXISTS tags SCHEMA_TRACING_PUBLIC.tag_maps NOT NULL DEFAULT '[{},{},{}]'::jsonb;
UPDATE SCHEMA_TRACING.event u
SET tags = jsonb_build_array(
    '{}'::jsonb, -- reserved for "special" tags
    u.old_tags
)
WHERE true
;
ALTER TABLE SCHEMA_TRACING.event DROP COLUMN IF EXISTS old_tags;
CREATE INDEX CONCURRENTLY IF NOT EXISTS event_tags_idx ON SCHEMA_TRACING.event USING gin (tags jsonb_path_ops);

/******************************************************************************
    ALTER THE LINK TABLE
******************************************************************************/
ALTER TABLE SCHEMA_TRACING.link RENAME COLUMN tags TO old_tags;
ALTER TABLE SCHEMA_TRACING.link ADD COLUMN IF NOT EXISTS tags SCHEMA_TRACING_PUBLIC.tag_maps NOT NULL DEFAULT '[{},{},{}]'::jsonb;
UPDATE SCHEMA_TRACING.link u
SET tags = jsonb_build_array(
    '{}'::jsonb, -- reserved for "special" tags
    u.old_tags
)
WHERE true
;
ALTER TABLE SCHEMA_TRACING.link DROP COLUMN IF EXISTS old_tags;
CREATE INDEX CONCURRENTLY IF NOT EXISTS link_tags_idx ON SCHEMA_TRACING.link USING gin (tags jsonb_path_ops);

/******************************************************************************
    STUB OUT NEW FUNCTIONS AND OPERATORS
******************************************************************************/
ALTER FUNCTION SCHEMA_TRACING_PUBLIC.get_tag_id                  (SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TRACING_PUBLIC.tag_k                 ) RENAME TO tag_map_get_tag_id                  ;
ALTER FUNCTION SCHEMA_TRACING_PUBLIC.has_tag                     (SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TRACING_PUBLIC.tag_k                 ) RENAME TO tag_map_has_tag                     ;
ALTER FUNCTION SCHEMA_TRACING_PUBLIC.match_equals                (SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAGGING.tag_op_equals                ) RENAME TO tag_map_match_equals                ;
ALTER FUNCTION SCHEMA_TRACING_PUBLIC.match_greater_than          (SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAGGING.tag_op_greater_than          ) RENAME TO tag_map_match_greater_than          ;
ALTER FUNCTION SCHEMA_TRACING_PUBLIC.match_greater_than_or_equal (SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAGGING.tag_op_greater_than_or_equal ) RENAME TO tag_map_match_greater_than_or_equal ;
ALTER FUNCTION SCHEMA_TRACING_PUBLIC.match_jsonb_path_exists     (SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAGGING.tag_op_jsonb_path_exists     ) RENAME TO tag_map_match_jsonb_path_exists     ;
ALTER FUNCTION SCHEMA_TRACING_PUBLIC.match_less_than             (SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAGGING.tag_op_less_than             ) RENAME TO tag_map_match_less_than             ;
ALTER FUNCTION SCHEMA_TRACING_PUBLIC.match_less_than_or_equal    (SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAGGING.tag_op_less_than_or_equal    ) RENAME TO tag_map_match_less_than_or_equal    ;
ALTER FUNCTION SCHEMA_TRACING_PUBLIC.match_not_equals            (SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAGGING.tag_op_not_equals            ) RENAME TO tag_map_match_not_equals            ;
ALTER FUNCTION SCHEMA_TRACING_PUBLIC.match_regexp_matches        (SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAGGING.tag_op_regexp_matches        ) RENAME TO tag_map_match_regexp_matches        ;
ALTER FUNCTION SCHEMA_TRACING_PUBLIC.match_regexp_not_matches    (SCHEMA_TRACING_PUBLIC.tag_maps, SCHEMA_TAGGING.tag_op_regexp_not_matches    ) RENAME TO tag_map_match_regexp_not_matches    ;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_element(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _index int)
RETURNS SCHEMA_TRACING_PUBLIC.tag_map
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT null::SCHEMA_TRACING_PUBLIC.tag_map
$func$
LANGUAGE sql STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.-> (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
    RIGHTARG = int,
    FUNCTION = SCHEMA_TRACING.tag_maps_element
);

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_get_tag_id(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _key SCHEMA_TRACING_PUBLIC.tag_k)
RETURNS jsonb
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT null::jsonb
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.# (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
    RIGHTARG = SCHEMA_TRACING_PUBLIC.tag_k,
    FUNCTION = SCHEMA_TRACING.tag_maps_get_tag_id
);

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_has_tag(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _key SCHEMA_TRACING_PUBLIC.tag_k)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.#? (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
    RIGHTARG = SCHEMA_TRACING_PUBLIC.tag_k,
    FUNCTION = SCHEMA_TRACING.tag_maps_has_tag
);

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_match_jsonb_path_exists(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _op SCHEMA_TAG.tag_op_jsonb_path_exists)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
    RIGHTARG = SCHEMA_TAG.tag_op_jsonb_path_exists,
    FUNCTION = SCHEMA_TRACING.tag_maps_match_jsonb_path_exists
);

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_match_regexp_matches(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _op SCHEMA_TAG.tag_op_regexp_matches)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
    RIGHTARG = SCHEMA_TAG.tag_op_regexp_matches,
    FUNCTION = SCHEMA_TRACING.tag_maps_match_regexp_matches
);

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_match_regexp_not_matches(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _op SCHEMA_TAG.tag_op_regexp_not_matches)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
    RIGHTARG = SCHEMA_TAG.tag_op_regexp_not_matches,
    FUNCTION = SCHEMA_TRACING.tag_maps_match_regexp_not_matches
);

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_match_equals(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _op SCHEMA_TAG.tag_op_equals)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
    RIGHTARG = SCHEMA_TAG.tag_op_equals,
    FUNCTION = SCHEMA_TRACING.tag_maps_match_equals
);

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_match_not_equals(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _op SCHEMA_TAG.tag_op_not_equals)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
    RIGHTARG = SCHEMA_TAG.tag_op_not_equals,
    FUNCTION = SCHEMA_TRACING.tag_maps_match_not_equals
);

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_match_less_than(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _op SCHEMA_TAG.tag_op_less_than)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
    RIGHTARG = SCHEMA_TAG.tag_op_less_than,
    FUNCTION = SCHEMA_TRACING.tag_maps_match_less_than
);

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_match_less_than_or_equal(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _op SCHEMA_TAG.tag_op_less_than_or_equal)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
    RIGHTARG = SCHEMA_TAG.tag_op_less_than_or_equal,
    FUNCTION = SCHEMA_TRACING.tag_maps_match_less_than_or_equal
);

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_match_greater_than(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _op SCHEMA_TAG.tag_op_greater_than)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
    RIGHTARG = SCHEMA_TAG.tag_op_greater_than,
    FUNCTION = SCHEMA_TRACING.tag_maps_match_greater_than
);

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_maps_match_greater_than_or_equal(_tag_maps SCHEMA_TRACING_PUBLIC.tag_maps, _op SCHEMA_TAG.tag_op_greater_than_or_equal)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_maps,
    RIGHTARG = SCHEMA_TAG.tag_op_greater_than_or_equal,
    FUNCTION = SCHEMA_TRACING.tag_maps_match_greater_than_or_equal
);
