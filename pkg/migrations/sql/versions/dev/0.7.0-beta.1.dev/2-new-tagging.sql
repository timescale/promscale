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
    DROP OLD FUNCTIONS AND OPERATORS
******************************************************************************/
DROP FUNCTION IF EXISTS SCHEMA_TRACING.get_tag_id(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TRACING_PUBLIC.tag_k);
DROP OPERATOR IF EXISTS SCHEMA_TRACING_PUBLIC.# (
    SCHEMA_TRACING_PUBLIC.tag_map,
    SCHEMA_TRACING_PUBLIC.tag_k
) CASCADE;

DROP FUNCTION IF EXISTS SCHEMA_TRACING.has_tag(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TRACING_PUBLIC.tag_k);
DROP OPERATOR IF EXISTS SCHEMA_TRACING_PUBLIC.#? (
    SCHEMA_TRACING_PUBLIC.tag_map,
    SCHEMA_TRACING_PUBLIC.tag_k
) CASCADE;

DROP FUNCTION IF EXISTS SCHEMA_TRACING.match_jsonb_path_exists(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_jsonb_path_exists);
DROP OPERATOR IF EXISTS SCHEMA_TRACING_PUBLIC.? (
    SCHEMA_TRACING_PUBLIC.tag_map,
    SCHEMA_TAG.tag_op_jsonb_path_exists
) CASCADE;

DROP FUNCTION IF EXISTS SCHEMA_TRACING.match_regexp_matches(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_regexp_matches);
DROP OPERATOR IF EXISTS SCHEMA_TRACING_PUBLIC.? (
    SCHEMA_TRACING_PUBLIC.tag_map,
    SCHEMA_TAG.tag_op_regexp_matches
) CASCADE;

DROP FUNCTION IF EXISTS SCHEMA_TRACING.match_regexp_not_matches(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_regexp_not_matches);
CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
    SCHEMA_TRACING_PUBLIC.tag_map,
    SCHEMA_TAG.tag_op_regexp_not_matches
) CASCADE;

DROP FUNCTION IF EXISTS SCHEMA_TRACING.match_equals(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_equals);
DROP OPERATOR IF EXISTS SCHEMA_TRACING_PUBLIC.? (
    SCHEMA_TRACING_PUBLIC.tag_maps,
    SCHEMA_TAG.tag_op_equals
) CASCADE;

DROP FUNCTION IF EXISTS SCHEMA_TRACING.match_not_equals(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_not_equals);
DROP OPERATOR IF EXISTS SCHEMA_TRACING_PUBLIC.? (
    SCHEMA_TRACING_PUBLIC.tag_maps,
    SCHEMA_TAG.tag_op_not_equals
) CASCADE;

DROP FUNCTION IF EXISTS SCHEMA_TRACING.match_less_than(SCHEMA_TRACING_PUBLIC.tag_map, _op SCHEMA_TAG.tag_op_less_than);
DROP OPERATOR IF EXISTS SCHEMA_TRACING_PUBLIC.? (
    SCHEMA_TRACING_PUBLIC.tag_map,
    SCHEMA_TAG.tag_op_less_than
) CASCADE;

DROP FUNCTION IF EXISTS SCHEMA_TRACING.match_less_than_or_equal(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_less_than_or_equal);
DROP OPERATOR IF EXISTS SCHEMA_TRACING_PUBLIC.? (
    SCHEMA_TRACING_PUBLIC.tag_map,
    SCHEMA_TAG.tag_op_less_than_or_equal
) CASCADE;

DROP FUNCTION IF EXISTS SCHEMA_TRACING.match_greater_than(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_greater_than);
DROP OPERATOR IF EXISTS SCHEMA_TRACING_PUBLIC.? (
    SCHEMA_TRACING_PUBLIC.tag_map,
    SCHEMA_TAG.tag_op_greater_than
) CASCADE;

DROP FUNCTION IF EXISTS SCHEMA_TRACING.match_greater_than_or_equal(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TAG.tag_op_greater_than_or_equal);
DROP OPERATOR IF EXISTS SCHEMA_TRACING_PUBLIC.? (
    SCHEMA_TRACING_PUBLIC.tag_map,
    SCHEMA_TAG.tag_op_greater_than_or_equal
) CASCADE;
