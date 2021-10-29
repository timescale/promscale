/******************************************************************************
    CREATE NEW DOMAIN FOR TAGS
******************************************************************************/
CALL SCHEMA_CATALOG.execute_everywhere('tag_maps', $ee$ DO $$ BEGIN
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
    DROP COLUMN IF EXISTS span_tags CASCADE,
    DROP COLUMN IF EXISTS resource_tags CASCADE
;
CREATE INDEX IF NOT EXISTS span_tags_idx ON SCHEMA_TRACING.span USING gin (tags jsonb_path_ops);

/******************************************************************************
    DROP OLD OPERATORS AND FUNCTIONS TO "MAKE ROOM" FOR NEW FUNCTIONS
******************************************************************************/

DO $do$
DECLARE
    _sql text;
BEGIN
    FOR _sql IN
    (
        SELECT format('DROP OPERATOR IF EXISTS %I.%s (%I, %I)',
            n.nspname,
            o.oprname,
            coalesce(l.typname, 'NONE'),
            coalesce(r.typname, 'NONE')
        )
        FROM pg_operator o
        INNER JOIN pg_namespace n on (o.oprnamespace = n.oid)
        LEFT OUTER JOIN pg_type l on (o.oprleft = l.oid)
        LEFT OUTER JOIN pg_type r on (o.oprright = r.oid)
        LEFT OUTER JOIN pg_type x on (o.oprresult = x.oid)
        WHERE n.nspname in ('_ps_trace', 'ps_trace')
    )
    LOOP
        EXECUTE _sql;
    END LOOP;
END;
$do$;

DO $do$
DECLARE
    _sql text;
BEGIN
    FOR _sql IN
    (
        SELECT format('DROP FUNCTION IF EXISTS %I.%I(%s)',
        n.nspname,
        p.proname,
        (
            SELECT string_agg(t.typname, ', ' order by ordinality)
            FROM unnest(p.proargtypes) with ORDINALITY x
            INNER JOIN pg_type t on (x = t.oid)
        ))
        FROM pg_catalog.pg_proc p
        INNER JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname in ('SCHEMA_TRACING_PUBLIC', 'SCHEMA_TRACING')
        and p.prokind = 'f'
    )
    LOOP
        EXECUTE _sql;
    END LOOP;
END;
$do$;

