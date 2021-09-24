

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_matchers_query(_key SCHEMA_TRACING_PUBLIC.tag_k, _path jsonpath)
RETURNS SCHEMA_TRACING_PUBLIC.tag_matchers
AS $sql$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::SCHEMA_TRACING_PUBLIC.tag_matchers
$sql$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_matchers_regex(_key SCHEMA_TRACING_PUBLIC.tag_k, _pattern text)
RETURNS SCHEMA_TRACING_PUBLIC.tag_matchers
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT '{}'::SCHEMA_TRACING_PUBLIC.tag_matchers
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_matchers_not_regex(_key SCHEMA_TRACING_PUBLIC.tag_k, _pattern text)
RETURNS SCHEMA_TRACING_PUBLIC.tag_matchers
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT '{}'::SCHEMA_TRACING_PUBLIC.tag_matchers
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.match(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _maps SCHEMA_TRACING_PUBLIC.tag_matchers)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_matchers_not_regex(_key SCHEMA_TRACING_PUBLIC.tag_k, _pattern text)
RETURNS SCHEMA_TRACING_PUBLIC.tag_matchers
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT '{}'::SCHEMA_TRACING_PUBLIC.tag_matchers
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.has_tag(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _key SCHEMA_TRACING_PUBLIC.tag_k)
RETURNS boolean
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators (no "if not exists" for operators)
    SELECT false
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.@? (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_k,
    RIGHTARG = jsonpath,
    FUNCTION = SCHEMA_TRACING.tag_matchers_query
);

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.==~ (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_k,
    RIGHTARG = text,
    FUNCTION = SCHEMA_TRACING.tag_matchers_regex
);

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.!=~ (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_k,
    RIGHTARG = text,
    FUNCTION = SCHEMA_TRACING.tag_matchers_not_regex
);

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.? (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
    RIGHTARG = SCHEMA_TRACING_PUBLIC.tag_matchers,
    FUNCTION = SCHEMA_TRACING.match
);

CREATE OPERATOR SCHEMA_TRACING_PUBLIC.# (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_map,
    RIGHTARG = SCHEMA_TRACING_PUBLIC.tag_k,
    FUNCTION = SCHEMA_TRACING.has_tag
);


/*
    The anonymous block below generates an equals and not_equals function for each data type. These are used to
    define the == and !== operators.
*/
DO $do$
DECLARE
    _tpl1 text =
$sql$
CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_matchers_typed_%s_%s(_key SCHEMA_TRACING_PUBLIC.tag_k, _val %s)
RETURNS SCHEMA_TRACING_PUBLIC.tag_matchers
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::SCHEMA_TRACING_PUBLIC.tag_matchers
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
$sql$;
    _tpl2 text =
$sql$
CREATE OPERATOR SCHEMA_TRACING_PUBLIC.%s (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_k,
    RIGHTARG = %s,
    FUNCTION = SCHEMA_TRACING.tag_matchers_typed_%s_%s
);
$sql$;
    _sql record;
BEGIN
    FOR _sql IN
    (
        SELECT
            format(_tpl1, replace(t.type, ' ', '_'), f.name, t.type) as func,
            format(_tpl2, f.op, t.type, replace(t.type, ' ', '_'), f.name) as op
        FROM
        (
            VALUES
            ('text'),
            ('smallint'),
            ('int'),
            ('bigint'),
            ('bool'),
            ('real'),
            ('double precision'),
            ('numeric'),
            ('timestamptz'),
            ('timestamp'),
            ('time'),
            ('date')
        ) t(type)
        CROSS JOIN
        (
            VALUES
            ('equal', '=='),
            ('not_equal', '!==')
        ) f(name, op)
    )
    LOOP
        EXECUTE _sql.func;
        EXECUTE _sql.op;
    END LOOP;
END;
$do$;

/*
    The anonymous block below generates 4 functions for each data type.
    The functions are used to define these operators:
    #<
    #<=
    #>
    #>=
*/
DO $do$
DECLARE
    _tpl1 text =
$sql$
CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_matchers_typed_%s_%s(_key SCHEMA_TRACING_PUBLIC.tag_k, _val %s)
RETURNS SCHEMA_TRACING_PUBLIC.tag_matchers
AS $func$
    -- this function body will be replaced later in idempotent script
    -- it's only here so we can create the operators
    SELECT '{}'::SCHEMA_TRACING_PUBLIC.tag_matchers
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
$sql$;
    _tpl2 text =
$sql$
CREATE OPERATOR SCHEMA_TRACING_PUBLIC.%s (
    LEFTARG = SCHEMA_TRACING_PUBLIC.tag_k,
    RIGHTARG = %s,
    FUNCTION = SCHEMA_TRACING.tag_matchers_typed_%s_%s
);
$sql$;
    _sql record;
BEGIN
    FOR _sql IN
    (
        SELECT
            format(_tpl1, replace(t.type, ' ', '_'), f.name, t.type) as func,
            format(_tpl2, f.op, t.type, replace(t.type, ' ', '_'), f.name) as op
        FROM
        (
            VALUES
            ('smallint'        ),
            ('int'             ),
            ('bigint'          ),
            ('bool'            ),
            ('real'            ),
            ('double precision'),
            ('numeric'         ),
            ('timestamptz'     ),
            ('timestamp'       ),
            ('time'            ),
            ('date'            )
        ) t(type)
        CROSS JOIN
        (
            VALUES
            ('less_than', '#<'),
            ('less_than_equal', '#<='),
            ('greater_than', '#>'),
            ('greater_than_equal', '#>=')
        ) f(name, op)
    )
    LOOP
        EXECUTE _sql.func;
        EXECUTE _sql.op;
    END LOOP;
END;
$do$;
