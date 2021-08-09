
CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_matchers(_key SCHEMA_TRACING_PUBLIC.tag_k, _qry jsonpath, _vars jsonb DEFAULT '{}'::jsonb, _silent boolean DEFAULT false)
RETURNS SCHEMA_TRACING_PUBLIC.tag_matchers
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), '{}')::SCHEMA_TRACING_PUBLIC.tag_matchers
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _key
    AND jsonb_path_exists(a.value, _qry, _vars, _silent)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_matchers(SCHEMA_TRACING_PUBLIC.tag_k, jsonpath, jsonb, boolean) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_matchers_query(_key SCHEMA_TRACING_PUBLIC.tag_k, _path jsonpath)
RETURNS SCHEMA_TRACING_PUBLIC.tag_matchers
AS $func$
    SELECT SCHEMA_TRACING.tag_matchers(_key, _path);
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_matchers_query(SCHEMA_TRACING_PUBLIC.tag_k, jsonpath) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_matchers_query IS
$$This function is used to define the @? operator.$$;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_matchers_regex(_key SCHEMA_TRACING_PUBLIC.tag_k, _pattern text)
RETURNS SCHEMA_TRACING_PUBLIC.tag_matchers
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), '{}')::SCHEMA_TRACING_PUBLIC.tag_matchers
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _key AND
    -- if the jsonb value is a string, apply the regex directly
    -- otherwise, convert the value to a text representation, back to a jsonb string, and then apply
    CASE jsonb_typeof(a.value)
        WHEN 'string' THEN jsonb_path_exists(a.value, format('$?(@ like_regex "%s")', _pattern)::jsonpath)
        ELSE jsonb_path_exists(to_jsonb(a.value#>>'{}'), format('$?(@ like_regex "%s")', _pattern)::jsonpath)
    END
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_matchers_regex(SCHEMA_TRACING_PUBLIC.tag_k, text) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_matchers_regex IS
$$This function is used to define the ==~ operator.$$;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_matchers_not_regex(_key SCHEMA_TRACING_PUBLIC.tag_k, _pattern text)
RETURNS SCHEMA_TRACING_PUBLIC.tag_matchers
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), '{}')::SCHEMA_TRACING_PUBLIC.tag_matchers
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _key AND
    -- if the jsonb value is a string, apply the regex directly
    -- otherwise, convert the value to a text representation, back to a jsonb string, and then apply
    CASE jsonb_typeof(a.value)
        WHEN 'string' THEN jsonb_path_exists(a.value, format('$?(!(@ like_regex "%s"))', _pattern)::jsonpath)
        ELSE jsonb_path_exists(to_jsonb(a.value#>>'{}'), format('$?(!(@ like_regex "%s"))', _pattern)::jsonpath)
    END
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_matchers_not_regex(SCHEMA_TRACING_PUBLIC.tag_k, text) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_matchers_not_regex IS
$$This function is used to define the !=~ operator.$$;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.match(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _maps SCHEMA_TRACING_PUBLIC.tag_matchers)
RETURNS boolean
AS $func$
    SELECT _tag_map @> ANY(_maps)
$func$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.match(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TRACING_PUBLIC.tag_matchers) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.match IS
$$This function is used to define the ? operator.$$;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_matchers_equal(_key SCHEMA_TRACING_PUBLIC.tag_k, _val SCHEMA_TRACING_PUBLIC.tag_v)
RETURNS SCHEMA_TRACING_PUBLIC.tag_matchers
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), '{}')::SCHEMA_TRACING_PUBLIC.tag_matchers
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _key
    AND a.value = _val
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_matchers_equal(SCHEMA_TRACING_PUBLIC.tag_k, SCHEMA_TRACING_PUBLIC.tag_v) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_matchers_equal IS
$$This function is used to define the === operator.$$;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_matchers_not_equal(_key SCHEMA_TRACING_PUBLIC.tag_k, _val SCHEMA_TRACING_PUBLIC.tag_v)
RETURNS SCHEMA_TRACING_PUBLIC.tag_matchers
AS $func$
    SELECT coalesce(array_agg(jsonb_build_object(a.key_id, a.id)), '{}')::SCHEMA_TRACING_PUBLIC.tag_matchers
    FROM SCHEMA_TRACING.tag a
    WHERE a.key = _key
    AND a.value != _val
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_matchers_not_equal(SCHEMA_TRACING_PUBLIC.tag_k, SCHEMA_TRACING_PUBLIC.tag_v) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.tag_matchers_not_equal IS
$$This function is used to define the !== operator.$$;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING.has_tag(_tag_map SCHEMA_TRACING_PUBLIC.tag_map, _key SCHEMA_TRACING_PUBLIC.tag_k)
RETURNS boolean
AS $func$
    SELECT SCHEMA_TRACING.match(_tag_map,
    (
        SELECT array_agg(jsonb_build_object(a.key_id, a.id))::SCHEMA_TRACING_PUBLIC.tag_matchers
        FROM SCHEMA_TRACING.tag a
        WHERE a.key = _key
    ))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.has_tag(SCHEMA_TRACING_PUBLIC.tag_map, SCHEMA_TRACING_PUBLIC.tag_k) TO prom_reader;
COMMENT ON FUNCTION SCHEMA_TRACING.has_tag IS
$$This function is used to define the # operator.$$;

/*
    The anonymous block below generates an equals and not_equals function for each data type. These are used to
    define the == and !== operators.
*/
DO $do$
DECLARE
    _tpl1 text =
$sql$
CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_matchers_typed_%1$s_%2$s(_key SCHEMA_TRACING_PUBLIC.tag_k, _val %3$s)
RETURNS SCHEMA_TRACING_PUBLIC.tag_matchers
AS $func$
    SELECT SCHEMA_TRACING.tag_matchers_%2$s(_key,to_jsonb(_val))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
$sql$;
    _tpl2 text =
$sql$
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_matchers_typed_%1$s_%2$s(SCHEMA_TRACING_PUBLIC.tag_k, %3$s) TO prom_reader;
$sql$;
    _tpl3 text =
$sql$
COMMENT ON FUNCTION SCHEMA_TRACING.tag_matchers_typed_%1$s_%2$s IS $$This function is used to define the %3$s operator.$$;
$sql$;
    _types text[] = ARRAY[
        'text',
        'smallint',
        'int',
        'bigint',
        'bool',
        'real',
        'double precision',
        'numeric',
        'timestamptz',
        'timestamp',
        'time',
        'date'
    ];
    _type text;
BEGIN
    FOREACH _type IN ARRAY _types
    LOOP
        EXECUTE format(_tpl1, replace(_type, ' ', '_'), 'equal', _type);
        EXECUTE format(_tpl2, replace(_type, ' ', '_'), 'equal', _type);
        EXECUTE format(_tpl3, replace(_type, ' ', '_'), 'equal', '==');
        EXECUTE format(_tpl1, replace(_type, ' ', '_'), 'not_equal', _type);
        EXECUTE format(_tpl2, replace(_type, ' ', '_'), 'not_equal', _type);
        EXECUTE format(_tpl3, replace(_type, ' ', '_'), 'not_equal', '!==');
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
CREATE OR REPLACE FUNCTION SCHEMA_TRACING.tag_matchers_typed_%1$s_%2$s(_key SCHEMA_TRACING_PUBLIC.tag_k, _val %3$s)
RETURNS SCHEMA_TRACING_PUBLIC.tag_matchers
AS $func$
    SELECT SCHEMA_TRACING.tag_matchers(_key, '%4$s', jsonb_build_object('x', to_jsonb(_val)))
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
$sql$;
    _tpl2 text =
$sql$
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING.tag_matchers_typed_%1$s_%2$s(SCHEMA_TRACING_PUBLIC.tag_k, %3$s) TO prom_reader;
$sql$;
    _tpl3 text =
$sql$
COMMENT ON FUNCTION SCHEMA_TRACING.tag_matchers_typed_%1$s_%2$s IS $$This function is used to define the %3$s operator.$$;
$sql$;
    _sql record;
BEGIN
    FOR _sql IN
    (
        SELECT
            format
            (
                _tpl1,
                replace(t.type, ' ', '_'),
                f.name,
                t.type,
                format('$?(@ %s $x)', f.jop)
            ) as func,
            format(_tpl2, replace(t.type, ' ', '_'), f.name, t.type) as op,
            format(_tpl3, replace(t.type, ' ', '_'), f.name, f.op) as com
        FROM
        (
            VALUES
            ('smallint'        ),
            ('int'             ),
            ('bigint'          ),
            ('bool'            ),
            ('real'            ),
            ('double precision'),
            ('numeric'         )
        ) t(type)
        CROSS JOIN
        (
            VALUES
            ('less_than'            , '#<'  , '<' ),
            ('less_than_equal'      , '#<=' , '<='),
            ('greater_than'         , '#>'  , '>' ),
            ('greater_than_equal'   , '#>=' , '>=')
        ) f(name, op, jop)
    )
    LOOP
        EXECUTE _sql.func;
        EXECUTE _sql.op;
        EXECUTE _sql.com;
    END LOOP;
END;
$do$;
