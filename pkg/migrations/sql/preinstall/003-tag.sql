
CREATE VIEW ps_tag.tag_op AS
SELECT *
FROM
(
    VALUES
    ('text'            , 'equals'                 , '=='  ),
    ('text'            , 'not_equals'             , '!==' ),
    ('text'            , 'less_than'              , '#<'  ),
    ('text'            , 'less_than_or_equals'    , '#<=' ),
    ('text'            , 'greater_than'           , '#>'  ),
    ('text'            , 'greater_than_or_equals' , '#>=' ),
    ('text'            , 'regexp_matches'         , '==~' ),
    ('text'            , 'regexp_not_matches'     , '!=~' ),
    ('jsonpath'        , 'jsonb_path_exists'      , '@?'  ),
    ('smallint'        , 'equals'                 , '=='  ),
    ('smallint'        , 'not_equals'             , '!==' ),
    ('smallint'        , 'less_than'              , '#<'  ),
    ('smallint'        , 'less_than_or_equals'    , '#<=' ),
    ('smallint'        , 'greater_than'           , '#>'  ),
    ('smallint'        , 'greater_than_or_equals' , '#>=' ),
    ('int'             , 'equals'                 , '=='  ),
    ('int'             , 'not_equals'             , '!==' ),
    ('int'             , 'less_than'              , '#<'  ),
    ('int'             , 'less_than_or_equals'    , '#<=' ),
    ('int'             , 'greater_than'           , '#>'  ),
    ('int'             , 'greater_than_or_equals' , '#>=' ),
    ('bigint'          , 'equals'                 , '=='  ),
    ('bigint'          , 'not_equals'             , '!==' ),
    ('bigint'          , 'less_than'              , '#<'  ),
    ('bigint'          , 'less_than_or_equals'    , '#<=' ),
    ('bigint'          , 'greater_than'           , '#>'  ),
    ('bigint'          , 'greater_than_or_equals' , '#>=' ),
    ('bool'            , 'equals'                 , '=='  ),
    ('bool'            , 'not_equals'             , '!==' ),
    ('bool'            , 'less_than'              , '#<'  ),
    ('bool'            , 'less_than_or_equals'    , '#<=' ),
    ('bool'            , 'greater_than'           , '#>'  ),
    ('bool'            , 'greater_than_or_equals' , '#>=' ),
    ('real'            , 'equals'                 , '=='  ),
    ('real'            , 'not_equals'             , '!==' ),
    ('real'            , 'less_than'              , '#<'  ),
    ('real'            , 'less_than_or_equals'    , '#<=' ),
    ('real'            , 'greater_than'           , '#>'  ),
    ('real'            , 'greater_than_or_equals' , '#>=' ),
    ('double precision', 'equals'                 , '=='  ),
    ('double precision', 'not_equals'             , '!==' ),
    ('double precision', 'less_than'              , '#<'  ),
    ('double precision', 'less_than_or_equals'    , '#<=' ),
    ('double precision', 'greater_than'           , '#>'  ),
    ('double precision', 'greater_than_or_equals' , '#>=' ),
    ('numeric'         , 'equals'                 , '=='  ),
    ('numeric'         , 'not_equals'             , '!==' ),
    ('numeric'         , 'less_than'              , '#<'  ),
    ('numeric'         , 'less_than_or_equals'    , '#<=' ),
    ('numeric'         , 'greater_than'           , '#>'  ),
    ('numeric'         , 'greater_than_or_equals' , '#>=' ),
    ('timestamptz'     , 'equals'                 , '=='  ),
    ('timestamptz'     , 'not_equals'             , '!==' ),
    ('timestamptz'     , 'less_than'              , '#<'  ),
    ('timestamptz'     , 'less_than_or_equals'    , '#<=' ),
    ('timestamptz'     , 'greater_than'           , '#>'  ),
    ('timestamptz'     , 'greater_than_or_equals' , '#>=' ),
    ('timestamp'       , 'equals'                 , '=='  ),
    ('timestamp'       , 'not_equals'             , '!==' ),
    ('timestamp'       , 'less_than'              , '#<'  ),
    ('timestamp'       , 'less_than_or_equals'    , '#<=' ),
    ('timestamp'       , 'greater_than'           , '#>'  ),
    ('timestamp'       , 'greater_than_or_equals' , '#>=' ),
    ('time'            , 'equals'                 , '=='  ),
    ('time'            , 'not_equals'             , '!==' ),
    ('time'            , 'less_than'              , '#<'  ),
    ('time'            , 'less_than_or_equals'    , '#<=' ),
    ('time'            , 'greater_than'           , '#>'  ),
    ('time'            , 'greater_than_or_equals' , '#>=' ),
    ('date'            , 'equals'                 , '=='  ),
    ('date'            , 'not_equals'             , '!==' ),
    ('date'            , 'less_than'              , '#<'  ),
    ('date'            , 'less_than_or_equals'    , '#<=' ),
    ('date'            , 'greater_than'           , '#>'  ),
    ('date'            , 'greater_than_or_equals' , '#>=' )
) x(typ, opname, op)
;

DO $do$
DECLARE
    _template text =
$sql$
CREATE TYPE ps_tag.tag_op_%1$s_%2$s AS (tag_key text, criterion %3$s);

CREATE FUNCTION ps_tag.tag_op_%1$s_%2$s(_tag_key text, _criterion %3$s)
RETURNS ps_tag.tag_op_%1$s_%2$s AS $func$
    SELECT ROW(_tag_key, _criterion)::ps_tag.tag_op_%1$s_%2$s
$func$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR ps_tag.%4$s
(
    LEFTARG = text,
    RIGHTARG = %3$s,
    FUNCTION = ps_tag.tag_op_%1$s_%2$s
);
$sql$;
    _sql text;
BEGIN
    FOR _sql IN
    (
        SELECT format
        (
            _template,
            replace(x.typ, ' ', '_'),
            x.opname,
            x.typ,
            x.op
        )
        FROM ps_tag.tag_op x
    )
    LOOP
        EXECUTE _sql;
    END LOOP;
END;
$do$;
