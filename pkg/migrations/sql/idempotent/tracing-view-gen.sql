

CREATE OR REPLACE FUNCTION ps_trace.create_view(_name text, variadic _spec text[])
RETURNS VOID
AS $func$
DECLARE
    _template text = $sql$
left outer join lateral
(
    select (t%1$s.value%4$s as %2$I
    from _ps_trace.tag t%1$s
    where t%1$s.key = '%3$s'
    and (s.span_tags->>(t%1$s.key_id::text))::bigint = t%1$s.id
) t%1$s on (true)$sql$;
    _sql text;
BEGIN
    if array_length(_spec, 1) % 3 != 0 then
        raise exception 'Invalid spec';
    end if;

    select
    format(E'create or replace view ps_trace.%I as\nselect\n    s.*,\n    ', _name)
    || string_agg(format('t%s.%I', x.ix, lower(x.col)), E',\n    ' order by x.ix)
    || E'\nfrom _ps_trace.span s'
    || string_agg(format(_template, x.ix, lower(x.col), x.key, x.conversion), '' order by x.ix)
    into strict _sql
    from
    (
        select row_number() over (order by k.ix) as ix, k.key, c.col, t.conversion
        from
        (
            select *
            from unnest(_spec) with ordinality k(key, ix)
            where ix % 3 = 1
        ) k
        inner join
        (
            select *
            from unnest(_spec) with ordinality c(col, ix)
            where ix % 3 = 2
        ) c on (k.ix + 1 = c.ix)
        inner join
        (
            select
                t.typ,
                t.ix,
                case lower(t.typ)
                    when 'jsonb' then ')'
                    when 'text' then $sql$#>>'{}')$sql$
                    else format($sql$#>>'{})::%s$sql$, lower(t.typ))
                end as conversion
            from unnest(_spec) with ordinality t(typ, ix)
            where ix % 3 = 0
        ) t on (k.ix + 2 = t.ix)
        order by k.ix
    ) x
    ;

    RAISE NOTICE '%', _sql;
    EXECUTE _sql;
END;
$func$ LANGUAGE plpgsql VOLATILE
;
