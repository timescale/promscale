
-------------------------------------------------------------------------------
-- tag type functions
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.span_tag_type()
RETURNS SCHEMA_TRACING_PUBLIC.tag_type
AS $sql$
    SELECT (1<<0)::smallint::SCHEMA_TRACING_PUBLIC.tag_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.span_tag_type() TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.resource_tag_type()
RETURNS SCHEMA_TRACING_PUBLIC.tag_type
AS $sql$
    SELECT (1<<1)::smallint::SCHEMA_TRACING_PUBLIC.tag_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.resource_tag_type() TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.event_tag_type()
RETURNS SCHEMA_TRACING_PUBLIC.tag_type
AS $sql$
    SELECT (1<<2)::smallint::SCHEMA_TRACING_PUBLIC.tag_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.event_tag_type() TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.link_tag_type()
RETURNS SCHEMA_TRACING_PUBLIC.tag_type
AS $sql$
    SELECT (1<<3)::smallint::SCHEMA_TRACING_PUBLIC.tag_type
$sql$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.link_tag_type() TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.is_span_tag_type(_tag_type SCHEMA_TRACING_PUBLIC.tag_type)
RETURNS BOOLEAN
AS $sql$
    SELECT _tag_type & SCHEMA_TRACING_PUBLIC.span_tag_type() = SCHEMA_TRACING_PUBLIC.span_tag_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.is_span_tag_type(SCHEMA_TRACING_PUBLIC.tag_type) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.is_resource_tag_type(_tag_type SCHEMA_TRACING_PUBLIC.tag_type)
RETURNS BOOLEAN
AS $sql$
    SELECT _tag_type & SCHEMA_TRACING_PUBLIC.resource_tag_type() = SCHEMA_TRACING_PUBLIC.resource_tag_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.is_resource_tag_type(SCHEMA_TRACING_PUBLIC.tag_type) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.is_event_tag_type(_tag_type SCHEMA_TRACING_PUBLIC.tag_type)
RETURNS BOOLEAN
AS $sql$
    SELECT _tag_type & SCHEMA_TRACING_PUBLIC.event_tag_type() = SCHEMA_TRACING_PUBLIC.event_tag_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.is_event_tag_type(SCHEMA_TRACING_PUBLIC.tag_type) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.is_link_tag_type(_tag_type SCHEMA_TRACING_PUBLIC.tag_type)
RETURNS BOOLEAN
AS $sql$
    SELECT _tag_type & SCHEMA_TRACING_PUBLIC.link_tag_type() = SCHEMA_TRACING_PUBLIC.link_tag_type()
$sql$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.is_link_tag_type(SCHEMA_TRACING_PUBLIC.tag_type) TO prom_reader;

-------------------------------------------------------------------------------
-- trace tree functions
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.trace_tree(_trace_id SCHEMA_TRACING_PUBLIC.trace_id)
RETURNS TABLE
(
    trace_id SCHEMA_TRACING_PUBLIC.trace_id,
    parent_span_id bigint,
    span_id bigint,
    lvl int,
    path bigint[]
)
AS $func$
    WITH RECURSIVE x as
    (
        SELECT
            s1.parent_span_id,
            s1.span_id,
            1 as lvl,
            array[s1.span_id] as path
        FROM SCHEMA_TRACING.span s1
        WHERE s1.trace_id = _trace_id
        AND s1.parent_span_id IS NULL
        UNION ALL
        SELECT
            s2.parent_span_id,
            s2.span_id,
            x.lvl + 1 as lvl,
            x.path || s2.span_id as path
        FROM x
        INNER JOIN LATERAL
        (
            SELECT
                s2.parent_span_id,
                s2.span_id
            FROM SCHEMA_TRACING.span s2
            WHERE s2.trace_id = _trace_id
            AND s2.parent_span_id = x.span_id
        ) s2 ON (true)
    )
    SELECT
        _trace_id,
        x.parent_span_id,
        x.span_id,
        x.lvl,
        x.path
    FROM x
$func$ LANGUAGE sql STABLE STRICT PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.trace_tree(SCHEMA_TRACING_PUBLIC.trace_id) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.upstream_spans(_trace_id SCHEMA_TRACING_PUBLIC.trace_id, _span_id bigint, _max_dist int default null)
RETURNS TABLE
(
    trace_id SCHEMA_TRACING_PUBLIC.trace_id,
    parent_span_id bigint,
    span_id bigint,
    dist int,
    path bigint[]
)
AS $func$
    WITH RECURSIVE x as
    (
        SELECT
          s1.parent_span_id,
          s1.span_id,
          0 as dist,
          array[s1.span_id] as path
        FROM SCHEMA_TRACING.span s1
        WHERE s1.trace_id = _trace_id
        AND s1.span_id = _span_id
        UNION ALL
        SELECT
          s2.parent_span_id,
          s2.span_id,
          x.dist + 1 as dist,
          s2.span_id || x.path as path
        FROM x
        INNER JOIN LATERAL
        (
            SELECT
                s2.parent_span_id,
                s2.span_id
            FROM SCHEMA_TRACING.span s2
            WHERE s2.trace_id = _trace_id
            AND s2.span_id = x.parent_span_id
        ) s2 ON (true)
        WHERE (_max_dist IS NULL OR x.dist + 1 <= _max_dist)
    )
    SELECT
        _trace_id,
        x.parent_span_id,
        x.span_id,
        x.dist,
        x.path
    FROM x
$func$ LANGUAGE sql STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.upstream_spans(SCHEMA_TRACING_PUBLIC.trace_id, bigint, int) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.downstream_spans(_trace_id SCHEMA_TRACING_PUBLIC.trace_id, _span_id bigint, _max_dist int default null)
RETURNS TABLE
(
    trace_id SCHEMA_TRACING_PUBLIC.trace_id,
    parent_span_id bigint,
    span_id bigint,
    dist int,
    path bigint[]
)
AS $func$
    WITH RECURSIVE x as
    (
        SELECT
          s1.parent_span_id,
          s1.span_id,
          0 as dist,
          array[s1.span_id] as path
        FROM SCHEMA_TRACING.span s1
        WHERE s1.trace_id = _trace_id
        AND s1.span_id = _span_id
        UNION ALL
        SELECT
          s2.parent_span_id,
          s2.span_id,
          x.dist + 1 as dist,
          x.path || s2.span_id as path
        FROM x
        INNER JOIN LATERAL
        (
            SELECT *
            FROM SCHEMA_TRACING.span s2
            WHERE s2.trace_id = _trace_id
            AND s2.parent_span_id = x.span_id
        ) s2 ON (true)
        WHERE (_max_dist IS NULL OR x.dist + 1 <= _max_dist)
    )
    SELECT
        _trace_id,
        x.parent_span_id,
        x.span_id,
        x.dist,
        x.path
    FROM x
$func$ LANGUAGE sql STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.downstream_spans(SCHEMA_TRACING_PUBLIC.trace_id, bigint, int) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.sibling_spans(_trace_id SCHEMA_TRACING_PUBLIC.trace_id, _span_id bigint)
RETURNS TABLE
(
    trace_id SCHEMA_TRACING_PUBLIC.trace_id,
    parent_span_id bigint,
    span_id bigint
)
AS $func$
    SELECT
        _trace_id,
        s.parent_span_id,
        s.span_id
    FROM SCHEMA_TRACING.span s
    WHERE s.trace_id = _trace_id
    AND s.parent_span_id =
    (
        SELECT parent_span_id
        FROM SCHEMA_TRACING.span x
        WHERE x.trace_id = _trace_id
        AND x.span_id = _span_id
    )
$func$ LANGUAGE sql STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.sibling_spans(SCHEMA_TRACING_PUBLIC.trace_id, bigint) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.operation_calls(_start_time_min timestamptz, _start_time_max timestamptz)
RETURNS TABLE
(
    parent_operation_id bigint,
    child_operation_id bigint,
    cnt bigint
)
AS $func$
    SELECT
        parent.operation_id as parent_operation_id,
        child.operation_id as child_operation_id,
        count(*) as cnt
    FROM
        _ps_trace.span child
    INNER JOIN
        _ps_trace.span parent ON (parent.span_id = child.parent_span_id AND parent.trace_id = child.trace_id)
    WHERE
        child.start_time > _start_time_min AND child.start_time < _start_time_max AND
        parent.start_time > _start_time_min AND parent.start_time < _start_time_max
    GROUP BY parent.operation_id, child.operation_id
$func$ LANGUAGE sql
--Always prefer a mergejoin here since this is a rollup over a lot of data.
--a nested loop is sometimes preferred by the planner but is almost never right
--(it may only be right in cases where there is not a lot of data, and then it does
-- not matter)
SET  enable_nestloop = off
STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.operation_calls(timestamptz, timestamptz) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.span_tree(_trace_id SCHEMA_TRACING_PUBLIC.trace_id, _span_id bigint, _max_dist int default null)
RETURNS TABLE
(
    trace_id SCHEMA_TRACING_PUBLIC.trace_id,
    parent_span_id bigint,
    span_id bigint,
    dist int,
    is_upstream bool,
    is_downstream bool,
    path bigint[]
)
AS $func$
    SELECT
        trace_id,
        parent_span_id,
        span_id,
        dist,
        true as is_upstream,
        false as is_downstream,
        path
    FROM SCHEMA_TRACING_PUBLIC.upstream_spans(_trace_id, _span_id, _max_dist) u
    WHERE u.dist != 0
    UNION ALL
    SELECT
        trace_id,
        parent_span_id,
        span_id,
        dist,
        false as is_upstream,
        dist != 0 as is_downstream,
        path
    FROM SCHEMA_TRACING_PUBLIC.downstream_spans(_trace_id, _span_id, _max_dist) d
$func$ LANGUAGE sql STABLE PARALLEL SAFE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.span_tree(SCHEMA_TRACING_PUBLIC.trace_id, bigint, int) TO prom_reader;

-------------------------------------------------------------------------------
-- get / put functions
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.put_tag_key(_key SCHEMA_TRACING_PUBLIC.tag_k, _tag_type SCHEMA_TRACING_PUBLIC.tag_type)
RETURNS bigint
AS $func$
DECLARE
    _tag_key SCHEMA_TRACING.tag_key;
BEGIN
    SELECT * INTO _tag_key
    FROM SCHEMA_TRACING.tag_key k
    WHERE k.key = _key
    FOR UPDATE;

    IF NOT FOUND THEN
        INSERT INTO SCHEMA_TRACING.tag_key as k (key, tag_type)
        VALUES (_key, _tag_type)
        ON CONFLICT (key) DO
        UPDATE SET tag_type = k.tag_type | EXCLUDED.tag_type
        WHERE k.tag_type & EXCLUDED.tag_type = 0;

        SELECT * INTO STRICT _tag_key
        FROM SCHEMA_TRACING.tag_key k
        WHERE k.key = _key;
    ELSIF _tag_key.tag_type & _tag_type = 0 THEN
        UPDATE SCHEMA_TRACING.tag_key k
        SET tag_type = k.tag_type | _tag_type
        WHERE k.id = _tag_key.id;
    END IF;

    RETURN _tag_key.id;
END;
$func$
LANGUAGE plpgsql VOLATILE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.put_tag_key(SCHEMA_TRACING_PUBLIC.tag_k, SCHEMA_TRACING_PUBLIC.tag_type) TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.put_tag(_key SCHEMA_TRACING_PUBLIC.tag_k, _value SCHEMA_TRACING_PUBLIC.tag_v, _tag_type SCHEMA_TRACING_PUBLIC.tag_type)
RETURNS BIGINT
AS $func$
DECLARE
    _tag SCHEMA_TRACING.tag;
BEGIN
    SELECT * INTO _tag
    FROM SCHEMA_TRACING.tag
    WHERE key = _key
    AND value = _value
    FOR UPDATE;

    IF NOT FOUND THEN
        INSERT INTO SCHEMA_TRACING.tag as t (tag_type, key_id, key, value)
        SELECT
            _tag_type,
            k.id,
            _key,
            _value
        FROM SCHEMA_TRACING.tag_key k
        WHERE k.key = _key
        ON CONFLICT (key, value) DO
        UPDATE SET tag_type = t.tag_type | EXCLUDED.tag_type
        WHERE t.tag_type & EXCLUDED.tag_type = 0;

        SELECT * INTO STRICT _tag
        FROM SCHEMA_TRACING.tag
        WHERE key = _key
        AND value = _value;
    ELSIF _tag.tag_type & _tag_type = 0 THEN
        UPDATE SCHEMA_TRACING.tag as t
        SET tag_type = t.tag_type | _tag_type
        WHERE t.key = _key -- partition elimination
        AND t.id = _tag.id;
    END IF;

    RETURN _tag.id;
END;
$func$
LANGUAGE plpgsql VOLATILE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.put_tag(SCHEMA_TRACING_PUBLIC.tag_k, SCHEMA_TRACING_PUBLIC.tag_v, SCHEMA_TRACING_PUBLIC.tag_type) TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.get_tag_map(_tags jsonb)
RETURNS SCHEMA_TRACING_PUBLIC.tag_map
AS $func$
    SELECT coalesce(jsonb_object_agg(a.key_id, a.id), '{}')::SCHEMA_TRACING_PUBLIC.tag_map
    FROM jsonb_each(_tags) x
    INNER JOIN LATERAL
    (
        SELECT a.key_id, a.id
        FROM SCHEMA_TRACING.tag a
        WHERE x.key = a.key
        AND x.value = a.value
        LIMIT 1
    ) a on (true)
$func$
LANGUAGE SQL STABLE PARALLEL SAFE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.get_tag_map(jsonb) TO prom_reader;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.put_operation(_service_name text, _span_name text, _span_kind SCHEMA_TRACING_PUBLIC.span_kind)
RETURNS bigint
AS $func$
DECLARE
    _service_name_id bigint;
    _operation_id bigint;
BEGIN
    SELECT id INTO _service_name_id
    FROM SCHEMA_TRACING.tag
    WHERE key = 'service.name'
    AND key_id = 1
    AND value = to_jsonb(_service_name::text)
    ;

    IF NOT FOUND THEN
        INSERT INTO SCHEMA_TRACING.tag (tag_type, key, key_id, value)
        VALUES
        (
            SCHEMA_TRACING_PUBLIC.resource_tag_type(),
            'service.name',
            1,
            to_jsonb(_service_name::text)
        )
        ON CONFLICT DO NOTHING
        RETURNING id INTO _service_name_id;

        IF _service_name_id IS NULL THEN
            SELECT id INTO STRICT _service_name_id
            FROM SCHEMA_TRACING.tag
            WHERE key = 'service.name'
            AND key_id = 1
            AND value = to_jsonb(_service_name::text);
        END IF;
    END IF;

    SELECT id INTO _operation_id
    FROM SCHEMA_TRACING.operation
    WHERE service_name_id = _service_name_id
    AND span_kind = _span_kind
    AND span_name = _span_name;

    IF NOT FOUND THEN
        INSERT INTO SCHEMA_TRACING.operation (service_name_id, span_kind, span_name)
        VALUES
        (
            _service_name_id,
            _span_kind,
            _span_name
        )
        ON CONFLICT DO NOTHING
        RETURNING id INTO _operation_id;

        IF _operation_id IS NULL THEN
            SELECT id INTO STRICT _operation_id
            FROM SCHEMA_TRACING.operation
            WHERE service_name_id = _service_name_id
            AND span_kind = _span_kind
            AND span_name = _span_name;
        END IF;
    END IF;

    RETURN _operation_id;
END;
$func$
LANGUAGE plpgsql VOLATILE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.put_operation(text, text, SCHEMA_TRACING_PUBLIC.span_kind) TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.put_schema_url(_schema_url text)
RETURNS bigint
AS $func$
DECLARE
    _schema_url_id bigint;
BEGIN
    SELECT id INTO _schema_url_id
    FROM SCHEMA_TRACING.schema_url
    WHERE url = _schema_url;

    IF NOT FOUND THEN
        INSERT INTO SCHEMA_TRACING.schema_url (url)
        VALUES
        (
            _schema_url
        )
        ON CONFLICT DO NOTHING
        RETURNING id INTO _schema_url_id;

        IF _schema_url_id IS NULL THEN
            SELECT id INTO _schema_url_id
            FROM SCHEMA_TRACING.schema_url
            WHERE url = _schema_url;
        END IF;
    END IF;

    RETURN _schema_url_id;
END;
$func$
LANGUAGE plpgsql VOLATILE STRICT;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.put_schema_url(text) TO prom_writer;

CREATE OR REPLACE FUNCTION SCHEMA_TRACING_PUBLIC.put_instrumentation_lib(_name text, _version text, _schema_url_id bigint)
RETURNS bigint
AS $func$
DECLARE
    _inst_lib_id bigint;
BEGIN
    SELECT id INTO _inst_lib_id
    FROM SCHEMA_TRACING.instrumentation_lib
    WHERE name = _name
    AND version = _version
    AND schema_url_id = _schema_url_id;

    IF NOT FOUND THEN
        INSERT INTO SCHEMA_TRACING.instrumentation_lib (name, version, schema_url_id)
        VALUES
        (
            _name,
            _version,
            _schema_url_id
        )
        ON CONFLICT DO NOTHING
        RETURNING id INTO _inst_lib_id;

        IF _inst_lib_id IS NULL THEN
            SELECT id INTO STRICT _inst_lib_id
            FROM SCHEMA_TRACING.instrumentation_lib
            WHERE name = _name
            AND version = _version
            AND schema_url_id = _schema_url_id;
        END IF;
    END IF;

    RETURN _inst_lib_id;
END;
$func$
LANGUAGE plpgsql VOLATILE;
GRANT EXECUTE ON FUNCTION SCHEMA_TRACING_PUBLIC.put_instrumentation_lib(text, text, bigint) TO prom_writer;
