
DO $$
    DECLARE
        current_version INT;
        is_dirty BOOLEAN;
        original_message TEXT;
    BEGIN
        BEGIN
            SELECT version, dirty
              INTO STRICT current_version, is_dirty
              FROM public.prom_schema_migrations;
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS original_message = MESSAGE_TEXT;
            RAISE EXCEPTION 'could not determine the version of the timescale-prometheus connector that was installed due to: %', original_message
            USING HINT='This extension should not be created manually. It will be created by the timescale-prometheus connector and requires the connector to be installed first.';
            RETURN;
        END;

        IF current_version < 1 OR is_dirty THEN
            RAISE EXCEPTION 'the requisite version of the timescale-prometheus connector has not been installed'
            USING HINT='This extension should not be created manually. It will be created by the timescale-prometheus connector and requires the connector to be installed first.';
        END IF;
    END
$$;

-- Set the search path to one that will find all the definitions provided by the
-- connector. Since the connector can change the schemas it stores things
-- in we cannot just hardcode the searchpath, instead we switch the search path
-- based on the schemas declared by the connector.
DO $$
    DECLARE
        ext_schema TEXT;
        prom_schema TEXT;
        metric_schema TEXT;
        catalog_schema TEXT;
        new_path TEXT;
    BEGIN
        SELECT value FROM public.prom_installation_info
         WHERE key = 'extension schema'
          INTO ext_schema;
        SELECT value FROM public.prom_installation_info
         WHERE key = 'prometheus API schema'
          INTO prom_schema;
        SELECT value FROM public.prom_installation_info
         WHERE key = 'catalog schema'
          INTO catalog_schema;
        new_path := format('public,%s,%s,%s', ext_schema, prom_schema, catalog_schema);
        PERFORM set_config('search_path', new_path, false);
    END
$$;

DO $$
    BEGIN
        CREATE ROLE prom_reader;
    EXCEPTION WHEN duplicate_object THEN
        RAISE NOTICE 'role prom_reader already exists, skipping create';
        RETURN;
    END
$$;

CREATE OR REPLACE FUNCTION @extschema@.const_support(internal) RETURNS INTERNAL
AS '$libdir/timescale_prometheus_extra', 'const_support'
LANGUAGE C IMMUTABLE STRICT;
GRANT EXECUTE ON FUNCTION @extschema@.const_support(internal) TO prom_reader;


--wrapper around jsonb_each_text to give a better row_estimate
--for labels (10 not 100)
CREATE OR REPLACE FUNCTION @extschema@.label_jsonb_each_text(js jsonb,  OUT key text, OUT value text)
 RETURNS SETOF record
 LANGUAGE internal
 IMMUTABLE PARALLEL SAFE STRICT ROWS 10
AS $function$jsonb_each_text$function$;
GRANT EXECUTE ON FUNCTION @extschema@.label_jsonb_each_text(jsonb) TO prom_reader;

--wrapper around unnest to give better row estimate (10 not 100)
CREATE OR REPLACE FUNCTION @extschema@.label_unnest(label_array anyarray)
 RETURNS SETOF anyelement
 LANGUAGE internal
 IMMUTABLE PARALLEL SAFE STRICT ROWS 10
AS $function$array_unnest$function$;
GRANT EXECUTE ON FUNCTION @extschema@.label_unnest(anyarray) TO prom_reader;

---------------------  comparison functions ---------------------

CREATE OR REPLACE FUNCTION @extschema@.label_find_key_equal(key label_key, pat pattern)
RETURNS matcher_positive
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::matcher_positive
    FROM label l
    WHERE l.key = key and l.value = pat
$func$
LANGUAGE SQL STABLE PARALLEL SAFE
SUPPORT @extschema@.const_support;
GRANT EXECUTE ON FUNCTION @extschema@.label_find_key_equal(label_key, pattern) TO prom_reader;

CREATE OR REPLACE FUNCTION @extschema@.label_find_key_not_equal(key label_key, pat pattern)
RETURNS matcher_negative
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::matcher_negative
    FROM label l
    WHERE l.key = key and l.value = pat
$func$
LANGUAGE SQL STABLE PARALLEL SAFE
SUPPORT @extschema@.const_support;
GRANT EXECUTE ON FUNCTION @extschema@.label_find_key_not_equal(label_key, pattern) TO prom_reader;

CREATE OR REPLACE FUNCTION @extschema@.label_find_key_regex(key label_key, pat pattern)
RETURNS matcher_positive
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::matcher_positive
    FROM label l
    WHERE l.key = key and l.value ~ pat
$func$
LANGUAGE SQL STABLE PARALLEL SAFE
SUPPORT @extschema@.const_support;
GRANT EXECUTE ON FUNCTION @extschema@.label_find_key_regex(label_key, pattern) TO prom_reader;

CREATE OR REPLACE FUNCTION @extschema@.label_find_key_not_regex(key label_key, pat pattern)
RETURNS matcher_negative
AS $func$
    SELECT COALESCE(array_agg(l.id), array[]::int[])::matcher_negative
    FROM label l
    WHERE l.key = key and l.value ~ pat
$func$
LANGUAGE SQL STABLE PARALLEL SAFE
SUPPORT @extschema@.const_support;
GRANT EXECUTE ON FUNCTION @extschema@.label_find_key_not_regex(label_key, pattern) TO prom_reader;

CREATE OPERATOR @extschema@.== (
    LEFTARG = label_key,
    RIGHTARG = pattern,
    FUNCTION = @extschema@.label_find_key_equal
);

CREATE OPERATOR @extschema@.!== (
    LEFTARG = label_key,
    RIGHTARG = pattern,
    FUNCTION = @extschema@.label_find_key_not_equal
);

CREATE OPERATOR @extschema@.==~ (
    LEFTARG = label_key,
    RIGHTARG = pattern,
    FUNCTION = @extschema@.label_find_key_regex
);

CREATE OPERATOR @extschema@.!=~ (
    LEFTARG = label_key,
    RIGHTARG = pattern,
    FUNCTION = @extschema@.label_find_key_not_regex
);
