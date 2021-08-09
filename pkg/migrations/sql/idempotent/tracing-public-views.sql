
CREATE OR REPLACE VIEW SCHEMA_TRACING_PUBLIC.span AS
SELECT
    s.trace_id,
    s.span_id,
    s.trace_state,
    s.parent_span_id,
    s.parent_span_id is null as is_root_span,
    n.name,
    s.span_kind,
    s.start_time,
    s.end_time,
    tstzrange(s.start_time, s.end_time, '[]') as time_range,
    s.end_time - s.start_time as duration,
    s.span_tags,
    s.dropped_tags_count,
    s.event_time,
    s.dropped_events_count,
    s.dropped_link_count,
    s.status_code,
    s.status_message,
    il.name as instrumentation_lib_name,
    il.version as instrumentation_lib_version,
    u1.url as instrumentation_lib_schema_url,
    s.resource_tags,
    s.resource_dropped_tags_count,
    u2.url as resource_schema_url
FROM SCHEMA_TRACING.span s
LEFT OUTER JOIN SCHEMA_TRACING.span_name n ON (s.name_id = n.id)
LEFT OUTER JOIN SCHEMA_TRACING.instrumentation_lib il ON (s.instrumentation_lib_id = il.id)
LEFT OUTER JOIN SCHEMA_TRACING.schema_url u1 on (il.schema_url_id = u1.id)
LEFT OUTER JOIN SCHEMA_TRACING.schema_url u2 on (il.schema_url_id = u2.id)
;
GRANT SELECT ON SCHEMA_TRACING_PUBLIC.span to prom_reader;

CREATE OR REPLACE VIEW SCHEMA_TRACING_PUBLIC.event AS
SELECT
    e.trace_id,
    e.span_id,
    e.time,
    e.name as event_name,
    e.tags as event_tags,
    e.dropped_tags_count,
    s.trace_state,
    n.name as span_name,
    s.span_kind,
    s.start_time as span_start_time,
    s.end_time as span_end_time,
    tstzrange(s.start_time, s.end_time, '[]') as span_time_range,
    s.end_time - s.start_time as span_duration,
    s.span_tags,
    s.dropped_tags_count as dropped_span_tags_count,
    s.resource_tags,
    s.resource_dropped_tags_count,
    s.status_code,
    s.status_message
FROM SCHEMA_TRACING.event e
LEFT OUTER JOIN SCHEMA_TRACING.span s on (e.span_id = s.span_id AND e.trace_id = s.trace_id)
LEFT OUTER JOIN SCHEMA_TRACING.span_name n ON (s.name_id = n.id)
;
GRANT SELECT ON SCHEMA_TRACING_PUBLIC.event to prom_reader;

CREATE OR REPLACE VIEW SCHEMA_TRACING_PUBLIC.link AS
SELECT
    s1.trace_id                         ,
    s1.span_id                          ,
    s1.trace_state                      ,
    s1.parent_span_id                   ,
    s1.is_root_span                     ,
    s1.name                             ,
    s1.span_kind                        ,
    s1.start_time                       ,
    s1.end_time                         ,
    s1.time_range                       ,
    s1.duration                         ,
    s1.span_tags                        ,
    s1.dropped_tags_count               ,
    s1.event_time                       ,
    s1.dropped_events_count             ,
    s1.dropped_link_count               ,
    s1.status_code                      ,
    s1.status_message                   ,
    s1.instrumentation_lib_name         ,
    s1.instrumentation_lib_version      ,
    s1.instrumentation_lib_schema_url   ,
    s1.resource_tags                    ,
    s1.resource_dropped_tags_count      ,
    s1.resource_schema_url              ,
    s2.trace_id                         as linked_trace_id                   ,
    s2.span_id                          as linked_span_id                    ,
    s2.trace_state                      as linked_trace_state                ,
    s2.parent_span_id                   as linked_parent_span_id             ,
    s2.is_root_span                     as linked_is_root_span               ,
    s2.name                             as linked_name                       ,
    s2.span_kind                        as linked_span_kind                  ,
    s2.start_time                       as linked_start_time                 ,
    s2.end_time                         as linked_end_time                   ,
    s2.time_range                       as linked_time_range                 ,
    s2.duration                         as linked_duration                   ,
    s2.span_tags                        as linked_span_tags                  ,
    s2.dropped_tags_count               as linked_dropped_tags_count         ,
    s2.event_time                       as linked_event_time                 ,
    s2.dropped_events_count             as linked_dropped_events_count       ,
    s2.dropped_link_count               as linked_dropped_link_count         ,
    s2.status_code                      as linked_status_code                ,
    s2.status_message                   as linked_status_message             ,
    s2.instrumentation_lib_name         as linked_inst_lib_name              ,
    s2.instrumentation_lib_version      as linked_inst_lib_version           ,
    s2.instrumentation_lib_schema_url   as linked_inst_lib_schema_url        ,
    s2.resource_tags                    as linked_resource_tags              ,
    s2.resource_dropped_tags_count      as linked_resource_dropped_tags_count,
    s2.resource_schema_url              as linked_resource_schema_url        ,
    k.tags as link_tags,
    k.dropped_tags_count as dropped_link_tags_count
FROM SCHEMA_TRACING.link k
LEFT OUTER JOIN SCHEMA_TRACING_PUBLIC.span s1 on (k.span_id = s1.span_id and k.trace_id = s1.trace_id)
LEFT OUTER JOIN SCHEMA_TRACING_PUBLIC.span s2 on (k.linked_span_id = s2.span_id and k.linked_trace_id = s2.trace_id)
;
GRANT SELECT ON SCHEMA_TRACING_PUBLIC.link to prom_reader;
