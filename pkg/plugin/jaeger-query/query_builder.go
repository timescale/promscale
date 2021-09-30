// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package jaeger_query

import (
	"fmt"
	"strings"
	"time"

	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
)

type builder struct {
	base                *strings.Builder
	serviceNameFormat   string
	operationNameFormat string
	tagsFormat          string
	startRangeFormat    string
	durationRangeFormat string
	numTracesFormat     string
}

type queryKind uint8

const (
	spansQuery queryKind = iota
	traceIdsQuery
)

const (
	selectForSpans = `SELECT s.trace_id,
	   	s.span_id,
       	s.parent_span_id,
       	s.start_time 						start_times,
       	s.end_time 							end_times,
       	s.span_kind,
       	s.dropped_tags_count 				dropped_tags_counts,
       	s.dropped_events_count 				dropped_events_counts,
       	s.dropped_link_count 				dropped_link_counts,
       	s.trace_state 						trace_states,
       	sch_url.url 						schema_urls,
       	sn.name     						span_names,
	   	ps_trace.jsonb(s.resource_tags) 	resource_tags,
	   	ps_trace.jsonb(s.span_tags) 		span_tags,
	   	array_agg(e.name)                 	event_names,
       	array_agg(e.time)                 	event_times,
	   	array_agg(e.dropped_tags_count)   	event_dropped_tags_count,
	   	jsonb_agg(ps_trace.jsonb(e.tags)) 	event_tags,
	   	inst_l.name 						library_name,
	   	inst_l.version 						library_version,
		sch_url_2.url 						library_schema_url
FROM   _ps_trace.span s
	   	LEFT JOIN _ps_trace.schema_url sch_url
   	ON s.resource_schema_url_id = sch_url.id
       	INNER JOIN _ps_trace.span_name sn
   	ON s.name_id = sn.id
	   	LEFT JOIN _ps_trace.event e
   	ON e.span_id = s.span_id
	   AND e.trace_id = s.trace_id
		LEFT JOIN _ps_trace.instrumentation_lib inst_l
	ON s.instrumentation_lib_id = inst_l.id
		LEFT JOIN _ps_trace.schema_url sch_url_2
	ON sch_url_2.id = inst_l.schema_url_id `
	selectForTraceIds = `SELECT s.trace_id
FROM   _ps_trace.span s
	   INNER JOIN _ps_trace.schema_url sch_url
   ON s.resource_schema_url_id = sch_url.id
       INNER JOIN _ps_trace.span_name sn
   ON s.name_id = sn.id `
)

// newTracesQueryBuilder returns a find-traces query builder. Depending on kind, the builder.query()
// gives out the type of query, i.e., whether to fetch spans or to fetch traceIds.
//
// The order of call should be strictly:
// 0. withWhere
// 1. withServiceName
// 2. withOperationName
// 3. withResourceTags
// 4. withStartRange
// 5. withDurationRange
// 6. withNumTraces
//
// Note: You can skip some functions from calling, depending on the query parameters
// while maintaining the order. For applying anything from 1 to 5, you need to first call
// withWhere.
func newTracesQueryBuilder(kind queryKind) *builder {
	b := &builder{
		base:                new(strings.Builder),
		serviceNameFormat:   `ps_trace.val_text(s.resource_tags, 'service.name')='%s' AND `,
		operationNameFormat: `sn.name='%s' AND `,
		tagsFormat:          `ps_trace.val_text(s.resource_tags, '%s')='%s' AND `,
		startRangeFormat:    `s.start_time BETWEEN '%s'::timestamptz AND '%s'::timestamptz AND `,
		durationRangeFormat: `(s.end_time - s.start_time) BETWEEN $1 AND $2 `,
		numTracesFormat:     `LIMIT %d`,
	}
	switch kind {
	case spansQuery:
		expandAndWrite(b.base, selectForSpans)
	case traceIdsQuery:
		expandAndWrite(b.base, selectForTraceIds)
	default:
		panic("wrong query kind")
	}
	return b
}

func (b *builder) query() string {
	b.trimANDIfExists()
	return strings.TrimSpace(b.base.String())
}

const and = "AND "

func (b *builder) trimANDIfExists() *builder {
	existing := b.base.String()
	if strings.HasSuffix(existing, and) {
		newStr := strings.TrimSuffix(b.base.String(), "AND ")
		b.base.Reset()
		expandAndWrite(b.base, newStr)
	}
	return b
}

func (b *builder) withWhere() *builder {
	expandAndWrite(b.base, " WHERE ")
	return b
}

func (b *builder) orderByTraceId() *builder {
	b.trimANDIfExists()
	expandAndWrite(b.base, " ORDER BY s.trace_id ")
	return b
}

func (b *builder) groupByTraceId() *builder {
	b.trimANDIfExists()
	expandAndWrite(b.base, " GROUP BY s.trace_id")
	return b
}

func (b *builder) groupBySpanTableAttributes() *builder {
	b.trimANDIfExists()
	expandAndWrite(b.base, ` GROUP BY s.trace_id,
	s.span_id,
	s.parent_span_id,
	s.start_time,
	s.end_time,
	sch_url.url,
	sn.name,
	s.resource_tags,
	s.span_tags,
	inst_l.name,
	inst_l.version,
	sch_url_2.url `)
	return b
}

func (b *builder) withServiceName(s string) *builder {
	expandAndWrite(b.base, fmt.Sprintf(b.serviceNameFormat, s))
	return b
}

func (b *builder) withOperationName(s string) *builder {
	expandAndWrite(b.base, fmt.Sprintf(b.operationNameFormat, s))
	return b
}

func (b *builder) withResourceTags(tags map[string]string) *builder {
	for k, v := range tags {
		expandAndWrite(b.base, fmt.Sprintf(b.tagsFormat, k, v))
	}
	return b
}

func (b *builder) withStartRange(min, max time.Time) *builder {
	expandAndWrite(b.base, fmt.Sprintf(b.startRangeFormat, min.Format(time.RFC3339), max.Format(time.RFC3339)))
	return b
}

// withDurationRange when using this function, provide the min and max duration in args.
func (b *builder) withDurationRange() *builder {
	expandAndWrite(b.base, b.durationRangeFormat)
	return b
}

func (b *builder) withNumTraces(n int32) *builder {
	b.trimANDIfExists()
	expandAndWrite(b.base, fmt.Sprintf(b.numTracesFormat, n))
	return b
}

func expandAndWrite(b *strings.Builder, s string) {
	b.Grow(len(s))
	b.WriteString(s)
}

func buildQuery(kind queryKind, q *storage_v1.TraceQueryParameters) (query string, hasDuration bool) {
	builder := newTracesQueryBuilder(kind)
	if len(q.ServiceName) > 0 {
		q.ServiceName = q.ServiceName[1 : len(q.ServiceName)-1] // temporary, based on trace gen behaviour
		builder.withWhere().withServiceName(q.ServiceName)
	}
	if len(q.OperationName) > 0 {
		builder.withOperationName(q.OperationName)
	}
	if len(q.Tags) > 0 {
		builder.withResourceTags(q.Tags)
	}

	var defaultTime time.Time
	if q.StartTimeMin != defaultTime && q.StartTimeMax != defaultTime {
		builder.withStartRange(q.StartTimeMin, q.StartTimeMax)
	}

	var defaultDuration time.Duration
	if q.DurationMin != defaultDuration && q.DurationMax != defaultDuration {
		hasDuration = true
		builder.withDurationRange()
	}

	switch kind {
	case spansQuery:
		builder.groupBySpanTableAttributes().orderByTraceId()
	case traceIdsQuery:
		builder.groupByTraceId()
	default:
		panic("wrong query kind")
	}

	if q.NumTraces != 0 {
		builder.withNumTraces(q.NumTraces)
	}

	return strings.TrimSpace(builder.query()), hasDuration
}
