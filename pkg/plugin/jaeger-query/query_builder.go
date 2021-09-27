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
	base                string
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
	selectForSpans = `
SELECT s.trace_id,
	   s.span_id,
       s.parent_span_id,
       s.start_time start_times,
       s.end_time end_times,
       s.span_kind,
       s.dropped_tags_count dropped_tags_counts,
       s.dropped_events_count dropped_events_counts,
       s.dropped_link_count dropped_link_counts,
       s.trace_state trace_states,
       sch_url.url schema_urls,
       sn.name     span_names,
	   _ps_trace.jsonb(s.resource_tags) resource_tags,
	   _ps_trace.jsonb(s.span_tags) span_tags
FROM   _ps_trace.span s
	   INNER JOIN _ps_trace.schema_url sch_url
   ON s.resource_schema_url_id = sch_url.id
       INNER JOIN _ps_trace.span_name sn
   ON s.name_id = sn.id`
	selectForTraceIds = `
SELECT s.trace_id
FROM   _ps_trace.span s
	   INNER JOIN _ps_trace.schema_url sch_url
   ON s.resource_schema_url_id = sch_url.id
       INNER JOIN _ps_trace.span_name sn
   ON s.name_id = sn.id`
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
		base:                ``,
		serviceNameFormat:   `_ps_trace.val_text(s.resource_tags, 'service.name')='%s' AND `,
		operationNameFormat: `sn.name='%s' AND `,
		tagsFormat:          `_ps_trace.val_text(s.resource_tags, '%s')='%s' AND `,
		startRangeFormat:    `s.start_time BETWEEN '%s'::timestamptz AND '%s'::timestamptz AND `,
		durationRangeFormat: `(s.end_time - s.start_time) BETWEEN $1 AND $2 `,
		numTracesFormat:     `LIMIT %d`,
	}
	switch kind {
	case spansQuery:
		b.base = selectForSpans
	case traceIdsQuery:
		b.base = selectForTraceIds
	default:
		panic("wrong query kind")
	}
	return b
}

func (b *builder) query() string {
	b.trimANDIfExists()
	return b.base
}

func (b *builder) trimANDIfExists() *builder {
	b.base = strings.TrimSuffix(b.base, "AND ")
	return b
}

func (b *builder) withWhere() *builder {
	b.base += " WHERE "
	return b
}

func (b *builder) orderByTraceId() *builder {
	b.trimANDIfExists()
	b.base += " ORDER BY s.trace_id "
	return b
}

func (b *builder) groupByTraceId() *builder {
	b.trimANDIfExists()
	b.base += " GROUP BY s.trace_id"
	return b
}

func (b *builder) withServiceName(s string) *builder {
	b.base += fmt.Sprintf(b.serviceNameFormat, s)
	return b
}

func (b *builder) withOperationName(s string) *builder {
	b.base += fmt.Sprintf(b.operationNameFormat, s)
	return b
}

func (b *builder) withResourceTags(tags map[string]string) *builder {
	for k, v := range tags {
		b.base += fmt.Sprintf(b.tagsFormat, k, v)
	}
	return b
}

func (b *builder) withStartRange(min, max time.Time) *builder {
	b.base += fmt.Sprintf(b.startRangeFormat, min.Format(time.RFC3339), max.Format(time.RFC3339))
	return b
}

// withDurationRange when using this function, provide the min and max duration in args.
func (b *builder) withDurationRange() *builder {
	b.base += b.durationRangeFormat
	return b
}

func (b *builder) withNumTraces(n int32) *builder {
	b.trimANDIfExists()
	b.base += fmt.Sprintf(b.numTracesFormat, n)
	return b
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
		builder.orderByTraceId()
	case traceIdsQuery:
		builder.groupByTraceId()
	default:
		panic("wrong query kind")
	}

	if q.NumTraces != 0 {
		builder.withNumTraces(q.NumTraces)
	}

	return builder.query(), hasDuration
}
