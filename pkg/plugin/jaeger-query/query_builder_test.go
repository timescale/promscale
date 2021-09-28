// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package jaeger_query

import (
	"testing"
	"time"

	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
	"github.com/stretchr/testify/require"
)

type queryCase struct {
	name          string
	kind          queryKind
	q             storage_v1.TraceQueryParameters
	hasDuration   bool
	expectedQuery string
}

var tcs = []queryCase{
	{
		name: "ideal query",
		kind: spansQuery,
		q: storage_v1.TraceQueryParameters{
			ServiceName:   "demo_service",
			OperationName: "demo_operation",
			Tags: map[string]string{
				"a": "b",
			},
			StartTimeMin: time.Unix(1, 0),
			StartTimeMax: time.Unix(10, 0),
			DurationMin:  time.Second,
			DurationMax:  time.Minute,
			NumTraces:    20,
		},
		hasDuration: true,
		expectedQuery: `SELECT s.trace_id,
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
	   ps_trace.jsonb(s.resource_tags) resource_tags,
	   ps_trace.jsonb(s.span_tags) span_tags
FROM   _ps_trace.span s
	   LEFT JOIN _ps_trace.schema_url sch_url
   ON s.resource_schema_url_id = sch_url.id
       INNER JOIN _ps_trace.span_name sn
   ON s.name_id = sn.id  WHERE ps_trace.val_text(s.resource_tags, 'service.name')='demo_service' AND sn.name='demo_operation' AND ps_trace.val_text(s.resource_tags, 'a')='b' AND s.start_time BETWEEN '1970-01-01T05:30:01+05:30'::timestamptz AND '1970-01-01T05:30:10+05:30'::timestamptz AND (s.end_time - s.start_time) BETWEEN $1 AND $2  ORDER BY s.trace_id LIMIT 20`,
	},
	{
		name: "no start time & duration",
		kind: spansQuery,
		q: storage_v1.TraceQueryParameters{
			ServiceName:   "demo_service",
			OperationName: "demo_operation",
			Tags: map[string]string{
				"a": "b",
			},
			NumTraces: 20,
		},
		hasDuration: false,
		expectedQuery: `SELECT s.trace_id,
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
	   ps_trace.jsonb(s.resource_tags) resource_tags,
	   ps_trace.jsonb(s.span_tags) span_tags
FROM   _ps_trace.span s
	   LEFT JOIN _ps_trace.schema_url sch_url
   ON s.resource_schema_url_id = sch_url.id
       INNER JOIN _ps_trace.span_name sn
   ON s.name_id = sn.id  WHERE ps_trace.val_text(s.resource_tags, 'service.name')='demo_service' AND sn.name='demo_operation' AND ps_trace.val_text(s.resource_tags, 'a')='b'  ORDER BY s.trace_id LIMIT 20`,
	},
	{
		name: "no tags",
		kind: spansQuery,
		q: storage_v1.TraceQueryParameters{
			ServiceName:   "demo_service",
			OperationName: "demo_operation",
			StartTimeMin:  time.Unix(1, 0),
			StartTimeMax:  time.Unix(10, 0),
			DurationMin:   time.Second,
			DurationMax:   time.Minute,
			NumTraces:     20,
		},
		hasDuration: true,
		expectedQuery: `SELECT s.trace_id,
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
	   ps_trace.jsonb(s.resource_tags) resource_tags,
	   ps_trace.jsonb(s.span_tags) span_tags
FROM   _ps_trace.span s
	   LEFT JOIN _ps_trace.schema_url sch_url
   ON s.resource_schema_url_id = sch_url.id
       INNER JOIN _ps_trace.span_name sn
   ON s.name_id = sn.id  WHERE ps_trace.val_text(s.resource_tags, 'service.name')='demo_service' AND sn.name='demo_operation' AND s.start_time BETWEEN '1970-01-01T05:30:01+05:30'::timestamptz AND '1970-01-01T05:30:10+05:30'::timestamptz AND (s.end_time - s.start_time) BETWEEN $1 AND $2  ORDER BY s.trace_id LIMIT 20`,
	},
	{
		name: "no operation name",
		kind: spansQuery,
		q: storage_v1.TraceQueryParameters{
			ServiceName: "demo_service",
			Tags: map[string]string{
				"c": "d",
			},
			StartTimeMin: time.Unix(1, 0),
			StartTimeMax: time.Unix(10, 0),
			DurationMin:  time.Second,
			DurationMax:  time.Minute,
			NumTraces:    20,
		},
		hasDuration: true,
		expectedQuery: `SELECT s.trace_id,
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
	   ps_trace.jsonb(s.resource_tags) resource_tags,
	   ps_trace.jsonb(s.span_tags) span_tags
FROM   _ps_trace.span s
	   LEFT JOIN _ps_trace.schema_url sch_url
   ON s.resource_schema_url_id = sch_url.id
       INNER JOIN _ps_trace.span_name sn
   ON s.name_id = sn.id  WHERE ps_trace.val_text(s.resource_tags, 'service.name')='demo_service' AND ps_trace.val_text(s.resource_tags, 'c')='d' AND s.start_time BETWEEN '1970-01-01T05:30:01+05:30'::timestamptz AND '1970-01-01T05:30:10+05:30'::timestamptz AND (s.end_time - s.start_time) BETWEEN $1 AND $2  ORDER BY s.trace_id LIMIT 20`,
	},
	{
		name: "no operation name & num traces",
		kind: spansQuery,
		q: storage_v1.TraceQueryParameters{
			ServiceName: "demo_service",
			Tags: map[string]string{
				"c": "d",
			},
			StartTimeMin: time.Unix(1, 0),
			StartTimeMax: time.Unix(10, 0),
			DurationMin:  time.Second,
			DurationMax:  time.Minute,
		},
		hasDuration: true,
		expectedQuery: `SELECT s.trace_id,
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
	   ps_trace.jsonb(s.resource_tags) resource_tags,
	   ps_trace.jsonb(s.span_tags) span_tags
FROM   _ps_trace.span s
	   LEFT JOIN _ps_trace.schema_url sch_url
   ON s.resource_schema_url_id = sch_url.id
       INNER JOIN _ps_trace.span_name sn
   ON s.name_id = sn.id  WHERE ps_trace.val_text(s.resource_tags, 'service.name')='demo_service' AND ps_trace.val_text(s.resource_tags, 'c')='d' AND s.start_time BETWEEN '1970-01-01T05:30:01+05:30'::timestamptz AND '1970-01-01T05:30:10+05:30'::timestamptz AND (s.end_time - s.start_time) BETWEEN $1 AND $2  ORDER BY s.trace_id`,
	},
	{
		name: "ideal query for trace ids only",
		kind: traceIdsQuery,
		q: storage_v1.TraceQueryParameters{
			ServiceName:   "demo_service",
			OperationName: "demo_operation",
			Tags: map[string]string{
				"a": "b",
			},
			StartTimeMin: time.Unix(1, 0),
			StartTimeMax: time.Unix(10, 0),
			DurationMin:  time.Second,
			DurationMax:  time.Minute,
			NumTraces:    20,
		},
		hasDuration: true,
		expectedQuery: `SELECT s.trace_id
FROM   _ps_trace.span s
	   INNER JOIN _ps_trace.schema_url sch_url
   ON s.resource_schema_url_id = sch_url.id
       INNER JOIN _ps_trace.span_name sn
   ON s.name_id = sn.id  WHERE ps_trace.val_text(s.resource_tags, 'service.name')='demo_service' AND sn.name='demo_operation' AND ps_trace.val_text(s.resource_tags, 'a')='b' AND s.start_time BETWEEN '1970-01-01T05:30:01+05:30'::timestamptz AND '1970-01-01T05:30:10+05:30'::timestamptz AND (s.end_time - s.start_time) BETWEEN $1 AND $2  GROUP BY s.trace_idLIMIT 20`,
	},
}

func runTest(t testing.TB) {
	for _, tc := range tcs {
		query, hasDuration := buildQuery(tc.kind, &tc.q)
		require.Equal(t, tc.hasDuration, hasDuration, tc.name)
		require.Equal(t, tc.expectedQuery, query, tc.name)
	}
}

func TestQueryBuilder(t *testing.T) {
	runTest(t)
}

func BenchmarkQueryBuilder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		runTest(b)
	}
}
