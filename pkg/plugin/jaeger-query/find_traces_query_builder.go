package jaeger_query

import (
	"fmt"
	"strings"
	"time"
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

// newFindtracesQueryBuilder returns a find-traces query builder.
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
func newFindtracesQueryBuilder() *builder {
	// todo: apply resource and span tags
	//	array_agg(s.resource_tags) resource_tags_arr,
	//	array_agg(s.span_tags) span_tags_arr
	return &builder{
		base: `
SELECT s.trace_id,
	   array_agg(s.span_id) span_ids,
       array_agg(s.parent_span_id) parent_span_ids,
       array_agg(s.start_time) start_times,
       array_agg(s.end_time) end_times,
       array_agg(s.span_kind) span_kinds,
       array_agg(s.dropped_tags_count) dropped_tags_counts,
       array_agg(s.dropped_events_count) dropped_events_counts,
       array_agg(s.dropped_link_count) dropped_link_counts,
       array_agg(s.trace_state) trace_states,
       array_agg(sch_url.url) schema_urls,
       array_agg(sn.name)     span_names
FROM   _ps_trace.span s
	   INNER JOIN _ps_trace.schema_url sch_url
   ON s.resource_schema_url_id = sch_url.id
       INNER JOIN _ps_trace.span_name sn
   ON s.name_id = sn.id`,
		serviceNameFormat:   `_ps_trace.val_text(s.resource_tags, 'service.name')='%s' AND `,
		operationNameFormat: `sn.name='%s' `,
		tagsFormat:          `_ps_trace.val_text(s.resource_tags, '%s')='%s' AND `,
		startRangeFormat:    `s.start_time BETWEEN '%s'::timestamptz AND '%s'::timestamptz AND `,
		durationRangeFormat: `(s.end_time - s.start_time) BETWEEN %s AND %s `,
		numTracesFormat:     `LIMIT %d`,
	}
}

func (b *builder) query() string {
	b.trimANDIfExists()
	return b.base
}

func (b *builder) trimANDIfExists() *builder {
	if strings.HasSuffix(b.base, "AND ") {
		b.base = strings.TrimSuffix(b.base, "AND ")
	}
	return b
}

func (b *builder) withWhere() *builder {
	b.base += " WHERE "
	return b
}

func (b *builder) groupBy() *builder {
	b.trimANDIfExists()
	b.base += " GROUP BY s.trace_id "
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

func (b *builder) withDurationRange(min, max time.Duration) *builder {
	b.base += fmt.Sprintf(b.durationRangeFormat, min, max)
	return b
}

func (b *builder) withNumTraces(n int32) *builder {
	b.trimANDIfExists()
	b.base += fmt.Sprintf(b.numTracesFormat, n)
	return b
}
