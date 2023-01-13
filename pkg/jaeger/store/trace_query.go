// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package store

import (
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

const (
	subqueryFormat = `
	SELECT
		trace_sub.trace_id, start_time_max - $%[1]d::interval as time_low, start_time_max + $%[1]d::interval as time_high
	FROM (
		SELECT
			s.trace_id,
			max(start_time) as start_time_max
		FROM _ps_trace.span s
		WHERE
			%[2]s
		GROUP BY s.trace_id
	) as trace_sub
	ORDER BY trace_sub.start_time_max DESC
	`
	subqueryTimeRangeForTraceID = `
		SELECT
			s.trace_id,
			s.start_time - $1::interval as time_low,
			s.start_time + $1::interval as time_high
		FROM _ps_trace.span s
		WHERE
			s.trace_id = $2
		LIMIT 1
	`

	// PostgreSQL badly overestimates the number of rows returned if the complete trace query
	// uses an IN clause on trace_id, but gives good estimates for equality conditions. So, leverage an INNER
	// JOIN LATERAL to provide an equality condition on the complete trace.
	//
	// A lateral join is used for the `link` and `event` table instead of using
	// directly `array_agg` calls in the main SELECT clause to avoid returning
	// duplicated values when the cartesian product of event x link is greater
	// than 1.
	//
	// The GROUP BY clause of the query can be removed because we are not doing
	// any kind of aggregation that would require it, but the tests we did showed
	// that with the GROUP BY the resulting query plan is better. In the
	// no-group-by case the join between the trace_ids with clause and the
	// main _ps_trace.span table happens as a hash_join where both sides scan
	// the span table.
	//
	// Query plans:
	//
	// - With GROUP BY https://explain.dalibo.com/plan/e6c74995bc36begd
	// - Without GROUP BY https://explain.dalibo.com/plan/f09259cd21g57dh3
	findTraceSQLFormat = `
	WITH trace_ids AS (
		%s
	)
	SELECT
		complete_trace.*
	FROM
		trace_ids
	INNER JOIN LATERAL (
		SELECT
			s.trace_id,
			s.span_id,
			s.parent_span_id,
			s.start_time start_times,
			s.end_time end_times,
			o.span_kind,
			s.dropped_tags_count   dropped_tags_counts,
			s.dropped_events_count dropped_events_counts,
			s.dropped_link_count dropped_link_counts,
			s.status_code status_code_string,
			s.status_message,
			s.trace_state trace_states,
			s_url.url schema_urls,
			o.span_name span_names,
			_ps_trace.tag_map_denormalize(s.resource_tags) resource_tags,
			_ps_trace.tag_map_denormalize(s.span_tags) span_tags,
			event.event_names,
			event.event_times,
			event.event_dropped_tags_count,
			event.event_tags,
			inst_lib.name library_name,
			inst_lib.version library_version,
			inst_lib_url.url library_schema_url,
			links_linked_trace_ids,
			links_linked_span_ids,
			links_trace_states,
			links_dropped_tags_count,
			links_tags
		FROM
			_ps_trace.span s
		INNER JOIN
			_ps_trace.operation o ON (s.operation_id = o.id)
		LEFT JOIN
			_ps_trace.schema_url s_url ON s.resource_schema_url_id = s_url.id
		LEFT JOIN
			_ps_trace.instrumentation_lib inst_lib ON s.instrumentation_lib_id = inst_lib.id
		LEFT JOIN
			_ps_trace.schema_url inst_lib_url ON inst_lib_url.id = inst_lib.schema_url_id
		LEFT JOIN LATERAL (
			SELECT
				array_agg(e.name ORDER BY e.event_nbr) event_names,
				array_agg(e.time ORDER BY e.event_nbr) event_times,
				array_agg(e.dropped_tags_count ORDER BY e.event_nbr) event_dropped_tags_count,
				array_agg(_ps_trace.tag_map_denormalize(e.tags) ORDER BY e.event_nbr) event_tags
			FROM _ps_trace.event as e
			WHERE e.trace_id = s.trace_id AND e.span_id = s.span_id
				AND e.time > trace_ids.time_low AND e.time < trace_ids.time_high
		) as event ON (TRUE)
		LEFT JOIN LATERAL (
			SELECT
				array_agg(lk.linked_trace_id ORDER BY lk.link_nbr) links_linked_trace_ids,
				array_agg(lk.linked_span_id ORDER BY lk.link_nbr) links_linked_span_ids,
				array_agg(lk.trace_state ORDER BY lk.link_nbr) links_trace_states,
				array_agg(lk.dropped_tags_count ORDER BY lk.link_nbr) links_dropped_tags_count,
				array_agg(_ps_trace.tag_map_denormalize(lk.tags) ORDER BY lk.link_nbr) links_tags
			FROM _ps_trace.link as lk
			WHERE lk.trace_id = s.trace_id AND lk.span_id = s.span_id
				AND lk.span_start_time > trace_ids.time_low AND lk.span_start_time < trace_ids.time_high
		) as link ON (TRUE)
		WHERE
			s.trace_id = trace_ids.trace_id
			AND s.start_time > trace_ids.time_low AND s.start_time < trace_ids.time_high
		GROUP BY
			s.trace_id,
			s.span_id,
			s.parent_span_id,
			s.start_time,
			s.end_time,
			s.resource_tags,
			s.span_tags,
			o.span_name,
			o.span_kind,
			s_url.url,
			inst_lib.name,
			inst_lib.version,
			inst_lib_url.url,
			event_names,
			event_times,
			event_dropped_tags_count,
			event_tags,
			links_linked_trace_ids,
			links_linked_span_ids,
			links_trace_states,
			links_dropped_tags_count,
			links_tags
	) AS complete_trace ON (TRUE)
`

	// Keys used to represent OTLP constructs from Jaeger tags which are then dropped from the tag map.
	TagError         = "error"
	TagHostname      = "hostname"
	TagJaegerVersion = "jaeger.version"
	TagSpanKind      = "span.kind"
	TagW3CTraceState = "w3c.tracestate"
	TagEventName     = "event"
)

type Builder struct {
	cfg *Config
}

func NewBuilder(cfg *Config) *Builder {
	return &Builder{cfg}
}

func (b *Builder) findTracesQuery(q *spanstore.TraceQueryParameters, tInfo *tagsInfo) (string, []interface{}) {
	subquery, params := b.BuildTraceIDSubquery(q, tInfo)
	return fmt.Sprintf(findTraceSQLFormat, subquery), params
}

func (b *Builder) findTraceIDsQuery(q *spanstore.TraceQueryParameters, tInfo *tagsInfo) (string, []interface{}) {
	return b.BuildTraceIDSubquery(q, tInfo)
}

func getUUIDFromTraceID(traceID model.TraceID) (pgtype.UUID, error) {
	var buf [16]byte
	var uuid pgtype.UUID
	n, err := traceID.MarshalTo(buf[:])
	if n != 16 || err != nil {
		return uuid, fmt.Errorf("marshaling TraceID: %w", err)
	}

	return pgtype.UUID{Bytes: buf, Valid: true}, nil
}

func (b *Builder) getTraceQuery(traceID model.TraceID) (string, []interface{}, error) {
	traceUUID, err := getUUIDFromTraceID(traceID)
	if err != nil {
		return "", nil, fmt.Errorf("TraceID to UUID conversion: %w", err)
	}

	//it may seem silly to build a traceID subquery when we know the traceID
	//but, this allows us to get the time range of the trace for the rest of the query.
	subquery, params := b.BuildTraceTimeRangeSubqueryForTraceID(traceUUID)
	return fmt.Sprintf(findTraceSQLFormat, subquery), params, nil
}

func (b *Builder) buildOperationSubquery(q *spanstore.TraceQueryParameters, tInfo *tagsInfo, params []interface{}) (string, []interface{}) {
	operationClauses := tInfo.operationClauses
	if len(q.ServiceName) > 0 {
		params = append(params, q.ServiceName)
		qual := fmt.Sprintf(`
		service_name_id = (
			SELECT id
			FROM _ps_trace.tag
			WHERE key = 'service.name'
			AND key_id = 1
			AND _prom_ext.jsonb_digest(value) = _prom_ext.jsonb_digest(to_jsonb($%d::text))
		)`, len(params))
		operationClauses = append(operationClauses, qual)
	}
	if len(q.OperationName) > 0 {
		params = append(params, q.OperationName)
		qual := fmt.Sprintf(`span_name = $%d`, len(params))
		operationClauses = append(operationClauses, qual)
	}

	if len(operationClauses) > 0 {
		subquery := fmt.Sprintf(`
			   SELECT
			     id
			   FROM _ps_trace.operation op
			   WHERE
			      %s
		`, strings.Join(operationClauses, " AND "))
		return subquery, params
	}
	return "", params
}

func (b *Builder) buildEventSubquery(q *spanstore.TraceQueryParameters, clauses []string, params []interface{}) (string, []interface{}) {
	var defaultTime time.Time
	if q.StartTimeMin != defaultTime {
		params = append(params, q.StartTimeMin.Add(-b.cfg.MaxTraceDuration))
		clauses = append(clauses, fmt.Sprintf(`e.time >= $%d`, len(params)))
	}
	if q.StartTimeMax != defaultTime {
		params = append(params, q.StartTimeMax.Add(b.cfg.MaxTraceDuration))
		clauses = append(clauses, fmt.Sprintf(`e.time <= $%d`, len(params)))
	}
	subquery := fmt.Sprintf(
		`SELECT 1
		 FROM _ps_trace.event e
		 WHERE s.trace_id = e.trace_id AND s.span_id = e.span_id AND %s`, strings.Join(clauses, " AND "))
	return subquery, params
}

func (b *Builder) buildTagClauses(q *spanstore.TraceQueryParameters, tInfo *tagsInfo, params []interface{}) (string, []interface{}) {
	clauses := make([]string, 0, len(tInfo.generalTags))
	for _, tag := range tInfo.generalTags {
		tagClauses := make([]string, 0, 3)
		params = append(params, tag.jsonbPairArray)
		if tag.isSpan {
			tagClauses = append(tagClauses, fmt.Sprintf("(s.span_tags @> ANY($%d::jsonb[]))", len(params)))
		}
		if tag.isResource {
			tagClauses = append(tagClauses, fmt.Sprintf("(s.resource_tags @> ANY($%d::jsonb[]))", len(params)))
		}
		if tag.isEvent {
			var subquery string
			subquery, params = b.buildEventSubquery(q, []string{fmt.Sprintf("(e.tags @> ANY($%d::jsonb[]))", len(params))}, params)
			tagClauses = append(tagClauses, fmt.Sprintf("EXISTS(%s)", subquery))
		}
		clauses = append(clauses, "("+strings.Join(tagClauses, " OR ")+")")
	}
	return "(" + strings.Join(clauses, " AND ") + ")", params

}

func (b *Builder) BuildTraceTimeRangeSubqueryForTraceID(traceID pgtype.UUID) (string, []interface{}) {
	params := []interface{}{b.cfg.MaxTraceDuration, traceID}
	return subqueryTimeRangeForTraceID, params
}

func (b *Builder) BuildTraceIDSubquery(q *spanstore.TraceQueryParameters, tInfo *tagsInfo) (string, []interface{}) {
	clauses := make([]string, 0, 15)
	params := tInfo.params

	clauses = append(clauses, tInfo.spanClauses...)
	if len(tInfo.eventClauses) > 0 {
		var subquery string
		subquery, params = b.buildEventSubquery(q, tInfo.eventClauses, params)
		clauses = append(clauses, fmt.Sprintf("EXISTS(%s)", subquery))
	}

	operationSubquery, params := b.buildOperationSubquery(q, tInfo, params)
	if len(operationSubquery) > 0 {
		qual := fmt.Sprintf(`
		   s.operation_id IN (
			      %s
		   )
		`, operationSubquery)
		clauses = append(clauses, qual)
	}

	if len(tInfo.generalTags) > 0 {
		var tagClause string
		tagClause, params = b.buildTagClauses(q, tInfo, params)
		clauses = append(clauses, tagClause)
	}
	//todo check the inclusive semantics here
	var defaultTime time.Time
	if q.StartTimeMin != defaultTime {
		params = append(params, q.StartTimeMin)
		clauses = append(clauses, fmt.Sprintf(`s.start_time >= $%d`, len(params)))
	}
	if q.StartTimeMax != defaultTime {
		params = append(params, q.StartTimeMax)
		clauses = append(clauses, fmt.Sprintf(`s.start_time <= $%d`, len(params)))
	}

	var defaultDuration time.Duration
	if q.DurationMin != defaultDuration {
		if q.DurationMin%time.Millisecond == 0 {
			params = append(params, q.DurationMin.Milliseconds())
			clauses = append(clauses, fmt.Sprintf(`duration_ms >= $%d`, len(params)))
		} else {
			params = append(params, q.DurationMin)
			clauses = append(clauses, fmt.Sprintf(`(end_time - start_time ) >= $%d`, len(params)))
		}
	}
	if q.DurationMax != defaultDuration {
		if q.DurationMax%time.Millisecond == 0 {
			params = append(params, q.DurationMax.Milliseconds())
			clauses = append(clauses, fmt.Sprintf(`duration_ms <= $%d`, len(params)))
		} else {
			params = append(params, q.DurationMax)
			clauses = append(clauses, fmt.Sprintf(`(end_time - start_time ) <= $%d`, len(params)))
		}

	}

	clauseString := ""
	if len(clauses) > 0 {
		clauseString = strings.Join(clauses, " AND ")
	} else {
		clauseString = "TRUE"
	}

	params = append(params, b.cfg.MaxTraceDuration)
	//Note: the parameter number for b.cfg.MaxTraceDuration is used in two places ($%[1]d in subqueryFormat)
	//to both add and subtract from start_time_max.
	query := fmt.Sprintf(subqueryFormat, len(params), clauseString)

	if q.NumTraces != 0 {
		query += fmt.Sprintf(" LIMIT %d", q.NumTraces)
	}
	return query, params
}
