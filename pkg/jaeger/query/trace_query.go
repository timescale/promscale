// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package query

import (
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

const (
	completeTraceSQLFormat = `
	SELECT
		s.trace_id,
	   	s.span_id,
		s.parent_span_id,
		s.start_time start_times,
		s.end_time end_times,
		o.span_kind,
		s.dropped_tags_count 			dropped_tags_counts,
		s.dropped_events_count 			dropped_events_counts,
		s.dropped_link_count 			dropped_link_counts,
		s.status_code 					status_code_string,
		s.status_message,
		s.trace_state 			trace_states,
		s_url.url 				schema_urls,
		o.span_name     		span_names,
	   	ps_trace.jsonb(s.resource_tags) 	resource_tags,
	   	ps_trace.jsonb(s.span_tags) 		span_tags,
	   	array_agg(e.name ORDER BY e.event_nbr) FILTER(WHERE e IS NOT NULL)			event_names,
		array_agg(e.time ORDER BY e.event_nbr) FILTER(WHERE e IS NOT NULL)			event_times,
	   	array_agg(e.dropped_tags_count ORDER BY e.event_nbr) FILTER(WHERE e IS NOT NULL)	event_dropped_tags_count,
	   	jsonb_agg(ps_trace.jsonb(e.tags) ORDER BY e.event_nbr) FILTER(WHERE e IS NOT NULL)	event_tags,
	   	inst_lib.name 				library_name,
	   	inst_lib.version 			library_version,
		inst_lib_url.url 			library_schema_url,
		array_agg(lk.linked_trace_id ORDER BY lk.link_nbr) FILTER(WHERE lk IS NOT NULL)	links_linked_trace_ids,
		array_agg(lk.linked_span_id ORDER BY lk.link_nbr)  FILTER(WHERE lk IS NOT NULL)	links_linked_span_ids,
		array_agg(lk.trace_state ORDER BY lk.link_nbr)     FILTER(WHERE lk IS NOT NULL)	links_trace_states,
		array_agg(lk.dropped_tags_count ORDER BY lk.link_nbr) FILTER(WHERE lk IS NOT NULL) 	links_dropped_tags_count,
		jsonb_agg(lk.tags ORDER BY lk.link_nbr) FILTER(WHERE lk IS NOT NULL)			links_tags
	FROM
		_ps_trace.span s
	INNER JOIN
		_ps_trace.operation o ON (s.operation_id = o.id)
	LEFT JOIN
		_ps_trace.schema_url s_url ON s.resource_schema_url_id = s_url.id
	LEFT JOIN
		_ps_trace.event e ON e.span_id = s.span_id AND e.trace_id = s.trace_id
	LEFT JOIN
		_ps_trace.instrumentation_lib inst_lib ON s.instrumentation_lib_id = inst_lib.id
	LEFT JOIN
		_ps_trace.schema_url inst_lib_url ON inst_lib_url.id = inst_lib.schema_url_id
	LEFT JOIN
		_ps_trace.link lk ON lk.trace_id = s.trace_id AND lk.span_id = s.span_id
	WHERE
	  %s
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
	  inst_lib_url.url`

	subqueryFormat = `
	SELECT
		trace_sub.trace_id
	FROM (
		SELECT
			trace_id,
			max(start_time) as start_time_max
		FROM _ps_trace.span s
		WHERE
			%s
		GROUP BY trace_id
	) as trace_sub
	ORDER BY trace_sub.start_time_max DESC
	`

	/* PostgreSQL badly overestimates the number of rows returned if the complete trace query
	uses an IN clause on trace_id, but gives good estimates for equality conditions. So, leverage an INNER
	JOIN LATERAL to provide an equality condition on the complete trace. */
	findTraceSQLFormat = `
	WITH trace_ids AS (
		%s
	)
	SELECT
		complete_trace.*
	FROM
		trace_ids
	INNER JOIN LATERAL (
		%s
	) AS complete_trace ON (TRUE)
	`
)

func buildCompleteTraceQuery(traceIDClause string) string {
	return fmt.Sprintf(
		completeTraceSQLFormat,
		traceIDClause)
}

func findTracesQuery(q *spanstore.TraceQueryParameters) (string, []interface{}) {
	subquery, params := buildTraceIDSubquery(q)
	traceIDClause := "s.trace_id = trace_ids.trace_id"
	completeTraceSQL := buildCompleteTraceQuery(traceIDClause)
	return fmt.Sprintf(findTraceSQLFormat, subquery, completeTraceSQL), params
}

func findTraceIDsQuery(q *spanstore.TraceQueryParameters) (string, []interface{}) {
	subquery, params := buildTraceIDSubquery(q)
	return subquery, params
}

func getTraceQuery(traceID model.TraceID) (string, []interface{}, error) {
	var b [16]byte
	n, err := traceID.MarshalTo(b[:])
	if n != 16 || err != nil {
		return "", nil, fmt.Errorf("marshaling TraceID: %w", err)
	}

	var uuid pgtype.UUID
	if err := uuid.Set(b); err != nil {
		return "", nil, fmt.Errorf("setting TraceID: %w", err)
	}
	params := []interface{}{uuid}

	traceIDClause := "s.trace_id = $1"
	return buildCompleteTraceQuery(traceIDClause), params, nil
}

func buildTraceIDSubquery(q *spanstore.TraceQueryParameters) (string, []interface{}) {
	clauses := make([]string, 0, 15)
	params := make([]interface{}, 0, 15)

	operation_clauses := make([]string, 0, 2)
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
		operation_clauses = append(operation_clauses, qual)
	}
	if len(q.OperationName) > 0 {
		params = append(params, q.OperationName)
		qual := fmt.Sprintf(`span_name = $%d`, len(params))
		operation_clauses = append(operation_clauses, qual)
	}

	if len(operation_clauses) > 0 {
		qual := fmt.Sprintf(`
		   s.operation_id IN (
			   SELECT
			     id
			   FROM _ps_trace.operation
			   WHERE
			      %s
		   )
		`, strings.Join(operation_clauses, " AND "))
		clauses = append(clauses, qual)
	}

	if len(q.Tags) > 0 {
		for k, v := range q.Tags {
			params = append(params, k, v)
			qual := fmt.Sprintf(`s.span_tags ? ($%d == $%d)`, len(params)-1, len(params))
			clauses = append(clauses, qual)
		}
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

	query := ""
	if len(clauses) > 0 {
		query = fmt.Sprintf(subqueryFormat, strings.Join(clauses, " AND "))
	} else {
		query = fmt.Sprintf(subqueryFormat, "TRUE")
	}

	if q.NumTraces != 0 {
		query += fmt.Sprintf(" LIMIT %d", q.NumTraces)
	}
	return query, params
}
