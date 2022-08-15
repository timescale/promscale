// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package store

import (
	"context"
	"fmt"

	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/timescale/promscale/pkg/pgxconn"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/collector/semconv/v1.6.1"
)

type tag struct {
	k              string
	v              string
	jsonbPairArray [][]byte
	isSpan         bool
	isResource     bool
	isEvent        bool
}

type tagsInfo struct {
	spanClauses      []string
	operationClauses []string
	eventClauses     []string
	generalTags      []*tag
	params           []interface{}
}

func FindTagInfo(ctx context.Context, q *spanstore.TraceQueryParameters, conn pgxconn.PgxConn) (*tagsInfo, error) {
	tagsInfo := &tagsInfo{}

	for k, v := range q.Tags {
		switch k {
		case TagError:
			var sc ptrace.StatusCode
			switch v {
			case "true":
				sc = ptrace.StatusCodeError
			case "false":
				sc = ptrace.StatusCodeUnset
			default:
				// Ignore tag if we don't match boolean.
				continue
			}
			tagsInfo.params = append(tagsInfo.params, statusCodeToInternal(sc))
			qual := fmt.Sprintf(`s.status_code = $%d`, len(tagsInfo.params))
			tagsInfo.spanClauses = append(tagsInfo.spanClauses, qual)
		case TagHostname:
			// https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/a13030ca6208fa7d4477f0805f3b95e271f32b24/pkg/translator/jaeger/jaegerproto_to_traces.go#L109
			tagsInfo.params = append(tagsInfo.params, semconv.AttributeHostName, "\""+v+"\"")
			qual := fmt.Sprintf(`s.span_tags @> (SELECT _ps_trace.tag_v_eq_matching_tags($%d::text, $%d::jsonb))`, len(tagsInfo.params)-1, len(tagsInfo.params))
			tagsInfo.spanClauses = append(tagsInfo.spanClauses, qual)
		case TagJaegerVersion:
			// https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/a13030ca6208fa7d4477f0805f3b95e271f32b24/pkg/translator/jaeger/jaegerproto_to_traces.go#L119
			tagsInfo.params = append(tagsInfo.params, "opencensus.exporterversion", "\"Jaeger-"+v+"\"")
			qual := fmt.Sprintf(`s.span_tags @> (SELECT _ps_trace.tag_v_eq_matching_tags($%d::text, $%d::jsonb))`, len(tagsInfo.params)-1, len(tagsInfo.params))
			tagsInfo.spanClauses = append(tagsInfo.spanClauses, qual)
		case TagSpanKind:
			tagsInfo.params = append(tagsInfo.params, jSpanKindToInternal(v))
			qual := fmt.Sprintf("op.span_kind = $%d", len(tagsInfo.params))
			tagsInfo.operationClauses = append(tagsInfo.operationClauses, qual)
		case TagW3CTraceState:
			tagsInfo.params = append(tagsInfo.params, v)
			qual := fmt.Sprintf(`s.trace_state = $%d`, len(tagsInfo.params))
			tagsInfo.spanClauses = append(tagsInfo.spanClauses, qual)
		case TagEventName:
			tagsInfo.params = append(tagsInfo.params, v)
			qual := fmt.Sprintf(`e.name = $%d`, len(tagsInfo.params))
			tagsInfo.eventClauses = append(tagsInfo.eventClauses, qual)
		default:
			tagsInfo.generalTags = append(tagsInfo.generalTags, &tag{k: k, v: v})
		}
	}

	if len(tagsInfo.generalTags) > 0 {
		query, params := tagsInfo.BuildTagsQuery(q)
		rows, err := conn.Query(ctx, query, params...)
		if err != nil {
			return nil, fmt.Errorf("querying traces error: %w query:\n%s", err, query)
		}
		defer rows.Close()
		possibleToMatch, err := tagsInfo.scanTags(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning traces error: %w query:\n%s", err, query)
		}
		if !possibleToMatch {
			return nil, nil
		}
	}

	return tagsInfo, nil
}

// BuildTagsQuery returns the query to be used to scan tags.
// Exposed for end to end tests.
func (t *tagsInfo) BuildTagsQuery(q *spanstore.TraceQueryParameters) (string, []interface{}) {
	params := make([]interface{}, 0, 2)

	keys := make([]string, len(t.generalTags))
	values := make([]string, len(t.generalTags))
	for i, tag := range t.generalTags {
		keys[i] = tag.k
		values[i] = tag.v
	}
	params = append(params, keys, values)
	c := fmt.Sprintf(`
		SELECT result.*
		FROM unnest ($%d::text[], $%d::text[]) with ordinality as tag_qual(key, value, nr)
		INNER JOIN LATERAL
		(
			SELECT
				array_agg(pg_catalog.jsonb_build_object(tag.key_id, tag.id)) json_pair_array,
				(bit_or(tag.tag_type) OPERATOR(pg_catalog.&) ps_trace.span_tag_type() != 0) is_span_type,
				(bit_or(tag.tag_type) OPERATOR(pg_catalog.&) ps_trace.resource_tag_type() != 0) is_resource_type,
				(bit_or(tag.tag_type) OPERATOR(pg_catalog.&) ps_trace.event_tag_type() != 0) is_event_type
			FROM
			(
					SELECT v, _prom_ext.jsonb_digest(v) as digest
					FROM unnest(_ps_trace.text_matches(tag_qual.value)) as v
			) as matchers
			INNER JOIN LATERAL (
				SELECT t.*
				FROM _ps_trace.tag t
				WHERE t.key OPERATOR(pg_catalog.=) tag_qual.key
				AND _prom_ext.jsonb_digest(t.value) OPERATOR(pg_catalog.=) matchers.digest
				AND t.value OPERATOR(pg_catalog.=) matchers.v
				LIMIT 1
			) tag on (true)
		) as result on (true)
		ORDER BY tag_qual.nr`, len(params)-1, len(params))

	return c, params
}

func (t *tagsInfo) scanTags(rows pgxconn.PgxRows) (bool, error) {
	i := 0
	for rows.Next() {
		if rows.Err() != nil {
			return false, fmt.Errorf("tag row iterator: %w", rows.Err())
		}
		tag := t.generalTags[i]
		var isSpan *bool
		var isResource *bool
		var isEvent *bool
		if err := rows.Scan(&tag.jsonbPairArray, &isSpan, &isResource, &isEvent); err != nil {
			return false, fmt.Errorf("error scanning tags: %w", err)
		}
		if tag.jsonbPairArray == nil {
			//such a tag does not exist. That means the tag predicate could not be fulfilled and the result
			//should always be the empty set.
			return false, nil
		}
		tag.isSpan = *isSpan
		tag.isResource = *isResource
		tag.isEvent = *isEvent
		i++
	}
	if rows.Err() != nil {
		return false, fmt.Errorf("tag row iterator: %w", rows.Err())
	}

	return true, nil
}
