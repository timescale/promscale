// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package store

import (
	"context"
	"fmt"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	jaegertranslator "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger"
	"github.com/timescale/promscale/pkg/pgxconn"
	"go.opentelemetry.io/collector/pdata/ptrace"
	conventions "go.opentelemetry.io/collector/semconv/v1.9.0"
)

func findTraces(ctx context.Context, builder *Builder, conn pgxconn.PgxConn, q *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	tInfo, err := FindTagInfo(ctx, q, conn)
	if err != nil {
		return nil, fmt.Errorf("querying trace tags error: %w", err)
	}
	if tInfo == nil {
		//tags cannot be matched
		return []*model.Trace{}, nil
	}
	query, params := builder.findTracesQuery(q, tInfo)
	rows, err := conn.Query(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("querying traces error: %w query:\n%s", err, query)
	}
	defer rows.Close()

	return scanTraces(rows)
}

func scanTraces(rows pgxconn.PgxRows) ([]*model.Trace, error) {
	traces := ptrace.NewTraces()
	for rows.Next() {
		if rows.Err() != nil {
			return nil, fmt.Errorf("trace row iterator: %w", rows.Err())
		}
		if err := ScanRow(rows, &traces); err != nil {
			return nil, fmt.Errorf("error scanning trace: %w", err)
		}

	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("trace row iterator: %w", rows.Err())
	}

	batch, err := jaegertranslator.ProtoFromTraces(traces)
	if err != nil {
		return nil, fmt.Errorf("internal-traces-to-jaeger-proto: %w", err)
	}

	return batchSliceToTraceSlice(batch, getOtherParents(traces)), nil
}

func batchSliceToTraceSlice(bSlice []*model.Batch, otherParents map[model.SpanID][]int) []*model.Trace {
	// Mostly Copied from Jaeger's grpc_client.go
	// https://github.com/jaegertracing/jaeger/blob/067dff713ab635ade66315bbd05518d7b28f40c6/plugin/storage/grpc/shared/grpc_client.go#L179
	traces := make([]*model.Trace, 0)
	var traceID model.TraceID
	var trace *model.Trace
	for j := range bSlice {
		batch := bSlice[j]
		for i := range batch.Spans {
			span := batch.Spans[i]
			if span.TraceID != traceID {
				trace = &model.Trace{}
				traceID = span.TraceID
				traces = append(traces, trace)
			}
			//copy over the process from the batch
			span.Process = batch.Process
			decodeSpanBinaryTags(span)

			// TODO: There's an open PR against the Jaeger translator that adds
			// support for keeping the RefType. Once the PR is merged we can remove
			// the following if condition and the getOtherParents function.
			//
			// https://github.com/open-telemetry/opentelemetry-collector-contrib/pull/14463
			if pIdxs, ok := otherParents[span.SpanID]; ok {
				refs := span.GetReferences()
				for _, i := range pIdxs {
					refs[i].RefType = model.ChildOf
				}
			}
			trace.Spans = append(trace.Spans, span)
		}
	}
	return traces
}

// getOtherParents returns a map where the keys are the IDs of Spans that have
// more than one parent and the values are the position in the Span.References
// list where those other parents references are.
//
// A parent is a link that has the `child_of` attribute defined in the semantic
// convention for opentracing:
//
// https://opentelemetry.io/docs/reference/specification/trace/semantic_conventions/compatibility/#opentracing
//
// It tracks the position instead of the SpanID because there might multiple
// links to the same SpanID but different RefTypes.
func getOtherParents(traces ptrace.Traces) map[model.SpanID][]int {
	otherParents := map[model.SpanID][]int{}

	resourceSpans := traces.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		rSpan := resourceSpans.At(i)
		sSpans := rSpan.ScopeSpans()
		for j := 0; j < sSpans.Len(); j++ {
			sSpan := sSpans.At(j)
			spans := sSpan.Spans()
			if spans.Len() == 0 {
				continue
			}
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				links := span.Links()

				// We need an offset because if the span has a ParentSpanID, then
				// that's going to be the first link when translating from OTEL to
				// Jaeger. We could say that is it doesn't have a ParentSpanID then
				// it shouldn't have other parents, but just to be extra safe we
				// inspect the attributes even if there's no ParentSpanID set.
				offset := 0
				if !span.ParentSpanID().IsEmpty() {
					offset = 1
				}
				for l := 0; l < links.Len(); l++ {
					link := links.At(l)
					v, ok := link.Attributes().Get(conventions.AttributeOpentracingRefType)
					if !ok || v.StringVal() != conventions.AttributeOpentracingRefTypeChildOf {
						continue
					}
					spanID := spanIDToJaegerProto(span.SpanID().Bytes())
					pIdxs, ok := otherParents[spanID]
					if !ok {
						pIdxs = []int{}
						otherParents[spanID] = pIdxs
					}
					otherParents[spanID] = append(pIdxs, l+offset)
				}
			}
		}
	}
	return otherParents
}
