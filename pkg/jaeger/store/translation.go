// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package store

import (
	"encoding/binary"
	"fmt"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	jaegertranslator "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	conventions "go.opentelemetry.io/collector/semconv/v1.9.0"
)

var (
	// Map of status codes to internal DB values.
	statusCodeInternalValue = map[string]ptrace.StatusCode{
		"ok":    ptrace.StatusCodeOk,
		"error": ptrace.StatusCodeError,
		"unset": ptrace.StatusCodeUnset,
	}
	// Map of span kinds to internal DB values.
	spanKindInternalValue = map[string]ptrace.SpanKind{
		"client":      ptrace.SpanKindClient,
		"server":      ptrace.SpanKindServer,
		"internal":    ptrace.SpanKindInternal,
		"consumer":    ptrace.SpanKindConsumer,
		"producer":    ptrace.SpanKindProducer,
		"unspecified": ptrace.SpanKindUnspecified,
	}
	// Map of Jaeger span kind tag strings to internal DB values.
	// This is kept explicit even though the values are identical.
	jSpanKindToInternalValue = map[string]string{
		"client":   "client",
		"server":   "server",
		"internal": "internal",
		"consumer": "consumer",
		"producer": "producer",
	}
	// Map of internal span kind to Jaeger.
	internalToJaegerSpanKind = map[string]string{
		"client":      "client",
		"server":      "server",
		"internal":    "internal",
		"consumer":    "consumer",
		"producer":    "producer",
		"unspecified": "",
	}
)

func internalToStatusCode(s string) ptrace.StatusCode {
	v, ok := statusCodeInternalValue[s]
	if !ok {
		panic(fmt.Sprintf("status code %s does not have internal representation", s))
	}
	return v
}

func statusCodeToInternal(sc ptrace.StatusCode) string {
	for k, v := range statusCodeInternalValue {
		if sc == v {
			return k
		}
	}
	panic(fmt.Sprintf("status code %s does not have internal representation", sc.String()))
}

func internalToJaegerOperation(name, kind string) spanstore.Operation {
	spanKind, ok := internalToJaegerSpanKind[kind]
	if !ok {
		return spanstore.Operation{}
	}
	return spanstore.Operation{
		Name:     name,
		SpanKind: spanKind,
	}
}

func internalToSpanKind(s string) ptrace.SpanKind {
	v, ok := spanKindInternalValue[s]
	if !ok {
		panic(fmt.Sprintf("span kind %s does not have internal representation", s))
	}
	return v
}

func spanKindStringToInternal(kind string) (string, error) {
	for k, v := range spanKindInternalValue {
		if v.String() == kind {
			return k, nil
		}
	}
	return "", fmt.Errorf("unknown span kind: %s", kind)
}

// JSpanKindToInternal translates Jaeger span kind tag value to internal enum value used for storage.
// Corresponds to:
// https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/a13030ca6208fa7d4477f0805f3b95e271f32b24/pkg/translator/jaeger/jaegerproto_to_traces.go#L303
func jSpanKindToInternal(spanKind string) string {
	if v, ok := jSpanKindToInternalValue[spanKind]; ok {
		return v
	}
	return "unspecified"
}

func ProtoToTraces(span *model.Span) (ptrace.Traces, error) {
	encodeBinaryTags(span)
	batches := []*model.Batch{
		{
			Spans: []*model.Span{span},
		},
	}
	traces, err := jaegertranslator.ProtoToTraces(batches)
	if err != nil {
		return ptrace.NewTraces(), err
	}

	// TODO: There's an open PR against the Jaeger translator that adds support
	// for keeping the RefType. Once the PR is merged we can remove the following
	// if condition and the addRefTypeAttributeToLinks function.
	//
	// https://github.com/open-telemetry/opentelemetry-collector-contrib/pull/14463
	if len(span.References) > 1 {
		links := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Links()
		addRefTypeAttributeToLinks(span, links)
	}

	return traces, nil
}

// TODO: There's an open PR against the Jaeger translator that adds support
// for keeping the RefType. Once the PR is merged we can delete this function.
//
// https://github.com/open-telemetry/opentelemetry-collector-contrib/pull/14463
//
// addRefTypeAttributeToLinks adds the RefType of the Jaeger Span references as
// an attribute to their corresponding OTEL links. The `links` argument must
// be the OTEL representation of the given `span.References`.
//
// The added attributes follow the OpenTracing to OTEL semantic convention
// https://opentelemetry.io/docs/reference/specification/trace/semantic_conventions/compatibility/#opentracing
func addRefTypeAttributeToLinks(span *model.Span, links ptrace.SpanLinkSlice) {

	// The reference to the parent span is stored directly as an attribute
	// of the Span and not as a Link.
	parentsSpanID := span.ParentSpanID()
	otherParentsSpanIDs := map[model.SpanID]struct{}{}

	// Since there are only 2 types of refereces, ChildOf and FollowsFrom, we
	// keep track only of the former.
	for _, ref := range span.References {
		if ref.RefType == model.ChildOf && ref.SpanID != parentsSpanID {
			otherParentsSpanIDs[ref.SpanID] = struct{}{}
		}
	}

	for i := 0; i < links.Len(); i++ {
		link := links.At(i)
		spanID := spanIDToJaegerProto(link.SpanID())

		// Everything that's not ChildOf will be set as FollowsFrom.
		if _, ok := otherParentsSpanIDs[spanID]; ok {
			link.Attributes().PutString(
				conventions.AttributeOpentracingRefType,
				conventions.AttributeOpentracingRefTypeChildOf,
			)
			continue
		}
		link.Attributes().PutString(
			conventions.AttributeOpentracingRefType,
			conventions.AttributeOpentracingRefTypeFollowsFrom,
		)
	}
}

func ProtoFromTraces(traces ptrace.Traces) ([]*model.Batch, error) {
	batches, err := jaegertranslator.ProtoFromTraces(traces)
	if err != nil {
		return nil, fmt.Errorf("internal-traces-to-jaeger-proto: %w", err)
	}
	otherParents := getOtherParents(traces)
	for _, batch := range batches {
		for _, span := range batch.GetSpans() {
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
		}
	}
	return batches, nil
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
					if !ok || v.Str() != conventions.AttributeOpentracingRefTypeChildOf {
						continue
					}
					spanID := spanIDToJaegerProto(span.SpanID())
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

func uInt64ToSpanID(id uint64) pcommon.SpanID {
	spanID := pcommon.SpanID{}
	binary.BigEndian.PutUint64(spanID[:], id)
	return spanID
}

// SpanIDToUInt64 converts the pcommon.SpanID to uint64 representation.
func spanIDToJaegerProto(rawSpanID [8]byte) model.SpanID {
	return model.SpanID(binary.BigEndian.Uint64(rawSpanID[:]))
}
