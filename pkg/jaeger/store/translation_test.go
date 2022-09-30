package store

import (
	"testing"

	"github.com/jaegertracing/jaeger/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	conventions "go.opentelemetry.io/collector/semconv/v1.9.0"
)

func TestSpanMultipleParentProtoFromTraces(t *testing.T) {
	// Given a Span
	traces := ptrace.NewTraces()
	span := traces.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	rawTraceID := [16]byte{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6'}
	traceID := pcommon.NewTraceID(rawTraceID)
	span.SetTraceID(traceID)

	// With a ParentSpanID set
	parentSpanID := [8]byte{'1', '2', '3', '4', '5', '6', '7', '8'}
	span.SetParentSpanID(pcommon.NewSpanID(parentSpanID))

	// And two links
	// And the first of the 2 links doesn't specify any RefType
	followsFromLink := span.Links().AppendEmpty()
	followsFromSpanID := [8]byte{'1', '2', '3', '4', '5', '6', '7', '1'}
	followsFromLink.SetSpanID(pcommon.NewSpanID(followsFromSpanID))
	followsFromLink.SetTraceID(traceID)
	// And the second of the links specifies the ChildOf attribute from the
	// OpenTracing semantic convention.
	childOfLink := span.Links().AppendEmpty()
	otherParentSpanID := [8]byte{'1', '2', '3', '4', '5', '6', '7', '9'}
	childOfLink.SetSpanID(pcommon.NewSpanID(otherParentSpanID))
	childOfLink.SetTraceID(traceID)
	childOfLink.Attributes().InsertString(
		conventions.AttributeOpentracingRefType,
		conventions.AttributeOpentracingRefTypeChildOf,
	)

	// When translating from OTEL to Jaeger
	batches, err := ProtoFromTraces(traces)
	require.NoError(t, err)

	// Then the ParentSpanID and the Links are transformed into Jaeger References
	references := batches[0].Spans[0].References
	assert.Equal(t, 3, len(references))

	// And the ParentSpanID is set as the first Reference and with
	// RefType=ChildOf
	assert.Equal(t, spanIDToJaegerProto(parentSpanID), references[0].SpanID)
	assert.Equal(t, model.ChildOf, references[0].RefType)

	// And the link that didn't specify a RefType is returned as FollowsFrom
	assert.Equal(t, spanIDToJaegerProto(followsFromSpanID), references[1].SpanID)
	assert.Equal(t, model.FollowsFrom, references[1].RefType)

	// And the link that specified the ChildOf semantic convention attribute
	// is returned with RefType ChildOf
	assert.Equal(t, spanIDToJaegerProto(otherParentSpanID), references[2].SpanID)
	assert.Equal(t, model.ChildOf, references[2].RefType)
}

func TestSpanWithMultipleParentProtoToTraces(t *testing.T) {

	traceID := model.TraceID{
		Low:  1,
		High: 42,
	}

	parentSpanID := uint64(24)
	otherParentSpanID := uint(2)
	followsFromSpanID := uint(3)

	// Given a Jaeger Span
	span := &model.Span{
		TraceID: traceID,
		SpanID:  42,
		// With 3 references
		References: []model.SpanRef{
			{
				TraceID: traceID,
				SpanID:  model.SpanID(parentSpanID),
				RefType: model.ChildOf,
			},
			{
				TraceID: traceID,
				SpanID:  model.SpanID(followsFromSpanID),
				RefType: model.FollowsFrom,
			},
			{
				TraceID: traceID,
				SpanID:  model.SpanID(otherParentSpanID),
				RefType: model.ChildOf,
			},
		},
		Tags:     []model.KeyValue{},
		Logs:     []model.Log{},
		Warnings: []string{},
		Process: &model.Process{
			Tags: []model.KeyValue{},
		},
	}

	// When converting from Jaeger to OTEL.
	traces, err := ProtoToTraces(span)
	require.NoError(t, err)
	otelSpan := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)

	// Then the first ChildOf Reference is set as ParentSpanID.
	assert.Equal(t, uInt64ToSpanID(parentSpanID), pcommon.SpanID(otelSpan.ParentSpanID()))
	// And the other 2 transformed into Links.
	assert.Equal(t, 2, otelSpan.Links().Len())

	// And the FollowsFrom Reference is transformed into a Link with the
	// FollowsFrom attribute from the OpenTracing semantic convention.
	followsFromLink := otelSpan.Links().At(0)
	assert.Equal(t, uInt64ToSpanID(uint64(followsFromSpanID)), followsFromLink.SpanID())
	assert.Equal(t, 1, followsFromLink.Attributes().Len())
	refType, ok := followsFromLink.Attributes().Get(conventions.AttributeOpentracingRefType)
	assert.True(t, ok)
	assert.Equal(t, conventions.AttributeOpentracingRefTypeFollowsFrom, refType.StringVal())

	// And the other ChildOf Reference is transformed into a Link with the
	// ChildOF attribute from the OpenTracing semantic convention.
	otherParentLink := otelSpan.Links().At(1)
	assert.Equal(t, uInt64ToSpanID(uint64(otherParentSpanID)), otherParentLink.SpanID())
	assert.Equal(t, 1, otherParentLink.Attributes().Len())
	refType, ok = otherParentLink.Attributes().Get(conventions.AttributeOpentracingRefType)
	assert.True(t, ok)
	assert.Equal(t, conventions.AttributeOpentracingRefTypeChildOf, refType.StringVal())
}
