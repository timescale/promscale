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

	spanKindToInternalValue = map[string]string{
		"SPAN_KIND_UNSPECIFIED": "unspecified",
		"SPAN_KIND_INTERNAL":    "internal",
		"SPAN_KIND_SERVER":      "server",
		"SPAN_KIND_CLIENT":      "client",
		"SPAN_KIND_PRODUCER":    "producer",
		"SPAN_KIND_CONSUMER":    "consumer",
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
	v, ok := spanKindToInternalValue[kind]
	if !ok {
		return "", fmt.Errorf("unknown span kind: %s", kind)
	}
	return v, nil
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

	return traces, nil
}

func ProtoFromTraces(traces ptrace.Traces) ([]*model.Batch, error) {
	batches, err := jaegertranslator.ProtoFromTraces(traces)
	if err != nil {
		return nil, fmt.Errorf("internal-traces-to-jaeger-proto: %w", err)
	}
	for _, batch := range batches {
		if batch != nil {
			decodeBinaryTags(batch.Process.Tags)
		}
		for _, span := range batch.GetSpans() {
			decodeSpanBinaryTags(span)

		}
	}
	return batches, nil
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
