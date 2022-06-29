// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package query

import (
	"fmt"

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
	// Map of Jaeger span kind tag strings to internal DB values.
	// This is kept explicit even though the values are identical.
	jSpanKindToInternalValue = map[string]string{
		"client":   "client",
		"server":   "server",
		"internal": "internal",
		"consumer": "consumer",
		"producer": "producer",
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
