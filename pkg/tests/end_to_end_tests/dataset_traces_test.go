// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/golang/snappy"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

var (
	traceID1               = [16]byte{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6'}
	traceID2               = [16]byte{'0', '2', '3', '4', '0', '6', '7', '8', '9', '0', '0', '2', '3', '4', '0', '6'}
	testSpanStartTime      = time.Date(2020, 2, 11, 20, 26, 12, 321000, time.UTC)
	testSpanStartTimestamp = pcommon.NewTimestampFromTime(testSpanStartTime)

	testSpanEventTime      = time.Date(2020, 2, 11, 20, 26, 13, 123000, time.UTC)
	testSpanEventTimestamp = pcommon.NewTimestampFromTime(testSpanEventTime)

	testSpanEndTime      = time.Date(2020, 2, 11, 20, 26, 13, 789000, time.UTC)
	testSpanEndTimestamp = pcommon.NewTimestampFromTime(testSpanEndTime)

	service0 = "service-name-0"

	spanAttributes = pcommon.NewMapFromRaw(
		map[string]interface{}{
			"span-attr":                  "span-attr-val",
			"host.name":                  "hostname1",
			"opencensus.exporterversion": "Jaeger-1.0.0",
			"http.status_code":           200,
		},
	)
	spanEventAttributes = pcommon.NewMapFromRaw(map[string]interface{}{"span-event-attr": "span-event-attr-val"})
	spanLinkAttributes  = pcommon.NewMapFromRaw(map[string]interface{}{"span-link-attr": "span-link-attr-val"})
)

func getTraceId(bSlice [16]byte) string {
	return hex.EncodeToString(bSlice[:])
}

// generateBrokenTestTraces switches start and end times for every span to check if
// we handle this broken situation correctly and reverse the times while ingesting.
func generateBrokenTestTraces() ptrace.Traces {
	data := generateTestTrace()
	startTime := data.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).StartTimestamp()
	endTime := data.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).EndTimestamp()

	data.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).SetStartTimestamp(endTime)
	data.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).SetEndTimestamp(startTime)
	return data
}

func generateTestTrace() ptrace.Traces {
	rand.Seed(1)
	spanCount := 4
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	initResourceAttributes(rs.Resource().Attributes(), 0)
	libSpans := rs.ScopeSpans().AppendEmpty()
	initInstLib(libSpans, 0)
	for i := 0; i < spanCount; i++ {
		fillSpanOne(libSpans.Spans().AppendEmpty())
	}
	ids := make([]pcommon.SpanID, 0)
	for i := 0; i < spanCount; i++ {
		sid := pcommon.NewSpanID(generateRandSpanID())
		fillSpanTwo(libSpans.Spans().AppendEmpty(), sid)
		ids = append(ids, sid)
	}
	rs = td.ResourceSpans().AppendEmpty()
	initResourceAttributes(rs.Resource().Attributes(), 1)
	libSpans = rs.ScopeSpans().AppendEmpty()
	initInstLib(libSpans, 1)
	for _, parentID := range ids {
		fillSpanThree(libSpans.Spans().AppendEmpty(), parentID)
	}

	return td
}

func generateTestTraceManyRS() []ptrace.Traces {
	traces := make([]ptrace.Traces, 5)
	for traceIndex := 0; traceIndex < len(traces); traceIndex++ {
		spanCount := 5
		td := ptrace.NewTraces()
		for resourceIndex := 0; resourceIndex < 5; resourceIndex++ {
			rs := td.ResourceSpans().AppendEmpty()
			initResourceAttributes(rs.Resource().Attributes(), resourceIndex)
			for libIndex := 0; libIndex < 5; libIndex++ {
				libSpans := rs.ScopeSpans().AppendEmpty()
				initInstLib(libSpans, libIndex)
				for i := 0; i < spanCount; i++ {
					fillSpanOne(libSpans.Spans().AppendEmpty())
				}
				for i := 0; i < spanCount; i++ {
					fillSpanTwo(libSpans.Spans().AppendEmpty(), pcommon.NewSpanID(generateRandSpanID()))
				}
			}
		}
		traces[traceIndex] = td
	}
	return traces
}

func initInstLib(dest ptrace.ScopeSpans, index int) {
	// Only add schema URL on half of instrumentation libs.
	if index%2 == 0 {
		dest.SetSchemaUrl(fmt.Sprintf("url-%d", index%2))
	}
	dest.Scope().SetName(fmt.Sprintf("inst-lib-name-%d", index))
	dest.Scope().SetVersion(fmt.Sprintf("1.%d.0", index%2))
}

func initResourceAttributes(dest pcommon.Map, index int) {
	tmpl := pcommon.NewMapFromRaw(map[string]interface{}{
		"resource-attr": fmt.Sprintf("resource-attr-val-%d", index%2),
		"service.name":  fmt.Sprintf("service-name-%d", index),
		"test-slice":    []string{},
	})
	dest.Clear()
	tmpl.CopyTo(dest)
}

func initSpanEventAttributes(dest pcommon.Map) {
	dest.Clear()
	spanEventAttributes.CopyTo(dest)
}

func initSpanAttributes(dest pcommon.Map) {
	dest.Clear()
	spanAttributes.CopyTo(dest)
}

func initSpanLinkAttributes(dest pcommon.Map) {
	dest.Clear()
	spanLinkAttributes.CopyTo(dest)
}

func generateRandSpanID() (result [8]byte) {
	rand.Read(result[:])
	return result
}

func fillSpanOne(span ptrace.Span) {
	span.SetTraceID(pcommon.NewTraceID(traceID1))
	span.SetSpanID(pcommon.NewSpanID(generateRandSpanID()))
	span.SetName("operationA")
	span.SetStartTimestamp(testSpanStartTimestamp)
	span.SetEndTimestamp(testSpanEndTimestamp)
	span.SetDroppedAttributesCount(1)
	span.SetTraceState("span-trace-state1")
	span.SetKind(ptrace.SpanKindClient)
	initSpanAttributes(span.Attributes())
	evs := span.Events()
	ev0 := evs.AppendEmpty()
	ev0.SetTimestamp(testSpanEventTimestamp)
	ev0.SetName("event-with-attr")
	initSpanEventAttributes(ev0.Attributes())
	ev0.SetDroppedAttributesCount(2)
	ev1 := evs.AppendEmpty()
	ev1.SetTimestamp(testSpanEventTimestamp)
	ev1.SetName("event")
	ev1.SetDroppedAttributesCount(2)
	span.SetDroppedEventsCount(1)
	status := span.Status()
	status.SetCode(ptrace.StatusCodeError)
	status.SetMessage("status-cancelled")
}

func fillSpanTwo(span ptrace.Span, spanID pcommon.SpanID) {
	span.SetTraceID(pcommon.NewTraceID(traceID2))
	span.SetSpanID(spanID)
	span.SetName("operationB")
	span.SetStartTimestamp(testSpanStartTimestamp)
	span.SetEndTimestamp(testSpanEndTimestamp)
	span.SetTraceState("span-trace-state2")
	initSpanAttributes(span.Attributes())
	link0 := span.Links().AppendEmpty()
	initSpanLinkAttributes(link0.Attributes())
	link0.SetTraceID(pcommon.NewTraceID([16]byte{'1'}))
	link0.SetSpanID(pcommon.NewSpanID(generateRandSpanID()))
	link0.SetDroppedAttributesCount(4)
	link0.SetTraceState("link-trace-state1")
	link1 := span.Links().AppendEmpty()
	link1.SetDroppedAttributesCount(4)
	link1.SetTraceID(pcommon.NewTraceID([16]byte{'1'}))
	link1.SetSpanID(pcommon.NewSpanID(generateRandSpanID()))
	link1.SetTraceState("link-trace-state2")
	span.SetDroppedLinksCount(3)
}

func fillSpanThree(span ptrace.Span, parentSpanID pcommon.SpanID) {
	span.SetTraceID(pcommon.NewTraceID(traceID2))
	span.SetSpanID(pcommon.NewSpanID(generateRandSpanID()))
	span.SetParentSpanID(parentSpanID)
	span.SetName("operationC")
	span.SetStartTimestamp(testSpanStartTimestamp)
	span.SetEndTimestamp(testSpanEndTimestamp)
}

// deep copy the traces since we mutate them.
func copyTraces(traces ptrace.Traces) ptrace.Traces {
	return traces.Clone()
}

func readTraces(t testing.TB, count int) []ptrace.Traces {
	// Clamp to max.
	if count > tracesEntryCount {
		count = tracesEntryCount
	}

	// Dataset was generated from traces which where generated by the
	// microservice demo app made by Google Cloud team. It contains
	// 2.5M spans.
	// https://github.com/GoogleCloudPlatform/microservices-demo/
	// Data is serialized from OTEL model ptrace.Traces structs
	// using a protobuf marshaller, encoded into byte slices using gob
	// std lib encoder and compressed using snappy in streaming format.
	f, err := os.Open("../testdata/traces-dataset.sz")
	if err != nil {
		if os.IsNotExist(err) {
			t.Skip("missing tracing dataset file, skipping test...\n to fetch the file try running `git lfs checkout`")
		}
		panic(err)
	}

	defer f.Close()
	gReader := gob.NewDecoder(snappy.NewReader(f))

	var (
		buf               []byte
		tr                ptrace.Traces
		tracesUnmarshaler = ptrace.NewProtoUnmarshaler()
		i                 = 0
		result            = make([]ptrace.Traces, 0, count)
	)

	for i < count {
		err = gReader.Decode(&buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		tr, err = tracesUnmarshaler.UnmarshalTraces(buf)
		if err != nil {
			panic(err)
		}

		result = append(result, tr)
		i++
	}
	return result
}

func generateAllTraces(t testing.TB) []ptrace.Traces {
	return readTraces(t, tracesEntryCount)
}

func generateSmallTraces(t testing.TB) []ptrace.Traces {
	return readTraces(t, 20)
}
