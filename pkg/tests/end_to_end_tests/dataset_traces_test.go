// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"time"

	"go.opentelemetry.io/collector/model/otlpgrpc"
	"go.opentelemetry.io/collector/model/pdata"
)

var (
	traceID1               = [16]byte{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6'}
	traceID2               = [16]byte{'0', '2', '3', '4', '0', '6', '7', '8', '9', '0', '0', '2', '3', '4', '0', '6'}
	testSpanStartTime      = time.Date(2020, 2, 11, 20, 26, 12, 321000, time.UTC)
	testSpanStartTimestamp = pdata.NewTimestampFromTime(testSpanStartTime)

	testSpanEventTime      = time.Date(2020, 2, 11, 20, 26, 13, 123000, time.UTC)
	testSpanEventTimestamp = pdata.NewTimestampFromTime(testSpanEventTime)

	testSpanEndTime      = time.Date(2020, 2, 11, 20, 26, 13, 789000, time.UTC)
	testSpanEndTimestamp = pdata.NewTimestampFromTime(testSpanEndTime)

	service0 = "service-name-0"

	spanAttributes      = pdata.NewAttributeMapFromMap(map[string]pdata.AttributeValue{"span-attr": pdata.NewAttributeValueString("span-attr-val")})
	spanEventAttributes = pdata.NewAttributeMapFromMap(map[string]pdata.AttributeValue{"span-event-attr": pdata.NewAttributeValueString("span-event-attr-val")})
	spanLinkAttributes  = pdata.NewAttributeMapFromMap(map[string]pdata.AttributeValue{"span-link-attr": pdata.NewAttributeValueString("span-link-attr-val")})
)

// func generateTestTrace() pdata.Traces {
// 	spanCount := 4
// 	td := pdata.NewTraces()
// 	td.ResourceSpans().AppendEmpty()
// 	rs0 := td.ResourceSpans().At(0)
// 	initResourceAttributes(rs0.Resource().Attributes(), 0)
// 	libSpans := rs0.InstrumentationLibrarySpans().AppendEmpty()
// 	initInstLib(libSpans, 0)
// 	for i := 0; i < spanCount; i++ {
// 		fillSpanOne(libSpans.Spans().AppendEmpty())
// 	}
// 	for i := 0; i < spanCount; i++ {
// 		fillSpanTwo(libSpans.Spans().AppendEmpty())
// 	}

// 	return td
// }

// func generateTestTraceManyRS() []pdata.Traces {
// 	traces := make([]pdata.Traces, 5)
// 	for traceIndex := 0; traceIndex < len(traces); traceIndex++ {
// 		spanCount := 5
// 		td := pdata.NewTraces()
// 		for resourceIndex := 0; resourceIndex < 5; resourceIndex++ {
// 			rs := td.ResourceSpans().AppendEmpty()
// 			initResourceAttributes(rs.Resource().Attributes(), resourceIndex)
// 			for libIndex := 0; libIndex < 5; libIndex++ {
// 				libSpans := rs.InstrumentationLibrarySpans().AppendEmpty()
// 				initInstLib(libSpans, libIndex)
// 				for i := 0; i < spanCount; i++ {
// 					fillSpanOne(libSpans.Spans().AppendEmpty())
// 				}
// 				for i := 0; i < spanCount; i++ {
// 					fillSpanTwo(libSpans.Spans().AppendEmpty())
// 				}
// 			}
// 		}
// 		traces[traceIndex] = td
// 	}
// 	return traces
// }

// func initInstLib(dest pdata.InstrumentationLibrarySpans, index int) {
// 	dest.SetSchemaUrl(fmt.Sprintf("url-%d", index%2))
// 	dest.InstrumentationLibrary().SetName(fmt.Sprintf("inst-lib-name-%d", index))
// 	dest.InstrumentationLibrary().SetVersion(fmt.Sprintf("1.%d.0", index%2))
// }

// func initResourceAttributes(dest pdata.AttributeMap, index int) {
// 	tmpl := pdata.NewAttributeMapFromMap(map[string]pdata.AttributeValue{
// 		"resource-attr": pdata.NewAttributeValueString(fmt.Sprintf("resource-attr-val-%d", index%2)),
// 		"service.name":  pdata.NewAttributeValueString(fmt.Sprintf("service-name-%d", index)),
// 	})
// 	dest.Clear()
// 	tmpl.CopyTo(dest)
// }

// func initSpanEventAttributes(dest pdata.AttributeMap) {
// 	dest.Clear()
// 	spanEventAttributes.CopyTo(dest)
// }

// func initSpanAttributes(dest pdata.AttributeMap) {
// 	dest.Clear()
// 	spanAttributes.CopyTo(dest)
// }

// func initSpanLinkAttributes(dest pdata.AttributeMap) {
// 	dest.Clear()
// 	spanLinkAttributes.CopyTo(dest)
// }

// func generateRandSpanID() (result [8]byte) {
// 	rand.Read(result[:])
// 	return result
// }

// func fillSpanOne(span pdata.Span) {
// 	span.SetTraceID(pdata.NewTraceID(traceID1))
// 	span.SetSpanID(pdata.NewSpanID(generateRandSpanID()))
// 	span.SetName("operationA")
// 	span.SetStartTimestamp(testSpanStartTimestamp)
// 	span.SetEndTimestamp(testSpanEndTimestamp)
// 	span.SetDroppedAttributesCount(1)
// 	span.SetTraceState("span-trace-state1")
// 	initSpanAttributes(span.Attributes())
// 	evs := span.Events()
// 	ev0 := evs.AppendEmpty()
// 	ev0.SetTimestamp(testSpanEventTimestamp)
// 	ev0.SetName("event-with-attr")
// 	initSpanEventAttributes(ev0.Attributes())
// 	ev0.SetDroppedAttributesCount(2)
// 	ev1 := evs.AppendEmpty()
// 	ev1.SetTimestamp(testSpanEventTimestamp)
// 	ev1.SetName("event")
// 	ev1.SetDroppedAttributesCount(2)
// 	span.SetDroppedEventsCount(1)
// 	status := span.Status()
// 	status.SetCode(pdata.StatusCodeError)
// 	status.SetMessage("status-cancelled")
// }

// func fillSpanTwo(span pdata.Span) {
// 	span.SetTraceID(pdata.NewTraceID(traceID2))
// 	span.SetSpanID(pdata.NewSpanID(generateRandSpanID()))
// 	span.SetName("operationB")
// 	span.SetStartTimestamp(testSpanStartTimestamp)
// 	span.SetEndTimestamp(testSpanEndTimestamp)
// 	span.SetTraceState("span-trace-state2")
// 	initSpanAttributes(span.Attributes())
// 	link0 := span.Links().AppendEmpty()
// 	initSpanLinkAttributes(link0.Attributes())
// 	link0.SetTraceID(pdata.NewTraceID([16]byte{'1'}))
// 	link0.SetSpanID(pdata.NewSpanID(generateRandSpanID()))
// 	link0.SetDroppedAttributesCount(4)
// 	link0.SetTraceState("link-trace-state1")
// 	link1 := span.Links().AppendEmpty()
// 	link1.SetDroppedAttributesCount(4)
// 	link1.SetTraceID(pdata.NewTraceID([16]byte{'1'}))
// 	link1.SetSpanID(pdata.NewSpanID(generateRandSpanID()))
// 	link1.SetTraceState("link-trace-state2")
// 	span.SetDroppedLinksCount(3)
// }

// deep copy the traces since we mutate them.
func copyTraces(traces pdata.Traces) pdata.Traces {
	return traces.Clone()
}

func newTracesRequest(traces pdata.Traces) (req otlpgrpc.TracesRequest) {
	req = otlpgrpc.NewTracesRequest()
	req.SetTraces(traces)
	return
}
