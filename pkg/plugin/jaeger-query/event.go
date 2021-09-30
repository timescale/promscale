// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package jaeger_query

import (
	"time"

	"go.opentelemetry.io/collector/model/pdata"
)

func makeEvents(
	spanEventSlice pdata.SpanEventSlice,
	eventNames *[]*string,
	eventTimes *[]*time.Time,
	eventDroppedTagsCount *[]*int,
	eventTags []map[string]interface{}) {
	var (
		// To avoid continuous memory references, its better to store absolute values in a variable.
		names            = *eventNames
		times            = *eventTimes
		droppedTagsCount = *eventDroppedTagsCount
	)

	n := len(*eventNames)
	for i := 0; i < n; i++ {
		event := spanEventSlice.AppendEmpty()
		event.SetName(stringOrEmpty(names[i]))
		if times[i] != nil {
			event.SetTimestamp(pdata.NewTimestampFromTime(*times[i]))
		}
		if droppedTagsCount[i] != nil {
			event.SetDroppedAttributesCount(uint32(*droppedTagsCount[i]))
		}
		if eventTags[i] != nil {
			event.Attributes().InitFromMap(makeAttributes(eventTags[i]))
		}
	}
}
