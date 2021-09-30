// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package jaeger_query

import (
	"fmt"

	"github.com/jackc/pgtype"
	"go.opentelemetry.io/collector/model/pdata"
)

func makeLinks(
	spanEventSlice pdata.SpanLinkSlice,
	linksLinkedTraceIds pgtype.UUIDArray,
	linksLinkedSpanIds *[]*int64,
	linksTraceStates *[]*string,
	linksDroppedTagsCount *[]*int,
	linksTags []map[string]interface{}) error {
	var (
		linkedSpanIds   = *linksLinkedSpanIds
		traceStates     = *linksTraceStates
		droppedTagCount = *linksDroppedTagsCount
		n               = len(linkedSpanIds)
	)

	var linkedTraceIds []*string
	if err := linksLinkedTraceIds.AssignTo(&linkedTraceIds); err != nil {
		return fmt.Errorf("linksLinkedTraceIds: AssignTo: %w", err)
	}

	for i := 0; i < n; i++ {
		if linkedTraceIds[i] == nil && linkedSpanIds[i] == nil {
			// The entire row is nil, hence quickly skip.
			continue
		}
		link := spanEventSlice.AppendEmpty()

		// Needs proper review below. @Mat, @Ante
		if linkedTraceIds[i] != nil {
			traceBSlice := []byte(*linkedTraceIds[i])
			if len(traceBSlice) != 16 {
				return fmt.Errorf("traceBSlice length not 16, rather of length %d", len(traceBSlice))
			}

			var b16 [16]byte
			copy(b16[:16], traceBSlice)

			link.SetTraceID(pdata.NewTraceID(b16))
		} else {
			// If linkedTraceIds is nil, just put an empty trace id.
			link.SetTraceID(pdata.NewTraceID([16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}))
		}

		spanId, err := makeSpanId(linkedSpanIds[i])
		if err != nil {
			return fmt.Errorf("make span id: %w", err)
		}
		link.SetSpanID(spanId)

		link.SetTraceState(pdata.TraceState(stringOrEmpty(traceStates[i])))

		if droppedTagCount[i] != nil {
			link.SetDroppedAttributesCount(uint32(*droppedTagCount[i]))
		}

		if linksTags[i] != nil {
			link.Attributes().InitFromMap(makeAttributes(linksTags[i]))
		}
	}
	return nil
}
