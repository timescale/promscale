// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package jaeger_query

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/model/pdata"

	"github.com/jackc/pgtype"
	"github.com/jaegertracing/jaeger/model"
	jaegertranslator "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger"
	"github.com/timescale/promscale/pkg/pgxconn"
)

type traceRowsIterator pgxconn.PgxRows

func getTraces(itr traceRowsIterator) ([]*model.Batch, error) {
	var (
		// Iteration vars.
		// Each element in the array corresponds to one span, of a trace.

		// From span table.
		traceId             pgtype.UUID
		spanId              int64
		parentSpanId        pgtype.Int8
		startTime           time.Time
		endTime             time.Time
		kind                pgtype.Text
		droppedTagsCounts   int
		droppedEventsCounts int
		droppedLinkCounts   int
		traceState          pgtype.Text
		schemaUrl           pgtype.Text
		spanName            string
		resourceTags        = make(map[string]interface{})
		spanTags            = make(map[string]interface{}, 0)

		// From events table.
		// We need Slice of pointers since any individual element can be nil.
		eventNames            = &[]*string{}
		eventTimes            = &[]*time.Time{}
		eventDroppedTagsCount = &[]*int{}
		eventTags             = make([]map[string]interface{}, 0)

		// From instrumentation lib table.
		instLibName      = new(string)
		instLibVersion   = new(string)
		instLibSchemaUrl = new(string)

		// From link table.
		linksLinkedTraceIds   pgtype.UUIDArray
		linksLinkedSpanIds    = &[]*int64{}
		linksTraceStates      = &[]*string{}
		linksDroppedTagsCount = &[]*int{}
		linksTags             = make([]map[string]interface{}, 0)

		err error

		traces = pdata.NewTraces()
	)

	for itr.Next() {
		// Each iteration in this block represents one trace.
		// rawTraces
		if itr.Err() != nil {
			err = fmt.Errorf("raw-traces iterator: %w", itr.Err())
			break
		}
		// Each scan is a scan for a complete Trace. This means,
		// it contains data from multiple spans, and hence, an array.
		if err = itr.Scan(
			// Span table.
			&traceId,
			&spanId,
			&parentSpanId,
			&startTime,
			&endTime,
			&kind,
			&droppedTagsCounts,
			&droppedEventsCounts,
			&droppedLinkCounts,
			&traceState,
			&schemaUrl,
			&spanName,
			&resourceTags,
			&spanTags,

			// Event table.
			&eventNames, // 14
			&eventTimes,
			&eventDroppedTagsCount,
			&eventTags,

			// Instrumentation lib table.
			&instLibName,
			&instLibVersion,
			&instLibSchemaUrl,

			// Link table.
			&linksLinkedTraceIds,
			&linksLinkedSpanIds,
			&linksTraceStates,
			&linksDroppedTagsCount,
			&linksTags,
		); err != nil {
			err = fmt.Errorf("scanning raw-traces: %w", err)
			break
		}

		if err = makeSpan(
			// From span table.
			traces.ResourceSpans().AppendEmpty(),
			traceId, spanId, parentSpanId,
			startTime, endTime,
			kind,
			droppedTagsCounts, droppedEventsCounts, droppedLinkCounts,
			traceState, schemaUrl, spanName,
			resourceTags, spanTags,

			// From event table.
			eventNames,
			eventTimes,
			eventDroppedTagsCount,
			eventTags,

			// From instrumentation lib table.
			instLibName,
			instLibVersion,
			instLibSchemaUrl,

			// From link table.
			linksLinkedTraceIds,
			linksLinkedSpanIds,
			linksTraceStates,
			linksDroppedTagsCount,
			linksTags); err != nil {
			return nil, fmt.Errorf("make span: %w", err)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("iterating raw-traces: %w", err)
	}

	batch, err := jaegertranslator.InternalTracesToJaegerProto(traces)
	if err != nil {
		return nil, fmt.Errorf("internal-traces-to-jaeger-proto: %w", err)
	}
	return applyProcess(batch), nil
}

func applyProcess(b []*model.Batch) []*model.Batch {
	for i := range b {
		p := b[i].Process
		for j := range b[i].Spans {
			b[i].Spans[j].Process = p
		}
	}
	return b
}

func batchSliceToTraceSlice(bSlice []*model.Batch) []*model.Trace {
	// Copied from Jaeger's grpc_client.go
	// https://github.com/jaegertracing/jaeger/blob/067dff713ab635ade66315bbd05518d7b28f40c6/plugin/storage/grpc/shared/grpc_client.go#L179
	traces := make([]*model.Trace, 0)
	var traceID model.TraceID
	var trace *model.Trace
	for _, batch := range bSlice {
		for i, span := range batch.Spans {
			if span.TraceID != traceID {
				trace = &model.Trace{}
				traceID = span.TraceID
				traces = append(traces, trace)
			}
			trace.Spans = append(trace.Spans, batch.Spans[i])
		}
	}
	return traces
}
