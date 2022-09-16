// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package store

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	jaegertranslator "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger"
	"github.com/timescale/promscale/pkg/pgxconn"
	"go.opentelemetry.io/collector/pdata/ptrace"
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
	spansBinaryTags := map[int64]*spanBinaryTags{}
	for rows.Next() {
		if rows.Err() != nil {
			return nil, fmt.Errorf("trace row iterator: %w", rows.Err())
		}
		var err error
		spanBinaryTags, err := ScanRow(rows, &traces)
		if err != nil {
			return nil, fmt.Errorf("error scanning trace: %w", err)
		}
		if !spanBinaryTags.isEmpty() {
			spansBinaryTags[spanBinaryTags.spanID] = spanBinaryTags
		}

	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("trace row iterator: %w", rows.Err())
	}

	batch, err := jaegertranslator.ProtoFromTraces(traces)
	if err != nil {
		return nil, fmt.Errorf("internal-traces-to-jaeger-proto: %w", err)
	}

	return batchSliceToTraceSlice(batch, spansBinaryTags), nil
}

func batchSliceToTraceSlice(bSlice []*model.Batch, spansWithBinaryTag map[int64]*spanBinaryTags) []*model.Trace {
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
			if binaryTags, ok := spansWithBinaryTag[int64(span.SpanID)]; ok {
				decodeBinaryTags(span, binaryTags)
			}
			trace.Spans = append(trace.Spans, span)
		}
	}
	return traces
}

func decodeBinaryTags(span *model.Span, binaryTags *spanBinaryTags) {
	if len(binaryTags.spanTags) != 0 {
		doDecodeBinaryTags(span.Tags, binaryTags.spanTags)
	}

	if len(binaryTags.processTags) != 0 {
		doDecodeBinaryTags(span.Process.Tags, binaryTags.processTags)
	}

	if len(binaryTags.logsTags) != 0 {
		for i, logsTags := range binaryTags.logsTags {
			doDecodeBinaryTags(span.Logs[i].Fields, logsTags)
		}
	}
}

func doDecodeBinaryTags(actualTags []model.KeyValue, binaryTags map[string]string) {
	for i, tag := range actualTags {
		if tag.GetVType() != model.ValueType_STRING {
			continue
		}
		strV, ok := binaryTags[tag.Key]
		if !ok || strV != tag.VStr {
			continue
		}
		vBin, err := base64.StdEncoding.DecodeString(tag.VStr)
		// If we can't decode it means that we didn't encode it in the
		// first place, so we should keep it as is.
		if err != nil {
			continue
		}
		actualTags[i] = model.KeyValue{
			Key:     tag.Key,
			VType:   model.ValueType_BINARY,
			VBinary: vBin,
		}
	}
}
