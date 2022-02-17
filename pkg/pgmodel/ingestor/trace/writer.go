// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package trace

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/timestamp"

	"go.opentelemetry.io/collector/model/pdata"

	"github.com/jackc/pgtype"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/metrics"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/telemetry"
	tput "github.com/timescale/promscale/pkg/util/throughput"
)

type TagType uint

const (
	SpanTagType TagType = 1 << iota
	ResourceTagType
	EventTagType
	LinkTagType
)
const (
	missingServiceName = "OTLPResourceNoServiceName"
	serviceNameTagKey  = "service.name"
)

const (
	insertSpanLinkSQL = `INSERT INTO _ps_trace.link (trace_id, span_id, span_start_time, linked_trace_id, linked_span_id, trace_state, tags, dropped_tags_count, link_nbr)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	insertSpanEventSQL = `INSERT INTO _ps_trace.event (time, trace_id, span_id, name, event_nbr, tags, dropped_tags_count)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`
	insertSpanSQL = `INSERT INTO _ps_trace.span (trace_id, span_id, trace_state, parent_span_id, operation_id, start_time, end_time, span_tags, dropped_tags_count,
		event_time, dropped_events_count, dropped_link_count, status_code, status_message, instrumentation_lib_id, resource_tags, resource_dropped_tags_count, resource_schema_url_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
		ON CONFLICT DO NOTHING`  // Most cases conflict only happens on retries, safe to ignore duplicate data.
)

type Writer interface {
	InsertTraces(ctx context.Context, traces pdata.Traces) error
}

type traceWriterImpl struct {
	conn pgxconn.PgxConn

	schemaCache  *clockcache.Cache
	instLibCache *clockcache.Cache
	opCache      *clockcache.Cache
	tagCache     *clockcache.Cache
}

func NewWriter(conn pgxconn.PgxConn, t telemetry.Engine) *traceWriterImpl {
	if err := t.RegisterMetric("promscale_ingested_spans_total", metrics.IngestorItems.With(prometheus.Labels{"type": "trace", "kind": "span", "subsystem": ""})); err != nil {
		log.Debug("msg", "err registering telemetry metric promscale_ingested_spans_total", "err", err.Error())
	}
	return &traceWriterImpl{
		conn:         conn,
		schemaCache:  newSchemaCache(),
		instLibCache: newInstrumentationLibraryCache(),
		opCache:      newOperationCache(),
		tagCache:     newTagCache(),
	}
}

func (t *traceWriterImpl) queueSpanLinks(linkBatch pgxconn.PgxBatch, tagsBatch tagBatch, links pdata.SpanLinkSlice, traceID pgtype.UUID, spanID pgtype.Int8, spanStartTime time.Time) error {
	for i := 0; i < links.Len(); i++ {
		link := links.At(i)
		linkedSpanID := getSpanID(link.SpanID().Bytes())

		jsonTags, err := tagsBatch.GetTagMapJSON(link.Attributes().AsRaw(), LinkTagType)
		if err != nil {
			return err
		}
		linkBatch.Queue(insertSpanLinkSQL,
			traceID,
			spanID,
			spanStartTime,
			TraceIDToUUID(link.TraceID().Bytes()),
			linkedSpanID,
			getTraceStateValue(link.TraceState()),
			string(jsonTags),
			link.DroppedAttributesCount(),
			i,
		)
	}
	return nil
}

func (t *traceWriterImpl) queueSpanEvents(eventBatch pgxconn.PgxBatch, tagsBatch tagBatch, events pdata.SpanEventSlice, traceID pgtype.UUID, spanID pgtype.Int8) error {
	for i := 0; i < events.Len(); i++ {
		event := events.At(i)
		jsonTags, err := tagsBatch.GetTagMapJSON(event.Attributes().AsRaw(), EventTagType)
		if err != nil {
			return err
		}
		eventBatch.Queue(insertSpanEventSQL,
			event.Timestamp().AsTime(),
			traceID,
			spanID,
			event.Name(),
			i,
			string(jsonTags),
			event.DroppedAttributesCount(),
		)

		if err != nil {
			return err
		}
	}
	return nil
}

func getServiceName(rSpan pdata.ResourceSpans) string {
	serviceName := missingServiceName
	av, found := rSpan.Resource().Attributes().Get(serviceNameTagKey)
	if found {
		serviceName = av.AsString()
	}
	return serviceName
}

var (
	traceLabel      = prometheus.Labels{"type": "trace"}
	traceSpanLabel  = prometheus.Labels{"type": "trace", "kind": "span"}
	traceEventLabel = prometheus.Labels{"type": "trace", "kind": "event"}
	traceLinkLabel  = prometheus.Labels{"type": "trace", "kind": "link"}
)

func (t *traceWriterImpl) InsertTraces(ctx context.Context, traces pdata.Traces) error {
	startIngest := time.Now() // Time taken for complete ingestion => Processing + DB insert.
	code := "500"
	metrics.IngestorActiveWriteRequests.With(traceSpanLabel).Inc()
	metrics.IngestorItemsReceived.With(traceSpanLabel).Add(float64(traces.SpanCount()))
	defer func() {
		metrics.IngestorRequests.With(prometheus.Labels{"type": "trace", "code": code}).Inc()
		metrics.IngestorDuration.With(prometheus.Labels{"type": "trace", "code": ""}).Observe(time.Since(startIngest).Seconds())
		metrics.IngestorActiveWriteRequests.With(traceSpanLabel).Dec()
	}()

	rSpans := traces.ResourceSpans()

	sURLBatch := newSchemaUrlBatch(t.schemaCache)
	for i := 0; i < rSpans.Len(); i++ {
		rSpan := rSpans.At(i)
		url := rSpan.SchemaUrl()
		sURLBatch.Queue(url)

		instLibSpans := rSpan.InstrumentationLibrarySpans()
		for j := 0; j < instLibSpans.Len(); j++ {
			instLibSpan := instLibSpans.At(j)
			url := instLibSpan.SchemaUrl()
			sURLBatch.Queue(url)
		}
	}
	if err := sURLBatch.SendBatch(ctx, t.conn); err != nil {
		return err
	}

	instrLibBatch := newInstrumentationLibraryBatch(t.instLibCache)
	operationBatch := newOperationBatch(t.opCache)
	tagsBatch := newTagBatch(t.tagCache)
	for i := 0; i < rSpans.Len(); i++ {
		rSpan := rSpans.At(i)
		serviceName := getServiceName(rSpan)
		instLibSpans := rSpan.InstrumentationLibrarySpans()
		for j := 0; j < instLibSpans.Len(); j++ {
			instLibSpan := instLibSpans.At(j)
			instLib := instLibSpan.InstrumentationLibrary()

			sURLID, err := sURLBatch.GetID(instLibSpan.SchemaUrl())
			if err != nil {
				return err
			}
			instrLibBatch.Queue(instLib.Name(), instLib.Version(), sURLID)

			spans := instLibSpan.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				spanName := span.Name()
				spanKind := span.Kind().String()

				operationBatch.Queue(serviceName, spanName, spanKind)

				rawResourceTags := rSpan.Resource().Attributes().AsRaw()
				if err := tagsBatch.Queue(rawResourceTags, ResourceTagType); err != nil {
					return err
				}

				rawSpanTags := span.Attributes().AsRaw()
				if err := tagsBatch.Queue(rawSpanTags, SpanTagType); err != nil {
					return err
				}
				for i := 0; i < span.Events().Len(); i++ {
					event := span.Events().At(i)
					rawTags := event.Attributes().AsRaw()
					if err := tagsBatch.Queue(rawTags, EventTagType); err != nil {
						return err
					}
				}

				for i := 0; i < span.Links().Len(); i++ {
					link := span.Links().At(i)

					rawTags := link.Attributes().AsRaw()
					if err := tagsBatch.Queue(rawTags, LinkTagType); err != nil {
						return err
					}
				}

			}
		}
	}

	metrics.InsertBatchSize.With(prometheus.Labels{"type": "trace", "kind": "instrumentation_lib"}).Observe(float64(instrLibBatch.b.Len()))
	metrics.InsertBatchSize.With(prometheus.Labels{"type": "trace", "kind": "operation"}).Observe(float64(operationBatch.b.Len()))
	metrics.InsertBatchSize.With(prometheus.Labels{"type": "trace", "kind": "tag"}).Observe(float64(tagsBatch.b.Len()))
	if err := instrLibBatch.SendBatch(ctx, t.conn); err != nil {
		return err
	}
	if err := operationBatch.SendBatch(ctx, t.conn); err != nil {
		return err
	}
	if err := tagsBatch.SendBatch(ctx, t.conn); err != nil {
		return err
	}

	maxEndTimestamp := uint64(0)
	spanBatch := t.conn.NewBatch()
	linkBatch := t.conn.NewBatch()
	eventBatch := t.conn.NewBatch()
	for i := 0; i < rSpans.Len(); i++ {
		rSpan := rSpans.At(i)
		instLibSpans := rSpan.InstrumentationLibrarySpans()
		serviceName := getServiceName(rSpan)

		url := rSpan.SchemaUrl()
		rSchemaURLID, err := sURLBatch.GetID(url)
		if err != nil {
			return err
		}
		for j := 0; j < instLibSpans.Len(); j++ {
			instLibSpan := instLibSpans.At(j)
			instLib := instLibSpan.InstrumentationLibrary()

			sURLID, err := sURLBatch.GetID(instLibSpan.SchemaUrl())
			if err != nil {
				return err
			}
			instLibID, err := instrLibBatch.GetID(instLib.Name(), instLib.Version(), sURLID)
			if err != nil {
				return err
			}

			spans := instLibSpan.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				traceID := TraceIDToUUID(span.TraceID().Bytes())
				spanID := getSpanID(span.SpanID().Bytes())
				parentSpanID := getSpanID(span.ParentSpanID().Bytes())
				spanName := span.Name()
				spanKind := span.Kind().String()
				operationID, err := operationBatch.GetID(serviceName, spanName, spanKind)
				if err != nil {
					return err
				}

				if err := t.queueSpanEvents(eventBatch, tagsBatch, span.Events(), traceID, spanID); err != nil {
					return err
				}
				if err := t.queueSpanLinks(linkBatch, tagsBatch, span.Links(), traceID, spanID, span.StartTimestamp().AsTime()); err != nil {
					return err
				}

				jsonResourceTags, err := tagsBatch.GetTagMapJSON(rSpan.Resource().Attributes().AsRaw(), ResourceTagType)
				if err != nil {
					return err
				}

				jsonTags, err := tagsBatch.GetTagMapJSON(span.Attributes().AsRaw(), SpanTagType)
				if err != nil {
					return err
				}

				eventTimeRange := getEventTimeRange(span.Events())

				// postgresql timestamptz only has microsecond precision while time.Time has nanosecond precision
				start := span.StartTimestamp().AsTime().Truncate(time.Microsecond)
				end := span.EndTimestamp().AsTime().Truncate(time.Microsecond)
				// make sure start <= end
				if end.Before(start) {
					start, end = end, start
				}

				if maxEndTimestamp < uint64(end.Unix()) {
					maxEndTimestamp = uint64(end.Unix())
				}

				spanBatch.Queue(
					insertSpanSQL,
					traceID,
					spanID,
					getTraceStateValue(span.TraceState()),
					parentSpanID,
					operationID,
					start,
					end,
					string(jsonTags),
					span.DroppedAttributesCount(),
					eventTimeRange,
					span.DroppedEventsCount(),
					span.DroppedLinksCount(),
					span.Status().Code().String(),
					span.Status().Message(),
					instLibID,
					string(jsonResourceTags),
					0, // TODO: Add resource_dropped_tags_count when it gets exposed upstream.
					rSchemaURLID,
				)
			}
		}
	}

	metrics.InsertBatchSize.With(traceSpanLabel).Observe(float64(spanBatch.Len()))
	metrics.InsertBatchSize.With(traceEventLabel).Observe(float64(eventBatch.Len()))
	metrics.InsertBatchSize.With(traceLinkLabel).Observe(float64(linkBatch.Len()))

	start := time.Now()
	if err := t.sendBatch(ctx, eventBatch); err != nil {
		return fmt.Errorf("error sending trace event batch: %w", err)
	}
	metrics.IngestorInsertDuration.With(prometheus.Labels{"type": "trace", "subsystem": "", "kind": "event"}).Observe(time.Since(start).Seconds())

	start = time.Now()
	if err := t.sendBatch(ctx, linkBatch); err != nil {
		return fmt.Errorf("error sending trace link batch: %w", err)
	}
	metrics.IngestorInsertDuration.With(prometheus.Labels{"type": "trace", "subsystem": "", "kind": "link"}).Observe(time.Since(start).Seconds())

	start = time.Now()
	if err := t.sendBatch(ctx, spanBatch); err != nil {
		return fmt.Errorf("error sending trace span batch: %w", err)
	}
	metrics.IngestorItems.With(prometheus.Labels{"type": "trace", "kind": "span", "subsystem": ""}).Add(float64(traces.SpanCount()))
	metrics.IngestorInsertDuration.With(prometheus.Labels{"type": "trace", "subsystem": "", "kind": "span"}).Observe(time.Since(start).Seconds())

	code = "2xx"
	metrics.IngestorInsertDuration.With(prometheus.Labels{"type": "trace", "subsystem": "", "kind": "span"}).Observe(time.Since(start).Seconds())
	metrics.IngestorMaxSentTimestamp.With(traceLabel).Set(float64(maxEndTimestamp))

	// Only report telemetry if ingestion successful.
	tput.ReportSpansProcessed(timestamp.FromTime(time.Now()), traces.SpanCount())
	return nil
}

func (t *traceWriterImpl) sendBatch(ctx context.Context, batch pgxconn.PgxBatch) error {
	br, err := t.conn.SendBatch(ctx, batch)
	if err != nil {
		return err
	}
	if err = br.Close(); err != nil {
		return err
	}
	return nil
}

func ByteArrayToInt64(buf [8]byte) int64 {
	return int64(binary.BigEndian.Uint64(buf[:]))
}

func Int64ToByteArray(x int64) [8]byte {
	var res [8]byte
	binary.BigEndian.PutUint64(res[:], uint64(x))
	return res
}

func TraceIDToUUID(buf [16]byte) pgtype.UUID {
	return pgtype.UUID{
		Bytes:  buf,
		Status: pgtype.Present,
	}
}

func getSpanID(buf [8]byte) pgtype.Int8 {
	i := int64(binary.BigEndian.Uint64(buf[:]))

	if i != 0 {
		return pgtype.Int8{
			Int:    i,
			Status: pgtype.Present,
		}
	}

	return pgtype.Int8{
		Status: pgtype.Null,
	}
}

func getEventTimeRange(events pdata.SpanEventSlice) (result pgtype.Tstzrange) {
	if events.Len() == 0 {
		result.Status = pgtype.Null
		return result
	}

	var lowerTime, upperTime time.Time

	for i := 0; i < events.Len(); i++ {
		eventTime := events.At(i).Timestamp().AsTime()

		if lowerTime.IsZero() || eventTime.Before(lowerTime) {
			lowerTime = eventTime
		}
		if upperTime.IsZero() || eventTime.After(upperTime) {
			upperTime = eventTime
		}
	}

	result = pgtype.Tstzrange{
		Lower:     pgtype.Timestamptz{Time: lowerTime, Status: pgtype.Present},
		Upper:     pgtype.Timestamptz{Time: upperTime, Status: pgtype.Present},
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Exclusive,
		Status:    pgtype.Present,
	}

	return result
}

func getTraceStateValue(ts pdata.TraceState) (result pgtype.Text) {
	if string(ts) == "" {
		result.Status = pgtype.Null
	} else {
		result.String = string(ts)
		result.Status = pgtype.Present
	}

	return result
}
