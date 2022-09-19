// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"context"
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	pgErrors "github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/metrics"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/tracer"
)

const getCreateMetricsTableWithNewSQL = "SELECT id, table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)"
const createExemplarTable = "SELECT * FROM _prom_catalog.create_exemplar_table_if_not_exists($1)"

func containsExemplars(data []model.Insertable) bool {
	for _, row := range data {
		if row.IsOfType(model.Exemplar) {
			return true
		}
	}
	return false
}

type readRequest struct {
	copySender <-chan copyRequest
}

func metricTableName(conn pgxconn.PgxConn, metric string) (info model.MetricInfo, possiblyNew bool, err error) {
	res, err := conn.Query(
		context.Background(),
		getCreateMetricsTableWithNewSQL,
		metric,
	)

	if err != nil {
		return info, true, fmt.Errorf("failed to get the table name for metric %s: %w", metric, err)
	}

	defer res.Close()
	if !res.Next() {
		if err := res.Err(); err != nil {
			return info, true, fmt.Errorf("failed to get the table name for metric %s: %w", metric, err)
		}
		return info, true, pgErrors.ErrMissingTableName
	}

	if err := res.Scan(&info.MetricID, &info.TableName, &possiblyNew); err != nil {
		return info, true, fmt.Errorf("failed to get the table name for metric %s: %w", metric, err)
	}

	if err := res.Err(); err != nil {
		return info, true, fmt.Errorf("failed to get the table name for metric %s: %w", metric, err)
	}

	if info.TableName == "" {
		return info, true, fmt.Errorf("failed to get the table name for metric %s: empty table name returned", metric)
	}

	if info.MetricID == 0 {
		return info, true, fmt.Errorf("failed to get the metric ID for metric %s: zero metric ID returned", metric)
	}

	// Setting schema to default data schema and series table defaults to
	// table name. This is always true for raw metrics as is the case here.
	info.TableSchema = schema.PromData
	info.SeriesTable = info.TableName

	return info, possiblyNew, nil
}

// Create the metric table for the metric we handle, if it does not already
// exist. This only does the most critical part of metric table creation, the
// rest is handled by completeMetricTableCreation().
func initializeMetricBatcher(conn pgxconn.PgxConn, metricName string, completeMetricCreationSignal chan struct{}, metricTableNames cache.MetricCache) (info model.MetricInfo, err error) {
	// Metric batchers are always initialized with metric names of samples and not of exemplars.
	mInfo, err := metricTableNames.Get(schema.PromData, metricName, false)
	if err == nil && mInfo.TableName != "" {
		return mInfo, nil
	}

	mInfo, possiblyNew, err := metricTableName(conn, metricName)
	if err != nil || mInfo.TableName == "" {
		return mInfo, err
	}

	// We ignore error here since this is just an optimization.
	//
	// Always set metric name while initializing with exemplars as false, since
	// the metric name set here is via fetching the metric name from metric table.
	//
	// Metric table is filled during start, but exemplar table is filled when we
	// first see an exemplar. Hence, that's the place to sent isExemplar as true.
	_ = metricTableNames.Set(
		schema.PromData,
		metricName,
		mInfo,
		false,
	)

	if possiblyNew {
		//pass a signal if there is space
		select {
		case completeMetricCreationSignal <- struct{}{}:
		default:
		}
	}
	return mInfo, err
}

// initilizeExemplars creates the necessary tables for exemplars. Called lazily only if exemplars are found
func initializeExemplars(conn pgxconn.PgxConn, metricName string) error {
	// We are seeing the exemplar belonging to this metric first time. It may be the
	// first time of this exemplar in the database. So, let's attempt to create a table
	// if it does not exists.
	var created bool
	err := conn.QueryRow(context.Background(), createExemplarTable, metricName).Scan(&created)
	if err != nil {
		return fmt.Errorf("error initializing exemplar tables for %s: %w", metricName, err)
	}
	return nil
}

func runMetricBatcher(conn pgxconn.PgxConn,
	input chan *insertDataRequest,
	metricName string,
	completeMetricCreationSignal chan struct{},
	metricTableNames cache.MetricCache,
	copierReadRequestCh chan<- readRequest,
) {
	var (
		info        model.MetricInfo
		firstReq    *insertDataRequest
		firstReqSet = false
	)

	for firstReq = range input {
		var err error
		info, err = initializeMetricBatcher(conn, metricName, completeMetricCreationSignal, metricTableNames)
		if err != nil {
			err := fmt.Errorf("initializing the insert routine for metric %v has failed with %w", metricName, err)
			log.Error("msg", err)
			firstReq.reportResult(err)
		} else {
			firstReqSet = true
			break
		}
	}

	//input channel was closed before getting a successful request
	if !firstReqSet {
		return
	}
	sendBatches(firstReq, input, conn, &info, copierReadRequestCh)
}

//the basic structure of communication from the batcher to the copier is as follows:
// 1. the batcher gets a request from the input channel
// 2. the batcher sends a readRequest to the copier on a channel shared by all the metric batchers
// 3. the batcher keeps on batching together requests from the input channel as long as the copier isn't ready to receive the batch
// 4. the copier catches up and gets the read request from the shared channel
// 5. the copier reads from the channel specified in the read request
// 6. the batcher is able to send its batch to the copier

// Notice some properties:
// 1. The shared channel in step 2 acts as a queue between metric batchers where the priority is approximately the earliest arrival time of any
//     request in the batch (that's why we only do step 2 after step 1). Note this means we probably want a single copier reading a batch
//     of requests consecutively to minimize processing delays. That's what the mutex in the copier does.
// 2. There is an auto-adjusting adaptation loop in step 3. The longer the copier takes to catch up to the readRequest in the queue, the more things will be batched
// 3. The batcher has only a single read request out at a time.
func sendBatches(firstReq *insertDataRequest, input chan *insertDataRequest, conn pgxconn.PgxConn, info *model.MetricInfo, copierReadRequestCh chan<- readRequest) {
	var (
		exemplarsInitialized = false
		span                 trace.Span
	)

	addReq := func(req *insertDataRequest, buf *pendingBuffer) {
		if !exemplarsInitialized && containsExemplars(req.data) {
			if err := initializeExemplars(conn, info.TableName); err != nil {
				log.Error("msg", err)
				req.reportResult(err)
				return
			}
			exemplarsInitialized = true
		}
		_, addSpan := tracer.Default().Start(buf.spanCtx, "add-req",
			trace.WithLinks(
				trace.Link{
					SpanContext: req.spanCtx,
				},
			),
			trace.WithAttributes(attribute.Int("insertable_count", len(req.data))),
		)
		buf.addReq(req)
		addSpan.End()
	}
	//This channel in synchronous (no buffering). This provides backpressure
	//to the batcher to keep batching until the copier is ready to read.
	copySender := make(chan copyRequest)
	defer close(copySender)
	readRequest := readRequest{copySender: copySender}

	pending := NewPendingBuffer()
	pending.spanCtx, span = tracer.Default().Start(context.Background(), "send-batches")
	span.SetAttributes(attribute.String("metric", info.TableName))
	addReq(firstReq, pending)
	copierReadRequestCh <- readRequest
	span.AddEvent("Sent a read request")

	for {
		if pending.IsEmpty() {
			span.AddEvent("Buffer is empty")
			req, ok := <-input
			if !ok {
				return
			}
			addReq(req, pending)
			copierReadRequestCh <- readRequest
			span.AddEvent("Sent a read request")
		}

		recvCh := input
		if pending.IsFull() {
			span.AddEvent("Buffer is full")
			recvCh = nil
		}

		numSeries := pending.batch.CountSeries()

		select {
		//try to send first, if not then keep batching
		case copySender <- copyRequest{pending, info}:
			metrics.IngestorFlushSeries.With(prometheus.Labels{"type": "metric", "subsystem": "metric_batcher"}).Observe(float64(numSeries))
			span.SetAttributes(attribute.Int("num_series", numSeries))
			span.End()
			pending = NewPendingBuffer()
			pending.spanCtx, span = tracer.Default().Start(context.Background(), "send-batches")
			span.SetAttributes(attribute.String("metric", info.TableName))
		case req, ok := <-recvCh:
			if !ok {
				if !pending.IsEmpty() {
					span.AddEvent("Sending last non-empty batch")
					copySender <- copyRequest{pending, info}
					metrics.IngestorFlushSeries.With(prometheus.Labels{"type": "metric", "subsystem": "metric_batcher"}).Observe(float64(numSeries))
				}
				span.AddEvent("Exiting metric batcher batch loop")
				span.SetAttributes(attribute.Int("num_series", numSeries))
				span.End()
				return
			}
			addReq(req, pending)
		}
	}
}
