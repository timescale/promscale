// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"context"
	"fmt"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const (
	getCreateMetricsTableWithNewSQL = "SELECT table_name, possibly_new FROM " + schema.Catalog + ".get_or_create_metric_table_name($1)"
)

type readRequest struct {
	copySender <-chan copyRequest
}

func metricTableName(conn pgxconn.PgxConn, metric string) (string, bool, error) {
	res, err := conn.Query(
		context.Background(),
		getCreateMetricsTableWithNewSQL,
		metric,
	)

	if err != nil {
		return "", true, fmt.Errorf("failed to get the table name for metric %s: %w", metric, err)
	}

	var tableName string
	var possiblyNew bool
	defer res.Close()
	if !res.Next() {
		if err := res.Err(); err != nil {
			return "", true, fmt.Errorf("failed to get the table name for metric %s: %w", metric, err)
		}
		return "", true, errors.ErrMissingTableName
	}

	if err := res.Scan(&tableName, &possiblyNew); err != nil {
		return "", true, fmt.Errorf("failed to get the table name for metric %s: %w", metric, err)
	}

	if err := res.Err(); err != nil {
		return "", true, fmt.Errorf("failed to get the table name for metric %s: %w", metric, err)
	}

	if tableName == "" {
		return "", true, fmt.Errorf("failed to get the table name for metric %s: empty table name returned", metric)
	}

	return tableName, possiblyNew, nil
}

// Create the metric table for the metric we handle, if it does not already
// exist. This only does the most critical part of metric table creation, the
// rest is handled by completeMetricTableCreation().
func initializeMetricBatcher(conn pgxconn.PgxConn, metricName string, completeMetricCreationSignal chan struct{}, metricTableNames cache.MetricCache) (tableName string, err error) {
	mInfo, err := metricTableNames.Get(schema.Data, metricName)
	if err == nil && mInfo.TableName != "" {
		return mInfo.TableName, nil
	}

	tableName, possiblyNew, err := metricTableName(conn, metricName)
	if err != nil || tableName == "" {
		return "", err
	}

	//ignore error since this is just an optimization
	_ = metricTableNames.Set(
		schema.Data,
		metricName,
		model.MetricInfo{
			TableSchema: schema.Data, TableName: tableName,
			SeriesTable: "",
		},
	)

	if possiblyNew {
		//pass a signal if there is space
		select {
		case completeMetricCreationSignal <- struct{}{}:
		default:
		}
	}
	return tableName, err
}

func runMetricBatcher(conn pgxconn.PgxConn,
	input chan *insertDataRequest,
	metricName string,
	completeMetricCreationSignal chan struct{},
	metricTableNames cache.MetricCache,
	copierReadRequestCh chan<- readRequest,
	labelArrayOID uint32) {

	var tableName string
	var firstReq *insertDataRequest
	firstReqSet := false

	//This channel in synchronous (no buffering). This provides backpressure
	//to the batcher to keep batching until the copier is ready to read.
	copySender := make(chan copyRequest)
	defer close(copySender)
	readRequest := readRequest{copySender: copySender}

	for firstReq = range input {
		var err error
		tableName, err = initializeMetricBatcher(conn, metricName, completeMetricCreationSignal, metricTableNames)
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

	//the basic structure of communication from the batcher to the copier is as follows:
	// 1. the batcher gets a request from the input channel
	// 2. the batcher sends a readRequest to the copier on a channel shared by all the metric batchers
	// 3. the batcher keeps on batching together requests from the input channel as long as the copier isn't ready to receive the batch
	// 4. the copier catches up and gets the read request from the shared channel
	// 5. the copier reads from the channel specified in the read request
	// 6. the batcher is able to send it's batch to the copier

	// Notice some properties:
	// 1. The shared channel in step 2 acts as a queue between metric batchers where the priority is approximately the earliest arrival time of any
	//     request in the batch (that's why we only do step 2 after step 1). Note this means we probably want a single copier reading a batch
	//     of requests consecutively so as to minimize processing delays. That's what the mutex in the copier does.
	// 2. There is an auto-adjusting adaptation loop in step 3. The longer the copier takes to catch up to the readRequest in the queue, the more things will be batched
	// 3. The batcher has only a single read request out at a time.

	pending := NewPendingBuffer()
	pending.addReq(firstReq)
	copierReadRequestCh <- readRequest

	for {
		if pending.IsEmpty() {
			req, ok := <-input
			if !ok {
				return
			}
			pending.addReq(req)
			copierReadRequestCh <- readRequest
		}

		recvCh := input
		if pending.IsFull() {
			recvCh = nil
		}

		numSeries := pending.batch.CountSeries()
		select {
		//try to send first, if not then keep batching
		case copySender <- copyRequest{pending, tableName}:
			MetricBatcherFlushSeries.Observe(float64(numSeries))
			pending = NewPendingBuffer()
		case req, ok := <-recvCh:
			if !ok {
				if !pending.IsEmpty() {
					copySender <- copyRequest{pending, tableName}
					MetricBatcherFlushSeries.Observe(float64(numSeries))
				}
				return
			}
			pending.addReq(req)
		}
	}
}
