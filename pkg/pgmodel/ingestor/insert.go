// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/common/model"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/metrics"
	pgmodel "github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
)

type copyRequest struct {
	data  *pendingBuffer
	table string
}

var getBatchMutex = &sync.Mutex{}

// Create the metric table for the metric we handle, if it does not already
// exist. This only does the most critical part of metric table creation, the
// rest is handled by completeMetricTableCreation().
func initializeInserterRoutine(conn pgxconn.PgxConn, metricName string, completeMetricCreationSignal chan struct{}, metricTableNames cache.MetricCache) (tableName string, err error) {
	tableName, err = metricTableNames.Get(metricName)
	if err == errors.ErrEntryNotFound {
		var possiblyNew bool
		tableName, possiblyNew, err = pgmodel.MetricTableName(conn, metricName)
		if err != nil {
			return "", err
		}

		//ignore error since this is just an optimization
		_ = metricTableNames.Set(metricName, tableName)

		if possiblyNew {
			//pass a signal if there is space
			select {
			case completeMetricCreationSignal <- struct{}{}:
			default:
			}
		}
	} else if err != nil {
		return "", err
	}
	return tableName, err
}

func runInserterRoutine(conn pgxconn.PgxConn, input chan *insertDataRequest, metricName string, completeMetricCreationSignal chan struct{}, metricTableNames cache.MetricCache, toCopiers chan copyRequest) {
	var tableName string
	var firstReq *insertDataRequest
	firstReqSet := false
	for firstReq = range input {
		var err error
		tableName, err = initializeInserterRoutine(conn, metricName, completeMetricCreationSignal, metricTableNames)
		if err != nil {
			firstReq.reportResult(fmt.Errorf("initializing the insert routine has failed with %w", err))
		} else {
			firstReqSet = true
			break
		}
	}

	//input channel was closed before getting a successful request
	if !firstReqSet {
		return
	}

	handler := insertHandler{
		conn:            conn,
		input:           input,
		pending:         NewPendingBuffer(),
		metricTableName: tableName,
		toCopiers:       toCopiers,
	}

	handler.handleReq(firstReq)

	// Grab new requests from our channel and handle them. We do this hot-load
	// style: we keep grabbing requests off the channel while we can do so
	// without blocking, and flush them to the next layer when we run out, or
	// reach a predetermined threshold. The theory is that wake/sleep and
	// flushing is relatively expensive, and can be easily amortized over
	// multiple requests, so it pays to batch as much as we are able. However,
	// writes to a given metric can be relatively rare, so if we don't have
	// additional requests immediately we're likely not going to for a while.
	for {
		if handler.pending.IsEmpty() {
			stillAlive := handler.blockingHandleReq()
			if !stillAlive {
				return
			}
			continue
		}

	hotReceive:
		for handler.nonblockingHandleReq() {
			if handler.pending.IsFull() {
				break hotReceive
			}
		}

		handler.flush()
	}
}

// Handles actual insertion into the DB.
// We have one of these per connection reserved for insertion.
func runInserter(conn pgxconn.PgxConn, in chan copyRequest) {
	// We grab copyRequests off the channel one at a time. This, and the name is
	// a legacy from when we used CopyFrom to perform the insertions, and may
	// change in the future.
	insertBatch := make([]copyRequest, 0, maxCopyRequestsPerTxn)
	for {
		var ok bool
		insertBatch, ok = inserterGetBatch(insertBatch, in)
		if !ok {
			return
		}

		// sort to prevent deadlocks
		sort.Slice(insertBatch, func(i, j int) bool {
			return insertBatch[i].table < insertBatch[j].table
		})

		//merge tables to prevent deadlocks (on row order) and for efficiency

		// invariant: there's a gap-free prefix containing data we want to insert
		//            in table order, Following this is a gap which once contained
		//            batches we compacted away, following this is the
		//            uncompacted data.
		dst := 0
		for src := 1; src < len(insertBatch); src++ {
			if insertBatch[dst].table == insertBatch[src].table {
				insertBatch[dst].data.absorb(insertBatch[src].data)
				insertBatch[src].data.release()
				insertBatch[src] = copyRequest{}
			} else {
				dst++
				if dst != src {
					insertBatch[dst] = insertBatch[src]
					insertBatch[src] = copyRequest{}
				}
			}
		}
		insertBatch = insertBatch[:dst+1]

		doInsertOrFallback(conn, insertBatch...)
		for i := range insertBatch {
			insertBatch[i] = copyRequest{}
		}
		insertBatch = insertBatch[:0]
	}
}

func inserterGetBatch(batch []copyRequest, in chan copyRequest) ([]copyRequest, bool) {
	//This mutex is not for safety, but rather for better batching.
	//It guarantees that only one copier is reading from the channel at one time
	//This ensures bigger batches as well as less spread of a
	//single http request to multiple copiers, decreasing latency via less sync
	//overhead especially on low-pressure systems.
	getBatchMutex.Lock()
	defer getBatchMutex.Unlock()
	req, ok := <-in
	if !ok {
		return batch, false
	}
	batch = append(batch, req)

	//we use a small timeout to prevent low-pressure systems from using up too many
	//txns and putting pressure on system
	timeout := time.After(20 * time.Millisecond)
hot_gather:
	for len(batch) < cap(batch) {
		select {
		case r2 := <-in:
			batch = append(batch, r2)
		case <-timeout:
			break hot_gather
		}
	}
	return batch, true
}

func doInsertOrFallback(conn pgxconn.PgxConn, reqs ...copyRequest) {
	err := doInsert(conn, reqs...)
	if err != nil {
		insertBatchErrorFallback(conn, reqs...)
		return
	}

	for i := range reqs {
		reqs[i].data.reportResults(nil)
		reqs[i].data.release()
	}
}

func insertBatchErrorFallback(conn pgxconn.PgxConn, reqs ...copyRequest) {
	for i := range reqs {
		reqs[i].data.batch.ResetPosition()
		err := doInsert(conn, reqs[i])
		if err != nil {
			err = insertErrorFallback(conn, err, reqs[i])
		}

		reqs[i].data.reportResults(err)
		reqs[i].data.release()
	}
}

// certain errors are recoverable, handle those we can
//   1. if the table is compressed, decompress and retry the insertion
func insertErrorFallback(conn pgxconn.PgxConn, err error, req copyRequest) error {
	err = tryRecovery(conn, err, req)
	if err != nil {
		log.Warn("msg", fmt.Sprintf("time out while processing error for %s", req.table), "error", err.Error())
		return err
	}
	return doInsert(conn, req)
}

// we can currently recover from one error:
// If we inserted into a compressed chunk, we decompress the chunk and try again.
// Since a single batch can have both errors, we need to remember the insert method
// we're using, so that we deduplicate if needed.
func tryRecovery(conn pgxconn.PgxConn, err error, req copyRequest) error {
	// we only recover from postgres errors right now
	pgErr, ok := err.(*pgconn.PgError)
	if !ok {
		errMsg := err.Error()
		log.Warn("msg", fmt.Sprintf("unexpected error while inserting to %s", req.table), "err", errMsg)
		return err
	}

	// If the error was that the table is already compressed, decompress and try again.
	if strings.Contains(pgErr.Message, "insert/update/delete not permitted") {
		decompressErr := decompressChunks(conn, req.data, req.table)
		if decompressErr != nil {
			return err
		}

		req.data.batch.ResetPosition()
		return nil
	}

	log.Warn("msg", fmt.Sprintf("unexpected postgres error while inserting to %s", req.table), "err", pgErr.Error())
	return err
}

// In the event we filling in old data and the chunk we want to INSERT into has
// already been compressed, we decompress the chunk and try again. When we do
// this we delay the recompression to give us time to insert additional data.
func decompressChunks(conn pgxconn.PgxConn, pending *pendingBuffer, table string) error {
	minTime := model.Time(pending.batch.MinSeen).Time()

	//how much faster are we at ingestion than wall-clock time?
	ingestSpeedup := 2
	//delay the next compression job proportional to the duration between now and the data time + a constant safety
	delayBy := (time.Since(minTime) / time.Duration(ingestSpeedup)) + 60*time.Minute
	maxDelayBy := time.Hour * 24
	if delayBy > maxDelayBy {
		delayBy = maxDelayBy
	}
	log.Warn("msg", fmt.Sprintf("Table %s was compressed, decompressing", table), "table", table, "min-time", minTime, "age", time.Since(minTime), "delay-job-by", delayBy)

	_, rescheduleErr := conn.Exec(context.Background(), "SELECT "+schema.Catalog+".delay_compression_job($1, $2)",
		table, time.Now().Add(delayBy))
	if rescheduleErr != nil {
		log.Error("msg", rescheduleErr, "context", "Rescheduling compression")
		return rescheduleErr
	}

	_, decompressErr := conn.Exec(context.Background(), "CALL "+schema.Catalog+".decompress_chunks_after($1, $2);", table, minTime)
	if decompressErr != nil {
		log.Error("msg", decompressErr, "context", "Decompressing chunks")
		return decompressErr
	}

	metrics.DecompressCalls.Inc()
	metrics.DecompressEarliest.WithLabelValues(table).Set(float64(minTime.UnixNano()) / 1e9)
	return nil
}

/*
Useful output for debugging batching issues
func debugInsert() {
	m := &dto.Metric{}
	dbBatchInsertDuration.Write(m)
	durs := time.Duration(*m.Histogram.SampleSum * float64(time.Second))
	cnt := *m.Histogram.SampleCount
	numRowsPerBatch.Write(m)
	rows := *m.Histogram.SampleSum
	numInsertsPerBatch.Write(m)
	inserts := *m.Histogram.SampleSum

	fmt.Println("avg:  duration/row", durs/time.Duration(rows), "rows/batch", uint64(rows)/cnt, "inserts/batch", uint64(inserts)/cnt, "rows/insert", rows/inserts)
}
*/

// Perform the actual insertion into the DB.
func doInsert(conn pgxconn.PgxConn, reqs ...copyRequest) (err error) {
	batch := conn.NewBatch()

	numRowsPerInsert := make([]int, 0, len(reqs))
	numRowsTotal := 0
	lowestEpoch := pgmodel.SeriesEpoch(math.MaxInt64)
	for r := range reqs {
		req := &reqs[r]
		numRows := req.data.batch.CountSamples()

		// flatten the various series into arrays.
		// there are four main bottlenecks for insertion:
		//   1. The round trip time.
		//   2. The number of requests sent.
		//   3. The number of individual INSERT statements.
		//   4. The amount of data sent.
		// While the first two of these can be handled by batching, for the latter
		// two we need to actually reduce the work done. It turns out that simply
		// collecting all the data into a postgres array and performing a single
		// INSERT using that overcomes most of the performance issues for sending
		// multiple data, and brings INSERT nearly on par with CopyFrom. In the
		// future we may wish to send compressed data instead.
		times := make([]time.Time, 0, numRows)
		vals := make([]float64, 0, numRows)
		series := make([]int64, 0, numRows)
		for req.data.batch.Next() {
			timestamp, val, seriesID, seriesEpoch := req.data.batch.Values()
			if seriesEpoch < lowestEpoch {
				lowestEpoch = seriesEpoch
			}
			times = append(times, timestamp)
			vals = append(vals, val)
			series = append(series, int64(seriesID))
		}
		if err = req.data.batch.Err(); err != nil {
			return err
		}
		if len(times) != numRows {
			panic("invalid insert request")
		}
		numRowsTotal += numRows
		numRowsPerInsert = append(numRowsPerInsert, numRows)
		queryString := fmt.Sprintf("INSERT INTO %s(time, value, series_id) SELECT * FROM unnest($1::TIMESTAMPTZ[], $2::DOUBLE PRECISION[], $3::BIGINT[]) a(t,v,s) ORDER BY s,t ON CONFLICT DO NOTHING", pgx.Identifier{schema.Data, req.table}.Sanitize())
		batch.Queue(queryString, times, vals, series)
	}

	//note the epoch increment takes an access exclusive on the table before incrementing.
	//thus we don't need row locking here. Note by doing this check at the end we can
	//have some wasted work for the inserts before this fails but this is rare.
	//avoiding an additional loop or memoization to find the lowest epoch ahead of time seems worth it.
	epochCheck := fmt.Sprintf("SELECT CASE current_epoch > $1::BIGINT + 1 WHEN true THEN %s.epoch_abort($1) END FROM %s.ids_epoch LIMIT 1", schema.Catalog, schema.Catalog)
	batch.Queue(epochCheck, int64(lowestEpoch))

	metrics.NumRowsPerBatch.Observe(float64(numRowsTotal))
	metrics.NumInsertsPerBatch.Observe(float64(len(reqs)))
	start := time.Now()
	results, err := conn.SendBatch(context.Background(), batch)
	if err != nil {
		return err
	}
	defer results.Close()

	var affectedMetrics uint64
	for _, numRows := range numRowsPerInsert {
		ct, err := results.Exec()
		if err != nil {
			return err
		}
		if int64(numRows) != ct.RowsAffected() {
			affectedMetrics++
			registerDuplicates(int64(numRows) - ct.RowsAffected())
		}
	}

	var val []byte
	row := results.QueryRow()
	err = row.Scan(&val)
	if err != nil {
		return err
	}
	reportDuplicates(affectedMetrics)
	metrics.DbBatchInsertDuration.Observe(time.Since(start).Seconds())
	return nil
}
