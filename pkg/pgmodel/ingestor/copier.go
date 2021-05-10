// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"sync"
	"time"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const (
	maxRequestsPerTxn = 100
	defaultWait       = time.Millisecond * 20
)

var batchMux = new(sync.Mutex)

var (
	getBatchMutex       = &sync.Mutex{}
	handleDecompression = retryAfterDecompression
)

func launchCopiers(conn pgxconn.PgxConn, numCopiers int) (chan<- samplesRequest, chan<- metadataRequest) {
	if numCopiers < 2 {
		// Set least possible copiers as 2 since we need to dedicate at least one for metadata and the other for samples.
		log.Warn("msg", "num copiers less than 2, setting to 2")
		numCopiers = 2
	}

	// We run inserters bus-style: all of them competing to grab requests off a
	// single channel. This should offer a decent compromise between batching
	// and balancing: if an inserter is awake and has little work, it'll be more
	// likely to win the race, while one that's busy or asleep won't.
	samplesCh := make(chan samplesRequest, numCopiers*maxRequestsPerTxn)
	metadataCh := make(chan metadataRequest, numCopiers*maxRequestsPerTxn)
	for i := 0; i < numCopiers; i++ {
		go runCopier(conn, samplesCh, metadataCh)
	}

	setCopierChannelToMonitor(samplesCh, metadataCh)
	return samplesCh, metadataCh
}

// Handles actual insertion into the DB.
// We have one of these per connection reserved for insertion.
func runCopier(conn pgxconn.PgxConn, samplesCh <-chan samplesRequest, metadataCh <-chan metadataRequest) {
	// We grab copyRequests off the channel one at a time. This, and the name is
	// a legacy from when we used CopyFrom to perform the insertions, and may
	// change in the future.
	var (
		samplesBatch  = make([]samplesRequest, 0, maxRequestsPerTxn)
		metadataBatch = make([]metadataRequest, 0, maxRequestsPerTxn/20) // A single metadataReq can contain metadata batch equal to number of series by a target.
	)
	for {
		batchMux.Lock()
		select {
		case req, ok := <-samplesCh:
			if !ok {
				batchMux.Unlock()
				return
			}
			samplesBatch = samplesBatch[:0]
			samplesBatch = append(samplesBatch, req)
			samplesBatch = listenSamplesBatch(samplesBatch, samplesCh)
			batchMux.Unlock()
			handleSamplesRequest(conn, samplesBatch)
		case metadataReq, ok := <-metadataCh:
			if !ok {
				batchMux.Unlock()
				return
			}
			metadataBatch = metadataBatch[:0]
			metadataBatch = append(metadataBatch, metadataReq)
			metadataBatch = listenMetadataBatch(metadataBatch, metadataCh)
			batchMux.Unlock()
			// We handle metadataReq as batched, similar to how each metric table request is batched before being batched
			// for querying. This allows us to handle high throughput for concurrent incoming metadata batches.
			handleMetadataRequest(conn, metadataBatch)
		}
	}
}

func listenSamplesBatch(batch []samplesRequest, in <-chan samplesRequest) []samplesRequest {
send_batch:
	for len(batch) < cap(batch) {
		select {
		case req, ok := <-in:
			if !ok {
				break send_batch
			}
			batch = append(batch, req)
		case <-time.After(defaultWait):
			break send_batch
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
			err = tryRecovery(conn, err, reqs[i])
		}

		reqs[i].data.reportResults(err)
		reqs[i].data.release()
	}
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

	if pgErr.Code == "0A000" || strings.Contains(pgErr.Message, "compressed") || strings.Contains(pgErr.Message, "insert/update/delete not permitted") {
		// If the error was that the table is already compressed, decompress and try again.
		return handleDecompression(conn, req)
	}

	log.Warn("msg", fmt.Sprintf("unexpected postgres error while inserting to %s", req.table), "err", pgErr.Error())
	return pgErr
}

func skipDecompression(_ pgxconn.PgxConn, _ copyRequest) error {
	log.WarnRateLimited("msg", "Rejecting samples falling on compressed chunks as decompression is disabled")
	return nil
}

// In the event we filling in old data and the chunk we want to INSERT into has
// already been compressed, we decompress the chunk and try again. When we do
// this we delay the recompression to give us time to insert additional data.
func retryAfterDecompression(conn pgxconn.PgxConn, req copyRequest) error {
	var (
		table   = req.table
		pending = req.data
		minTime = model.Time(pending.batch.MinSeen).Time()
	)
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

	req.data.batch.ResetPosition()
	return doInsert(conn, req) // Attempt an insert again.
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

func listenMetadataBatch(batch []metadataRequest, in <-chan metadataRequest) []metadataRequest {
send_batch:
	for len(batch) < cap(batch) {
		select {
		case req, ok := <-in:
			if !ok {
				break send_batch
			}
			batch = append(batch, req)
		case <-time.After(defaultWait):
			break send_batch
		}
	}
	return batch
}
