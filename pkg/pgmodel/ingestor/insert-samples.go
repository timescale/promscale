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
	"time"

	"github.com/jackc/pgconn"
	"github.com/prometheus/common/model"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/metrics"
	pgmodel "github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
)

type samplesRequest struct {
	data  *pendingBuffer
	table string
}

func handleSamplesRequest(conn pgxconn.PgxConn, batch []samplesRequest) {
	// sort to prevent deadlocks
	sort.Slice(batch, func(i, j int) bool {
		return batch[i].table < batch[j].table
	})

	//merge tables to prevent deadlocks (on row order) and for efficiency

	// invariant: there's a gap-free prefix containing data we want to insert
	//            in table order, Following this is a gap which once contained
	//            batches we compacted away, following this is the
	//            uncompacted data.
	dst := 0
	for src := 1; src < len(batch); src++ {
		if batch[dst].table == batch[src].table {
			batch[dst].data.absorb(batch[src].data)
			batch[src].data.release()
			batch[src] = samplesRequest{}
		} else {
			dst++
			if dst != src {
				batch[dst] = batch[src]
				batch[src] = samplesRequest{}
			}
		}
	}
	batch = batch[:dst+1]

	doInsertOrFallback(conn, batch...)
	for i := range batch {
		batch[i] = samplesRequest{}
	}
}

func doInsertOrFallback(conn pgxconn.PgxConn, reqs ...samplesRequest) {
	err := insertSamples(conn, reqs...)
	if err != nil {
		insertBatchErrorFallback(conn, reqs...)
		return
	}

	for i := range reqs {
		reqs[i].data.reportResults(nil)
		reqs[i].data.release()
	}
}

func insertBatchErrorFallback(conn pgxconn.PgxConn, reqs ...samplesRequest) {
	for i := range reqs {
		reqs[i].data.batch.ResetPosition()
		err := insertSamples(conn, reqs[i])
		if err != nil {
			err = insertErrorFallback(conn, err, reqs[i])
		}

		reqs[i].data.reportResults(err)
		reqs[i].data.release()
	}
}

// certain errors are recoverable, handle those we can
//   1. if the table is compressed, decompress and retry the insertion
func insertErrorFallback(conn pgxconn.PgxConn, err error, req samplesRequest) error {
	err = tryRecovery(conn, err, req)
	if err != nil {
		log.Warn("msg", fmt.Sprintf("time out while processing error for %s", req.table), "error", err.Error())
		return err
	}
	return insertSamples(conn, req)
}

// we can currently recover from one error:
// If we inserted into a compressed chunk, we decompress the chunk and try again.
// Since a single batch can have both errors, we need to remember the insert method
// we're using, so that we deduplicate if needed.
func tryRecovery(conn pgxconn.PgxConn, err error, req samplesRequest) error {
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

// insertSamples batches the insert query for inserting series samples into the db and sends to the db.
func insertSamples(conn pgxconn.PgxConn, reqs ...samplesRequest) (err error) {
	batch := conn.NewBatch()

	numRowsPerInsert := make([]int, 0, len(reqs))
	numRowsTotal := 0
	lowestEpoch := pgmodel.SeriesEpoch(math.MaxInt64)
	for i := range reqs {
		req := &reqs[i]
		numRows := req.data.batch.CountSamples()
		NumRowsPerInsert.Observe(float64(numRows))

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
		batch.Queue("SELECT "+schema.Catalog+".insert_metric_row($1, $2::TIMESTAMPTZ[], $3::DOUBLE PRECISION[], $4::BIGINT[])", req.table, times, vals, series)
	}

	//note the epoch increment takes an access exclusive on the table before incrementing.
	//thus we don't need row locking here. Note by doing this check at the end we can
	//have some wasted work for the inserts before this fails but this is rare.
	//avoiding an additional loop or memoization to find the lowest epoch ahead of time seems worth it.
	epochCheck := fmt.Sprintf("SELECT CASE current_epoch > $1::BIGINT + 1 WHEN true THEN %s.epoch_abort($1) END FROM %s.ids_epoch LIMIT 1", schema.Catalog, schema.Catalog)
	batch.Queue(epochCheck, int64(lowestEpoch))

	NumRowsPerBatch.Observe(float64(numRowsTotal))
	NumInsertsPerBatch.Observe(float64(len(reqs)))
	start := time.Now()
	results, err := conn.SendBatch(context.Background(), batch)
	if err != nil {
		return err
	}
	defer results.Close()

	var affectedMetrics uint64
	for _, numRows := range numRowsPerInsert {
		var insertedRows int64
		err := results.QueryRow().Scan(&insertedRows)
		if err != nil {
			return err
		}
		numRowsExpected := int64(numRows)
		if numRowsExpected != insertedRows {
			affectedMetrics++
			registerDuplicates(numRowsExpected - insertedRows)
		}
	}

	var val []byte
	row := results.QueryRow()
	err = row.Scan(&val)
	if err != nil {
		return err
	}
	reportDuplicates(affectedMetrics)
	DbBatchInsertDuration.Observe(time.Since(start).Seconds())
	return nil
}
