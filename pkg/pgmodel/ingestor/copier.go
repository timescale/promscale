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
	"github.com/prometheus/common/model"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/metrics"
	pgmodel "github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const maxInsertStmtPerTxn = 100

type copyRequest struct {
	data          *pendingBuffer
	tableCallback func(string) (string, error)
}

type insertRequest struct {
	data  *pgmodel.SamplesBatch
	table string
}

var (
	getBatchMutex       = &sync.Mutex{}
	handleDecompression = retryAfterDecompression
)

type copyBatch []*insertRequest

func (c copyBatch) VisitSeries(cb func(s *pgmodel.Series) error) error {
	for _, req := range c {
		samples := req.data.GetSeriesSamples()
		for _, sample := range samples {
			err := cb(sample.GetSeries())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c copyBatch) NumSeries() int {
	i := 0
	for _, req := range c {
		i += len(req.data.GetSeriesSamples())
	}
	return i
}

// Handles actual insertion into the DB.
// We have one of these per connection reserved for insertion.
func runCopier(conn pgxconn.PgxConn, in chan readRequest, sw *seriesWriter) {
	insertBatch := make([]*insertRequest, 0, 1000)
loop:
	for {
		var ok bool

		request, ok := <-in
		if !ok {
			return
		}

		copyRequest, ok := <-request.copySender
		if !ok {
			panic("here should never happen")
			continue
		}

		for metricName, sb := range copyRequest.data.batch {
			tableName, err := copyRequest.tableCallback(metricName)
			if err != nil {
				copyRequest.data.reportResults(err)
				copyRequest.data.release()
				goto loop
			}
			insertBatch = append(insertBatch, &insertRequest{data: sb, table: tableName})
		}

		// sort to prevent deadlocks on table locks
		sort.Slice(insertBatch, func(i, j int) bool {
			return insertBatch[i].table < insertBatch[j].table
		})

		err := sw.WriteSeries(copyBatch(insertBatch))
		if err != nil {
			fmt.Println(err)
		}

		batchCt := 100
		err = nil
		for i := 0; i < len(insertBatch); i += batchCt {
			high := i + batchCt
			if len(insertBatch) < high {
				high = len(insertBatch)
			}
			batch := insertBatch[i:high]
			err = doInsertOrFallback(conn, copyRequest, batch...)
			if err != nil {
				break
			}
		}
		copyRequest.data.reportResults(err)
		copyRequest.data.release()

		for i := range insertBatch {
			insertBatch[i] = &insertRequest{}
		}
		insertBatch = insertBatch[:0]
	}
}

func copierGetBatch(batch []readRequest, in <-chan readRequest) ([]readRequest, bool) {
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

func doInsertOrFallback(conn pgxconn.PgxConn, copyRequest copyRequest, reqs ...*insertRequest) error {
	err := insertSeries(conn, reqs...)
	if err != nil {
		return insertBatchErrorFallback(conn, copyRequest, reqs...)
	}
	return nil
}

func insertBatchErrorFallback(conn pgxconn.PgxConn, copyRequest copyRequest, reqs ...*insertRequest) error {
	var oneErr error = nil
	for i := range reqs {
		reqs[i].data.ResetPosition()
		err := insertSeries(conn, reqs[i])
		if err != nil {
			err = tryRecovery(conn, err, reqs[i])
		}
		if err != nil {
			oneErr = err
		}

	}
	//FIXME
	return oneErr
}

// we can currently recover from one error:
// If we inserted into a compressed chunk, we decompress the chunk and try again.
// Since a single batch can have both errors, we need to remember the insert method
// we're using, so that we deduplicate if needed.
func tryRecovery(conn pgxconn.PgxConn, err error, req *insertRequest) error {
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

func skipDecompression(_ pgxconn.PgxConn, _ *insertRequest) error {
	log.WarnRateLimited("msg", "Rejecting samples falling on compressed chunks as decompression is disabled")
	return nil
}

// In the event we filling in old data and the chunk we want to INSERT into has
// already been compressed, we decompress the chunk and try again. When we do
// this we delay the recompression to give us time to insert additional data.
func retryAfterDecompression(conn pgxconn.PgxConn, req *insertRequest) error {
	var (
		table   = req.table
		pending = req.data
		minTime = model.Time(pending.MinSeen).Time()
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

	req.data.ResetPosition()
	return insertSeries(conn, req) // Attempt an insert again.
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

// insertSeries performs the insertion of time-series into the DB.
func insertSeries(conn pgxconn.PgxConn, reqs ...*insertRequest) (err error) {
	batch := conn.NewBatch()

	numRowsPerInsert := make([]int, 0, len(reqs))
	numRowsTotal := 0
	lowestEpoch := pgmodel.SeriesEpoch(math.MaxInt64)
	for r := range reqs {
		req := reqs[r]
		numRows := req.data.CountSamples()
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
		for req.data.Next() {
			timestamp, val, seriesID, seriesEpoch := req.data.Values()
			if seriesEpoch < lowestEpoch {
				lowestEpoch = seriesEpoch
			}
			times = append(times, timestamp)
			vals = append(vals, val)
			series = append(series, int64(seriesID))
		}
		if err = req.data.Err(); err != nil {
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

func insertMetadata(conn pgxconn.PgxConn, reqs []pgmodel.Metadata) (insertedRows uint64, err error) {
	numRows := len(reqs)
	timeSlice := make([]time.Time, numRows)
	metricFamilies := make([]string, numRows)
	units := make([]string, numRows)
	types := make([]string, numRows)
	helps := make([]string, numRows)
	n := time.Now()
	for i := range reqs {
		timeSlice[i] = n
		metricFamilies[i] = reqs[i].MetricFamily
		units[i] = reqs[i].Unit
		types[i] = reqs[i].Type
		helps[i] = reqs[i].Help
	}
	start := time.Now()
	row := conn.QueryRow(context.Background(), "SELECT "+schema.Catalog+".insert_metric_metadatas($1::TIMESTAMPTZ[], $2::TEXT[], $3::TEXT[], $4::TEXT[], $5::TEXT[])",
		timeSlice, metricFamilies, types, units, helps)
	if err := row.Scan(&insertedRows); err != nil {
		return 0, fmt.Errorf("send metadata batch: %w", err)
	}
	MetadataBatchInsertDuration.Observe(time.Since(start).Seconds())
	return insertedRows, nil
}
