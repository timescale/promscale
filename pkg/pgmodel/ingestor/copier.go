// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"context"
	"fmt"
	"github.com/timescale/promscale/pkg/prompb"
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

const maxCopyRequestsPerTxn = 100

type copyRequest struct {
	data  *pendingBuffer
	table string
}

var (
	getBatchMutex       = &sync.Mutex{}
	handleDecompression = retryAfterDecompression
)

type copyBatch []copyRequest

func (c copyBatch) VisitSeries(cb func(s *pgmodel.Series) error) error {
	for _, req := range c {
		samples := req.data.batch.GetSeriesSamples()
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
		i += len(req.data.batch.GetSeriesSamples())
	}
	return i
}

// Handles actual insertion into the DB.
// We have one of these per connection reserved for insertion.
func runCopier(conn pgxconn.PgxConn, in chan copyRequest, sw *seriesWriter) {
	// We grab copyRequests off the channel one at a time. This, and the name is
	// a legacy from when we used CopyFrom to perform the insertions, and may
	// change in the future.
	insertBatch := make([]copyRequest, 0, maxCopyRequestsPerTxn)
	for {
		var ok bool
		insertBatch, ok = copierGetBatch(insertBatch, in)
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

		err := sw.WriteSeries(copyBatch(insertBatch))
		if err != nil {
			for i := range insertBatch {
				insertBatch[i].data.reportResults(err)
				insertBatch[i].data.release()
			}
			return
		}

		doInsertOrFallback(conn, insertBatch...)
		for i := range insertBatch {
			insertBatch[i] = copyRequest{}
		}
		insertBatch = insertBatch[:0]
	}
}

func copierGetBatch(batch []copyRequest, in chan copyRequest) ([]copyRequest, bool) {
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
	err := insertSeries(conn, reqs...)
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
		err := insertSeries(conn, reqs[i])
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
func insertSeries(conn pgxconn.PgxConn, reqs ...copyRequest) (err error) {
	batch := conn.NewBatch()

	numRowsPerInsert := make([]int, 0, len(reqs))
	numRowsTotal := 0
	totalSamples := 0
	totalExemplars := 0
	lowestEpoch := pgmodel.SeriesEpoch(math.MaxInt64)
	for r := range reqs {
		req := &reqs[r]
		numSamples, numExemplars := req.data.batch.Count()
		NumRowsPerInsert.Observe(float64(numSamples + numExemplars))

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
		timeSamples := make([]time.Time, 0, numSamples)
		valSamples := make([]float64, 0, numSamples)
		seriesSamples := make([]int64, 0, numSamples)

		timeE := make([]time.Time, 0, numExemplars)
		valE := make([]float64, 0, numExemplars)
		seriesE := make([]int64, 0, numExemplars)
		exemplarLbls := make([][]string, 0, numExemplars)

		hasSamples := false
		hasExemplars := false
		batchIterator := req.data.batch.Iterator()

		for batchIterator.HasNext() {
			labels, timestamp, val, seriesID, seriesEpoch, typ := batchIterator.Value()
			// Note: `labels` received above are labels of exemplars since samples do not have labels.
			// Hence, in case of samples. the `labels` will actually be nil.
			if seriesEpoch < lowestEpoch {
				lowestEpoch = seriesEpoch
			}
			switch typ {
			case pgmodel.Sample:
				hasSamples = true
				timeSamples = append(timeSamples, timestamp)
				valSamples = append(valSamples, val)
				seriesSamples = append(seriesSamples, int64(seriesID))
			case pgmodel.Exemplar:
				hasExemplars = true
				timeE = append(timeE, timestamp)
				valE = append(valE, val)
				seriesE = append(seriesE, int64(seriesID))
				exemplarLbls = append(exemplarLbls, labelsToStringSlice(labels))
			}
		}
		batchIterator.Close()

		if err = req.data.batch.Err(); err != nil {
			return err
		}
		numRowsTotal += numSamples + numExemplars
		totalSamples += numSamples
		totalExemplars += numExemplars
		if hasSamples {
			numRowsPerInsert = append(numRowsPerInsert, numSamples)
			batch.Queue("SELECT "+schema.Catalog+".insert_metric_row($1::NAME, $2::TIMESTAMPTZ[], $3::DOUBLE PRECISION[], $4::BIGINT[])", req.table, timeSamples, valSamples, seriesSamples)
		}
		if hasExemplars {
			// We cannot send 2-D [][]TEXT to postgres via the pgx.encoder. For this and easier querying reasons, we create a
			// new type in postgres by the name SCHEMA_PROM.label_value_array and use that type as array (which forms a 2D array of TEXT)
			// which is then used to push using the unnest method apprach.
			labelValues := pgmodel.GetCustomType(pgmodel.LabelValueArray)
			if err := labelValues.Set(exemplarLbls); err != nil {
				return fmt.Errorf("setting prom_api.label_value_array[] value: %w", err)
			}
			numRowsPerInsert = append(numRowsPerInsert, numExemplars)
			batch.Queue("SELECT "+schema.Catalog+".insert_exemplar_row($1::NAME, $2::TIMESTAMPTZ[], $3::BIGINT[], $4::"+schema.Prom+".label_value_array[], $5::DOUBLE PRECISION[])", req.table, timeE, seriesE, labelValues, valE)
		}
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
	numSamplesInserted.Add(float64(totalSamples))
	numExemplarsInserted.Add(float64(totalExemplars))

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

func labelsToStringSlice(lbls []prompb.Label) []string {
	s := make([]string, len(lbls))
	for i := range lbls {
		s[i] = lbls[i].Value
	}
	return s
}
