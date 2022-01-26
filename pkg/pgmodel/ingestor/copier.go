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
	"github.com/timescale/promscale/pkg/tracer"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const maxInsertStmtPerTxn = 100

type copyRequest struct {
	data  *pendingBuffer
	table string
}

var (
	getBatchMutex       = &sync.Mutex{}
	handleDecompression = retryAfterDecompression
)

type copyBatch []copyRequest

func (reqs copyBatch) VisitSeries(callBack func(s *pgmodel.Series) error) error {
	for _, req := range reqs {
		insertables := req.data.batch.Data()
		for i := range insertables {
			if err := callBack(insertables[i].Series()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (reqs copyBatch) VisitExemplar(callBack func(s *pgmodel.PromExemplars) error) error {
	for _, req := range reqs {
		insertables := req.data.batch.Data()
		for i := range insertables {
			exemplar, ok := insertables[i].(*pgmodel.PromExemplars)
			if ok {
				if err := callBack(exemplar); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Handles actual insertion into the DB.
// We have one of these per connection reserved for insertion.
func runCopier(conn pgxconn.PgxConn, in chan readRequest, sw *seriesWriter, elf *ExemplarLabelFormatter) {
	requestBatch := make([]readRequest, 0, maxInsertStmtPerTxn)
	insertBatch := make([]copyRequest, 0, cap(requestBatch))
	for {
		ctx, span := tracer.Default().Start(context.Background(), "copier-run")
		var (
			ok        bool
			numSeries int
		)

		//fetch the batch of request upfront to make sure all of the
		//requests fetched are for unique metrics. This is insured
		//by the fact that the batcher only has one outstanding readRequest
		//at a time and the fact that we fetch the entire batch before
		//executing any of the reads. This guarantees that we never
		//need to batch the same metrics together in the copier
		requestBatch, ok = copierGetBatch(ctx, requestBatch, in)
		if !ok {
			span.End()
			return
		}

		insertCtx, insertSpan := tracer.Default().Start(ctx, "insert-batch-append")
		for i := range requestBatch {
			copyRequest, ok := <-requestBatch[i].copySender
			if !ok {
				continue
			}

			_, batchSpan := tracer.Default().Start(insertCtx, "append-batch", trace.WithLinks(
				trace.Link{
					SpanContext: trace.SpanContextFromContext(copyRequest.data.spanCtx),
				},
			))
			insertBatch = append(insertBatch, copyRequest)
			numSeries = numSeries + copyRequest.data.batch.CountSeries()
			batchSpan.End()
		}
		insertSpan.End()

		span.SetAttributes(attribute.Int("num_series", numSeries))

		// sort to prevent deadlocks on table locks
		sort.Slice(insertBatch, func(i, j int) bool {
			return insertBatch[i].table < insertBatch[j].table
		})

		err := persistBatch(ctx, conn, sw, elf, insertBatch)
		if err != nil {
			for i := range insertBatch {
				insertBatch[i].data.reportResults(err)
				insertBatch[i].data.release()
			}
		}
		for i := range requestBatch {
			requestBatch[i] = readRequest{}
		}
		for i := range insertBatch {
			insertBatch[i] = copyRequest{}
		}
		insertBatch = insertBatch[:0]
		requestBatch = requestBatch[:0]
		span.End()
	}
}

func persistBatch(ctx context.Context, conn pgxconn.PgxConn, sw *seriesWriter, elf *ExemplarLabelFormatter, insertBatch []copyRequest) error {
	ctx, span := tracer.Default().Start(ctx, "persist-batch")
	defer span.End()
	batch := copyBatch(insertBatch)
	err := sw.WriteSeries(ctx, batch)
	if err != nil {
		return fmt.Errorf("copier: writing series: %w", err)
	}
	err = elf.orderExemplarLabelValues(batch)
	if err != nil {
		return fmt.Errorf("copier: formatting exemplar label values: %w", err)
	}

	doInsertOrFallback(ctx, conn, insertBatch...)
	return nil
}

func copierGetBatch(ctx context.Context, batch []readRequest, in <-chan readRequest) ([]readRequest, bool) {
	_, span := tracer.Default().Start(ctx, "get-batch")
	defer span.End()
	//This mutex is not for safety, but rather for better batching.
	//It guarantees that only one copier is reading from the channel at one time
	//This ensures bigger batches as well as less spread of a
	//single http request to multiple copiers, decreasing latency via less sync
	//overhead especially on low-pressure systems.
	span.AddEvent("Acquiring lock")
	getBatchMutex.Lock()
	span.AddEvent("Got the lock")
	defer func(span trace.Span) {
		getBatchMutex.Unlock()
		span.AddEvent("Unlocking")
	}(span)

	req, ok := <-in
	if !ok {
		return batch, false
	}
	span.AddEvent("Appending first batch")
	batch = append(batch, req)

	//we use a small timeout to prevent low-pressure systems from using up too many
	//txns and putting pressure on system
	timeout := time.After(20 * time.Millisecond)
hot_gather:
	for len(batch) < cap(batch) {
		select {
		case r2 := <-in:
			span.AddEvent("Appending batch")
			batch = append(batch, r2)
		case <-timeout:
			span.AddEvent("Timeout appending batches")
			break hot_gather
		}
	}
	if len(batch) == cap(batch) {
		span.AddEvent("Batch is full")
	}
	span.SetAttributes(attribute.Int("num_batches", len(batch)))
	return batch, true
}

func doInsertOrFallback(ctx context.Context, conn pgxconn.PgxConn, reqs ...copyRequest) {
	ctx, span := tracer.Default().Start(ctx, "do-insert-or-fallback")
	defer span.End()
	err, _ := insertSeries(ctx, conn, reqs...)
	if err != nil {
		insertBatchErrorFallback(ctx, conn, reqs...)
		return
	}

	for i := range reqs {
		reqs[i].data.reportResults(nil)
		reqs[i].data.release()
	}
}

func insertBatchErrorFallback(ctx context.Context, conn pgxconn.PgxConn, reqs ...copyRequest) {
	ctx, span := tracer.Default().Start(ctx, "insert-batch-error-fallback")
	defer span.End()
	for i := range reqs {
		err, minTime := insertSeries(ctx, conn, reqs[i])
		if err != nil {
			err = tryRecovery(conn, err, reqs[i], minTime)
		}

		reqs[i].data.reportResults(err)
		reqs[i].data.release()
	}
}

// we can currently recover from one error:
// If we inserted into a compressed chunk, we decompress the chunk and try again.
// Since a single batch can have both errors, we need to remember the insert method
// we're using, so that we deduplicate if needed.
func tryRecovery(conn pgxconn.PgxConn, err error, req copyRequest, minTime int64) error {
	// we only recover from postgres errors right now
	pgErr, ok := err.(*pgconn.PgError)
	if !ok {
		errMsg := err.Error()
		log.Warn("msg", fmt.Sprintf("unexpected error while inserting to %s", req.table), "err", errMsg)
		return err
	}

	if pgErr.Code == "0A000" || strings.Contains(pgErr.Message, "compressed") || strings.Contains(pgErr.Message, "insert/update/delete not permitted") {
		// If the error was that the table is already compressed, decompress and try again.
		return handleDecompression(conn, req, minTime)
	}

	log.Warn("msg", fmt.Sprintf("unexpected postgres error while inserting to %s", req.table), "err", pgErr.Error())
	return pgErr
}

func skipDecompression(_ pgxconn.PgxConn, _ copyRequest, _ int64) error {
	log.WarnRateLimited("msg", "Rejecting samples falling on compressed chunks as decompression is disabled")
	return nil
}

// In the event we filling in old data and the chunk we want to INSERT into has
// already been compressed, we decompress the chunk and try again. When we do
// this we delay the recompression to give us time to insert additional data.
func retryAfterDecompression(conn pgxconn.PgxConn, req copyRequest, minTimeInt int64) error {
	var (
		table   = req.table
		minTime = model.Time(minTimeInt).Time()
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
	err, _ := insertSeries(context.Background(), conn, req) // Attempt an insert again.
	return err
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
func insertSeries(ctx context.Context, conn pgxconn.PgxConn, reqs ...copyRequest) (error, int64) {
	_, span := tracer.Default().Start(ctx, "insert-series")
	defer span.End()
	batch := conn.NewBatch()

	numRowsPerInsert := make([]int, 0, len(reqs))
	numRowsTotal := 0
	totalSamples := 0
	totalExemplars := 0
	lowestEpoch := pgmodel.SeriesEpoch(math.MaxInt64)
	lowestMinTime := int64(math.MaxInt64)
	for r := range reqs {
		req := &reqs[r]
		// Since seriesId order is not guaranteed we need to sort it to avoid row deadlock when duplicates are sent (eg. Prometheus retry)
		// Samples inside series should be sorted by Prometheus
		// We sort after WriteSeries call because we now have guarantees that all seriesIDs have been populated
		sort.Sort(&req.data.batch)
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
		var (
			hasSamples   bool
			hasExemplars bool

			timeSamples   []time.Time
			timeExemplars []time.Time

			valSamples   []float64
			valExemplars []float64

			seriesIdSamples   []int64
			seriesIdExemplars []int64

			exemplarLbls [][]string
		)

		if numSamples > 0 {
			timeSamples = make([]time.Time, 0, numSamples)
			valSamples = make([]float64, 0, numSamples)
			seriesIdSamples = make([]int64, 0, numSamples)
		}
		if numExemplars > 0 {
			timeExemplars = make([]time.Time, 0, numExemplars)
			valExemplars = make([]float64, 0, numExemplars)
			seriesIdExemplars = make([]int64, 0, numExemplars)
			exemplarLbls = make([][]string, 0, numExemplars)
		}

		visitor := req.data.batch.Visitor()
		err := visitor.Visit(
			func(t time.Time, v float64, seriesId int64) {
				hasSamples = true
				timeSamples = append(timeSamples, t)
				valSamples = append(valSamples, v)
				seriesIdSamples = append(seriesIdSamples, seriesId)
			},
			func(t time.Time, v float64, seriesId int64, lvalues []string) {
				hasExemplars = true
				timeExemplars = append(timeExemplars, t)
				valExemplars = append(valExemplars, v)
				seriesIdExemplars = append(seriesIdExemplars, seriesId)
				exemplarLbls = append(exemplarLbls, lvalues)
			},
		)
		if err != nil {
			return err, lowestMinTime
		}
		epoch := visitor.LowestEpoch()
		if epoch < lowestEpoch {
			lowestEpoch = epoch
		}
		minTime := visitor.MinTime()
		if minTime < lowestMinTime {
			lowestMinTime = minTime
		}

		numRowsTotal += numSamples + numExemplars
		totalSamples += numSamples
		totalExemplars += numExemplars
		if hasSamples {
			numRowsPerInsert = append(numRowsPerInsert, numSamples)
			batch.Queue("SELECT "+schema.Catalog+".insert_metric_row($1, $2::TIMESTAMPTZ[], $3::DOUBLE PRECISION[], $4::BIGINT[])", req.table, timeSamples, valSamples, seriesIdSamples)
		}
		if hasExemplars {
			// We cannot send 2-D [][]TEXT to postgres via the pgx.encoder. For this and easier querying reasons, we create a
			// new type in postgres by the name SCHEMA_PROM.label_value_array and use that type as array (which forms a 2D array of TEXT)
			// which is then used to push using the unnest method apprach.
			labelValues := pgmodel.GetCustomType(pgmodel.LabelValueArray)
			if err := labelValues.Set(exemplarLbls); err != nil {
				return fmt.Errorf("setting prom_api.label_value_array[] value: %w", err), lowestMinTime
			}
			numRowsPerInsert = append(numRowsPerInsert, numExemplars)
			batch.Queue("SELECT "+schema.Catalog+".insert_exemplar_row($1::NAME, $2::TIMESTAMPTZ[], $3::BIGINT[], $4::"+schema.Prom+".label_value_array[], $5::DOUBLE PRECISION[])", req.table, timeExemplars, seriesIdExemplars, labelValues, valExemplars)
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
		return err, lowestMinTime
	}
	defer results.Close()

	var affectedMetrics uint64
	for _, numRows := range numRowsPerInsert {
		var insertedRows int64
		err := results.QueryRow().Scan(&insertedRows)
		if err != nil {
			return err, lowestMinTime
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
		return err, lowestMinTime
	}
	reportDuplicates(affectedMetrics)
	DbBatchInsertDuration.Observe(time.Since(start).Seconds())
	return nil, lowestMinTime
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
