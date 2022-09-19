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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/metrics"
	pgmodel "github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/tracer"
)

const (
	sqlInsertIntoFrom = "INSERT INTO %[1]s.%[2]s(%[3]s) SELECT %[3]s FROM %[4]s ON CONFLICT DO NOTHING"
)

type copyRequest struct {
	data *pendingBuffer
	info *pgmodel.MetricInfo
}

var (
	getBatchMutex       = &sync.Mutex{}
	handleDecompression = retryAfterDecompression
)

type copyBatch []copyRequest

func (reqs copyBatch) VisitSeries(callBack func(info *pgmodel.MetricInfo, s *pgmodel.Series) error) error {
	for _, req := range reqs {
		insertables := req.data.batch.Data()
		for i := range insertables {
			if err := callBack(req.info, insertables[i].Series()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (reqs copyBatch) VisitExemplar(callBack func(info *pgmodel.MetricInfo, s *pgmodel.PromExemplars) error) error {
	for _, req := range reqs {
		insertables := req.data.batch.Data()
		for i := range insertables {
			exemplar, ok := insertables[i].(*pgmodel.PromExemplars)
			if ok {
				if err := callBack(req.info, exemplar); err != nil {
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
	requestBatch := make([]readRequest, 0, metrics.MaxInsertStmtPerTxn)
	insertBatch := make([]copyRequest, 0, cap(requestBatch))
	for {
		ctx, span := tracer.Default().Start(context.Background(), "copier-run")
		var (
			ok        bool
			numSeries int
		)

		// fetch a batch of read requests upfront to make sure that all the
		// requests fetched are for unique metrics. This is ensured by the fact
		// that the batcher only has one outstanding readRequest at a time and
		// the fact that we fetch the entire batch before executing any of the
		// reads. This guarantees that we never need to batch the same metrics
		// together in the copier.
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
			return insertBatch[i].info.TableName < insertBatch[j].info.TableName
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
	err := sw.PopulateOrCreateSeries(ctx, batch)

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
	err, _ := insertSeries(ctx, conn, false, reqs...)
	if err != nil {
		if isPGUniqueViolation(err) {
			err, _ = insertSeries(ctx, conn, true, reqs...)
		}
		if err != nil {
			log.Error("msg", err)
			insertBatchErrorFallback(ctx, conn, reqs...)
			return
		}
	}

	for i := range reqs {
		reqs[i].data.reportResults(nil)
		reqs[i].data.release()
	}
}

// check if we got error for duplicates
func isPGUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	pgErr, ok := err.(*pgconn.PgError)
	if ok && pgErr.Code == "23505" {
		return true
	}
	return false
}

func insertBatchErrorFallback(ctx context.Context, conn pgxconn.PgxConn, reqs ...copyRequest) {
	ctx, span := tracer.Default().Start(ctx, "insert-batch-error-fallback")
	defer span.End()
	for i := range reqs {
		err, minTime := insertSeries(ctx, conn, true, reqs[i])
		if err != nil {
			err = tryRecovery(ctx, conn, err, reqs[i], minTime)
		}

		reqs[i].data.reportResults(err)
		reqs[i].data.release()
	}
}

// we can currently recover from one error:
// If we inserted into a compressed chunk, we decompress the chunk and try again.
// Since a single batch can have both errors, we need to remember the insert method
// we're using, so that we deduplicate if needed.
func tryRecovery(ctx context.Context, conn pgxconn.PgxConn, err error, req copyRequest, minTime int64) error {
	ctx, span := tracer.Default().Start(ctx, "try-recovery")
	defer span.End()
	// we only recover from postgres errors right now
	pgErr, ok := err.(*pgconn.PgError)
	if !ok {
		errMsg := err.Error()
		log.Warn("msg", fmt.Sprintf("unexpected error while inserting to %s", req.info.TableName), "err", errMsg)
		return err
	}

	if pgErr.Code == "0A000" || strings.Contains(pgErr.Message, "compressed") || strings.Contains(pgErr.Message, "insert/update/delete not permitted") {
		// If the error was that the table is already compressed, decompress and try again.
		return handleDecompression(ctx, conn, req, minTime)
	}

	log.Warn("msg", fmt.Sprintf("unexpected postgres error while inserting to %s", req.info.TableName), "err", pgErr.Error())
	return pgErr
}

func skipDecompression(_ context.Context, _ pgxconn.PgxConn, _ copyRequest, _ int64) error {
	log.WarnRateLimited("msg", "Rejecting samples falling on compressed chunks as decompression is disabled")
	return nil
}

// In the event we filling in old data and the chunk we want to INSERT into has
// already been compressed, we decompress the chunk and try again. When we do
// this we delay the recompression to give us time to insert additional data.
func retryAfterDecompression(ctx context.Context, conn pgxconn.PgxConn, req copyRequest, minTimeInt int64) error {
	ctx, span := tracer.Default().Start(ctx, "retry-after-decompression")
	defer span.End()
	var (
		table   = req.info.TableName
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

	_, rescheduleErr := conn.Exec(context.Background(), "SELECT _prom_catalog.delay_compression_job($1, $2)",
		table, time.Now().Add(delayBy))
	if rescheduleErr != nil {
		log.Error("msg", rescheduleErr, "context", "Rescheduling compression")
		return rescheduleErr
	}

	_, decompressErr := conn.Exec(context.Background(), "CALL _prom_catalog.decompress_chunks_after($1, $2);", table, minTime)
	if decompressErr != nil {
		log.Error("msg", decompressErr, "context", "Decompressing chunks")
		return decompressErr
	}

	metrics.IngestorDecompressCalls.With(prometheus.Labels{"type": "metric", "kind": "sample"}).Inc()
	metrics.IngestorDecompressEarliest.With(prometheus.Labels{"type": "metric", "kind": "sample", "table": table}).Set(float64(minTime.UnixNano()) / 1e9)
	err, _ := insertSeries(ctx, conn, false, req) // Attempt an insert again.
	if isPGUniqueViolation(err) {
		err, _ = insertSeries(ctx, conn, true, req) // And again :)
	}
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

var labelsCopier = prometheus.Labels{"type": "metric", "subsystem": "copier"}

// insertSeries performs the insertion of time-series into the DB.
func insertSeries(ctx context.Context, conn pgxconn.PgxConn, onConflict bool, reqs ...copyRequest) (error, int64) {
	_, span := tracer.Default().Start(ctx, "insert-series")
	defer span.End()
	numRowsPerInsert := make([]int, 0, len(reqs))
	insertedRows := make([]int, 0, len(reqs))
	numRowsTotal := 0
	totalSamples := 0
	totalExemplars := 0
	var sampleRows [][]interface{}
	var exemplarRows [][]interface{}
	insertStart := time.Now()
	lowestMinTime := int64(math.MaxInt64)
	minCacheEpoch := pgmodel.SeriesEpoch(math.MaxInt64)
	tx, err := conn.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction for inserting metrics: %v", err), lowestMinTime
	}
	defer func() {
		if tx != nil {
			if err := tx.Rollback(ctx); err != nil && err != pgx.ErrTxClosed {
				log.Error("rollback err", err)
			}
		}
	}()

	for r := range reqs {
		req := &reqs[r]
		epoch := req.data.batch.SeriesCacheEpoch()
		if minCacheEpoch.After(epoch) {
			minCacheEpoch = epoch
		}
		// Since seriesId order is not guaranteed we need to sort it to avoid row deadlock when duplicates are sent (eg. Prometheus retry)
		// Samples inside series should be sorted by Prometheus
		// We sort after PopulateOrCreateSeries call because we now have guarantees that all seriesIDs have been populated
		sort.Sort(&req.data.batch)
		numSamples, numExemplars := req.data.batch.Count()
		metrics.IngestorRowsPerInsert.With(labelsCopier).Observe(float64(numSamples + numExemplars))

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
		)

		if numSamples > 0 {
			sampleRows = make([][]interface{}, 0, numSamples)
		}
		if numExemplars > 0 {
			exemplarRows = make([][]interface{}, 0, numExemplars)
		}

		visitor := req.data.batch.Visitor()
		err = visitor.Visit(
			func(t time.Time, v float64, seriesId int64) {
				hasSamples = true
				sampleRows = append(sampleRows, []interface{}{t, v, seriesId})
			},
			func(t time.Time, v float64, seriesId int64, lvalues []string) {
				hasExemplars = true
				exemplarRows = append(exemplarRows, []interface{}{t, seriesId, lvalues, v})
			},
		)
		if err != nil {
			return err, lowestMinTime
		}
		minTime := visitor.MinTime()
		if minTime < lowestMinTime {
			lowestMinTime = minTime
		}

		numRowsTotal += numSamples + numExemplars
		totalSamples += numSamples
		totalExemplars += numExemplars

		copyFromFunc := func(tableName, schemaName string, isExemplar bool) error {
			columns := schema.PromDataColumns
			tempTablePrefix := fmt.Sprintf("s%d_", req.info.MetricID)
			rows := sampleRows
			if isExemplar {
				columns = schema.PromExemplarColumns
				tempTablePrefix = fmt.Sprintf("e%d_", req.info.MetricID)
				rows = exemplarRows
			}
			table := pgx.Identifier{schemaName, tableName}
			if onConflict {
				// we append table prefix to make sure that temp table name is unique
				table, err = createTempIngestTable(ctx, tx, tableName, schemaName, tempTablePrefix)
				if err != nil {
					return err
				}
			}
			inserted, err := tx.CopyFrom(ctx, table, columns, pgx.CopyFromRows(rows))
			if err != nil {
				return err
			}
			if onConflict {
				res, err := tx.Exec(ctx,
					fmt.Sprintf(sqlInsertIntoFrom, schemaName, pgx.Identifier{tableName}.Sanitize(),
						strings.Join(columns[:], ","), table.Sanitize()))
				if err != nil {
					return err
				}
				inserted = res.RowsAffected()

			}
			insertedRows = append(insertedRows, int(inserted))
			return nil
		}

		if hasSamples {
			numRowsPerInsert = append(numRowsPerInsert, numSamples)
			if err = copyFromFunc(req.info.TableName, req.info.TableSchema, false); err != nil {
				return err, lowestMinTime
			}
		}
		if hasExemplars {
			numRowsPerInsert = append(numRowsPerInsert, numExemplars)
			if err = copyFromFunc(req.info.TableName, schema.PromDataExemplar, true); err != nil {
				return err, lowestMinTime
			}
		}
	}

	// Note: The epoch increment takes an access exclusive on the table before
	// incrementing, thus we don't need row locking here. By doing this check
	// at the end we can have some wasted work for the inserts before this
	// fails but this is rare. Avoiding an additional loop or memoization to
	// find the lowest epoch ahead of time seems worth it.
	row := tx.QueryRow(ctx, "SELECT CASE $1 <= delete_epoch WHEN true THEN _prom_catalog.epoch_abort($1) END FROM _prom_catalog.ids_epoch LIMIT 1", minCacheEpoch)
	var val []byte
	if err = row.Scan(&val); err != nil {
		return err, lowestMinTime
	}

	if err = tx.Commit(ctx); err != nil {
		return err, lowestMinTime
	}
	metrics.IngestorRowsPerBatch.With(labelsCopier).Observe(float64(numRowsTotal))
	metrics.IngestorInsertsPerBatch.With(labelsCopier).Observe(float64(len(reqs)))

	var affectedMetrics uint64
	for idx, numRows := range numRowsPerInsert {
		if numRows != insertedRows[idx] {
			affectedMetrics++
			registerDuplicates(int64(numRows - insertedRows[idx]))
		}
	}
	metrics.IngestorItems.With(prometheus.Labels{"type": "metric", "subsystem": "copier", "kind": "sample"}).Add(float64(totalSamples))
	metrics.IngestorItems.With(prometheus.Labels{"type": "metric", "subsystem": "copier", "kind": "exemplar"}).Add(float64(totalExemplars))

	reportDuplicates(affectedMetrics)
	metrics.IngestorInsertDuration.With(prometheus.Labels{"type": "metric", "subsystem": "copier", "kind": "sample"}).Observe(time.Since(insertStart).Seconds())
	return nil, lowestMinTime
}

func createTempIngestTable(ctx context.Context, tx pgx.Tx, table, schema, prefix string) (pgx.Identifier, error) {
	var tempTableNameRawString string
	row := tx.QueryRow(ctx, "SELECT _prom_catalog.create_ingest_temp_table($1, $2, $3)", table, schema, prefix)
	if err := row.Scan(&tempTableNameRawString); err != nil {
		return nil, err
	}
	return pgx.Identifier{tempTableNameRawString}, nil
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
	row := conn.QueryRow(context.Background(), "SELECT _prom_catalog.insert_metric_metadatas($1::TIMESTAMPTZ[], $2::TEXT[], $3::TEXT[], $4::TEXT[], $5::TEXT[])",
		timeSlice, metricFamilies, types, units, helps)
	if err := row.Scan(&insertedRows); err != nil {
		return 0, fmt.Errorf("send metadata batch: %w", err)
	}
	metrics.IngestorItems.With(prometheus.Labels{"type": "metric", "kind": "metadata", "subsystem": ""}).Add(float64(insertedRows))
	metrics.IngestorInsertDuration.With(prometheus.Labels{"type": "metric", "subsystem": "", "kind": "exemplar"}).Observe(time.Since(start).Seconds())
	return insertedRows, nil
}
