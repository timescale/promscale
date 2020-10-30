package ingestor

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/common/model"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/utils"
)

const (
	// maximum number of insertDataRequests that should be buffered before the
	// insertHandler flushes to the next layer. We don't want too many as this
	// increases the number of lost writes if the connector dies. This number
	// was chosen arbitrarily.
	flushSize = 2000
)

type insertDataRequest struct {
	metric   string
	data     []utils.SamplesInfo
	finished *sync.WaitGroup
	errChan  chan error
}

func (idr *insertDataRequest) reportResult(err error) {
	if err != nil {
		select {
		case idr.errChan <- err:
		default:
		}
	}
	idr.finished.Done()
}

type insertDataTask struct {
	finished *sync.WaitGroup
	errChan  chan error
}

// Report that this task is completed, along with any error that may have
// occured. Since this is a backedge on the goroutine graph, it
// _must never block_: blocking here will cause deadlocks.
func (idt *insertDataTask) reportResult(err error) {
	if err != nil {
		select {
		case idt.errChan <- err:
		default:
		}
	}
	idt.finished.Done()
}

type copyRequest struct {
	data  *pendingBuffer
	table string
}

func runInserterRoutine(
	conn utils.PgxConn,
	input chan insertDataRequest,
	metricName string,
	completeMetricCreationSignal chan struct{},
	metricTableNames utils.MetricCache,
	toCopiers chan copyRequest,
) {
	var tableName string
	var firstReq insertDataRequest
	firstReqSet := false
	for firstReq = range input {
		var err error
		tableName, err = initializeInserterRoutine(conn, metricName, completeMetricCreationSignal, metricTableNames)
		if err != nil {
			firstReq.reportResult(fmt.Errorf("Initializing the insert routine has failed with %w", err))
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
		conn:             conn,
		input:            input,
		pending:          pendingBuffers.Get().(*pendingBuffer),
		seriesCache:      make(map[string]utils.SeriesID),
		seriesCacheEpoch: -1,
		// set to run at half our deletion interval
		seriesCacheRefresh: time.NewTicker(30 * time.Minute),
		metricTableName:    tableName,
		toCopiers:          toCopiers,
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
		if !handler.hasPendingReqs() {
			stillAlive := handler.blockingHandleReq()
			if !stillAlive {
				return
			}
			continue
		}

	hotReceive:
		for handler.nonblockingHandleReq() {
			if len(handler.pending.batch.SampleInfos) >= flushSize {
				break hotReceive
			}
		}

		handler.flush()
	}
}

// Create the metric table for the metric we handle, if it does not already
// exist. This only does the most critical part of metric table creation, the
// rest is handled by completeMetricTableCreation().
func initializeInserterRoutine(
	conn utils.PgxConn,
	metricName string,
	completeMetricCreationSignal chan struct{},
	metricTableNames utils.MetricCache,
) (tableName string, err error) {
	tableName, err = metricTableNames.Get(metricName)
	if err == utils.ErrEntryNotFound {
		var possiblyNew bool
		tableName, possiblyNew, err = utils.GetMetricTableName(conn, metricName)
		if err != nil {
			return "", err
		}

		//ignone error since this is just an optimization
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

// Handles actual insertion into the DB.
// We have one of these per connection reserved for insertion.
func runInserter(conn utils.PgxConn, in chan copyRequest) {
	// We grab copyRequests off the channel one at a time. This, and the name is
	// a legacy from when we used CopyFrom to perform the insetions, and may
	// change in the future.
	for {
		req, ok := <-in
		if !ok {
			return
		}
		err := doInsert(conn, req)
		if err != nil {
			err = insertErrorFallback(conn, req, err)
		}

		req.data.reportResults(err)
		pendingBuffers.Put(req.data)
	}
}

// certain errors are recoverable, handle those we can
//   1. if the table is compressed, decompress and retry the insertion
func insertErrorFallback(conn utils.PgxConn, req copyRequest, err error) error {
	err = tryRecovery(conn, req, err)
	if err != nil {
		log.Warn("msg", fmt.Sprintf("time out while processing error for %s", req.table), "err", err.Error())
		return err
	}

	return doInsert(conn, req)
}

// we can currently recover from one error:
// If we inserted into a compressed chunk, we decompress the chunk and try again.
// Since a single batch can have both errors, we need to remember the insert method
// we're using, so that we deduplicate if needed.
func tryRecovery(conn utils.PgxConn, req copyRequest, err error) error {
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

// Perform the actual insertion into the DB.
func doInsert(conn utils.PgxConn, req copyRequest) (err error) {
	numRows := 0
	for i := range req.data.batch.SampleInfos {
		numRows += len(req.data.batch.SampleInfos[i].Samples)
	}
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
		time, val, serie := req.data.batch.Values()
		times = append(times, time)
		vals = append(vals, val)
		series = append(series, int64(serie))
	}
	if len(times) != numRows {
		panic("invalid insert request")
	}

	batch := conn.NewBatch()

	epochCheck := fmt.Sprintf("SELECT CASE current_epoch > $1::BIGINT + 1 WHEN true THEN %s.epoch_abort($1) END FROM %s.ids_epoch LIMIT 1", utils.CatalogSchema, utils.CatalogSchema)
	batch.Queue(epochCheck, req.data.epoch)

	dataInsert := fmt.Sprintf("INSERT INTO %s(time, value, series_id) SELECT * FROM unnest($1::TIMESTAMPTZ[], $2::DOUBLE PRECISION[], $3::BIGINT[]) a(t,v,s) ORDER BY s,t ON CONFLICT DO NOTHING", pgx.Identifier{utils.DataSchema, req.table}.Sanitize())
	batch.Queue(dataInsert, times, vals, series)

	var ct pgconn.CommandTag
	var res pgx.BatchResults
	res, err = conn.SendBatch(context.Background(), batch)
	if err != nil {
		return
	}
	defer func() { _ = res.Close() }()

	var val []byte
	row := res.QueryRow()
	err = row.Scan(&val)
	if err != nil {
		return
	}

	ct, err = res.Exec()
	if err != nil {
		return
	}

	if int64(numRows) != ct.RowsAffected() {
		log.Warn("msg", "duplicate data in sample", "table", req.table, "duplicate_count", int64(numRows)-ct.RowsAffected(), "row_count", numRows)
		utils.DuplicateSamples.Add(float64(int64(numRows) - ct.RowsAffected()))
		utils.DuplicateWrites.Inc()
	}
	return nil
}

// In the event we filling in old data and the chunk we want to INSERT into has
// already been compressed, we decompress the chunk and try again. When we do
// this we delay the recompression to give us time to insert additional data.
func decompressChunks(conn utils.PgxConn, pending *pendingBuffer, table string) error {
	minTime := model.Time(pending.batch.MinSeen).Time()

	//how much faster are we at ingestion than wall-clock time?
	ingestSpeedup := 2
	//delay the next compression job proportional to the duration between now and the data time + a constant safety
	delayBy := (time.Since(minTime) / time.Duration(ingestSpeedup)) + time.Duration(60*time.Minute)
	maxDelayBy := time.Hour * 24
	if delayBy > maxDelayBy {
		delayBy = maxDelayBy
	}
	log.Warn("msg", fmt.Sprintf("Table %s was compressed, decompressing", table), "table", table, "min-time", minTime, "age", time.Since(minTime), "delay-job-by", delayBy)

	_, rescheduleErr := conn.Exec(context.Background(), "SELECT "+utils.CatalogSchema+".delay_compression_job($1, $2)",
		table, time.Now().Add(delayBy))
	if rescheduleErr != nil {
		log.Error("msg", rescheduleErr, "context", "Rescheduling compression")
		return rescheduleErr
	}

	_, decompressErr := conn.Exec(context.Background(), "CALL "+utils.CatalogSchema+".decompress_chunks_after($1, $2);", table, minTime)
	if decompressErr != nil {
		log.Error("msg", decompressErr, "context", "Decompressing chunks")
		return decompressErr
	}

	utils.DecompressCalls.Inc()
	utils.DecompressEarliest.WithLabelValues(table).Set(float64(minTime.UnixNano()) / 1e9)
	return nil
}

// Epoch for the ID caches, -1 means that the epoch was not set.
type Epoch = int64

var pendingBuffers = sync.Pool{
	New: func() interface{} {
		pb := new(pendingBuffer)
		pb.needsResponse = make([]insertDataTask, 0)
		pb.batch = utils.NewSampleInfoIterator()
		pb.epoch = -1
		return pb
	},
}

type pendingBuffer struct {
	needsResponse []insertDataTask
	batch         utils.SampleInfoIterator
	epoch         Epoch
}

// Report completion of an insert batch to all goroutines that may be waiting
// on it, along with any error that may have occurred.
// This function also resets the pending in preperation for the next batch.
func (pending *pendingBuffer) reportResults(err error) {
	for i := 0; i < len(pending.needsResponse); i++ {
		pending.needsResponse[i].reportResult(err)
		pending.needsResponse[i] = insertDataTask{}
	}
	pending.needsResponse = pending.needsResponse[:0]

	for i := 0; i < len(pending.batch.SampleInfos); i++ {
		// nil all pointers to prevent memory leaks
		pending.batch.SampleInfos[i] = utils.SamplesInfo{}
	}
	pending.batch = utils.SampleInfoIterator{SampleInfos: pending.batch.SampleInfos[:0]}
	pending.batch.ResetPosition()
	pending.epoch = -1
}

func (pending *pendingBuffer) addReq(req insertDataRequest, epoch Epoch) bool {
	pending.addEpoch(epoch)
	pending.needsResponse = append(pending.needsResponse, insertDataTask{finished: req.finished, errChan: req.errChan})
	pending.batch.SampleInfos = append(pending.batch.SampleInfos, req.data...)
	return len(pending.batch.SampleInfos) > flushSize
}

func (pending *pendingBuffer) addEpoch(epoch Epoch) {
	if pending.epoch == -1 || epoch < pending.epoch {
		pending.epoch = epoch
	}
}
