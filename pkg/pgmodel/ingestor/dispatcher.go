// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/metrics"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	tput "github.com/timescale/promscale/pkg/util/throughput"
)

const (
	MetricBatcherChannelCap = 1000
	finalizeMetricCreation  = "CALL " + schema.Catalog + ".finalize_metric_creation()"
	getEpochSQL             = "SELECT current_epoch FROM " + schema.Catalog + ".ids_epoch LIMIT 1"
)

// pgxDispatcher redirects incoming samples to the appropriate metricBatcher
// corresponding to the metric in the sample.
type pgxDispatcher struct {
	conn                   pgxconn.PgxConn
	metricTableNames       cache.MetricCache
	scache                 cache.SeriesCache
	batchers               sync.Map
	completeMetricCreation chan struct{}
	asyncAcks              bool
	copierReadRequestCh    chan<- readRequest
	seriesEpochRefresh     *time.Ticker
	doneChannel            chan struct{}
	doneWG                 sync.WaitGroup
	labelArrayOID          uint32
}

func newPgxDispatcher(conn pgxconn.PgxConn, cache cache.MetricCache, scache cache.SeriesCache, cfg *Cfg) (*pgxDispatcher, error) {
	numCopiers := cfg.NumCopiers
	if numCopiers < 1 {
		log.Warn("msg", "num copiers less than 1, setting to 1")
		numCopiers = 1
	}

	//the copier read request channel keep the queue order
	//between metrucs
	maxMetrics := 10000
	copierReadRequestCh := make(chan readRequest, maxMetrics)
	setCopierChannelToMonitor(copierReadRequestCh)

	if cfg.IgnoreCompressedChunks {
		// Handle decompression to not decompress anything.
		handleDecompression = skipDecompression
	}

	var labelArrayOID uint32
	err := conn.QueryRow(context.Background(), `SELECT '`+schema.Prom+`.label_array'::regtype::oid`).Scan(&labelArrayOID)
	if err != nil {
		return nil, err
	}

	sw := NewSeriesWriter(conn, labelArrayOID)

	for i := 0; i < numCopiers; i++ {
		go runCopier(conn, copierReadRequestCh, sw)
	}

	inserter := &pgxDispatcher{
		conn:                   conn,
		metricTableNames:       cache,
		scache:                 scache,
		completeMetricCreation: make(chan struct{}, 1),
		copierReadRequestCh:    copierReadRequestCh,
		// set to run at half our deletion interval
		seriesEpochRefresh: time.NewTicker(30 * time.Minute),
		doneChannel:        make(chan struct{}),
	}
	runBatchWatcher(inserter.doneChannel)

	//on startup run a completeMetricCreation to recover any potentially
	//incomplete metric
	err = inserter.CompleteMetricCreation()
	if err != nil {
		return nil, err
	}

	go inserter.runCompleteMetricCreationWorker()

	if !cfg.DisableEpochSync {
		inserter.doneWG.Add(1)
		go func() {
			defer inserter.doneWG.Done()
			inserter.runSeriesEpochSync()
		}()
	}
	return inserter, nil
}

func (p *pgxDispatcher) runCompleteMetricCreationWorker() {
	for range p.completeMetricCreation {
		err := p.CompleteMetricCreation()
		if err != nil {
			log.Warn("msg", "Got an error finalizing metric", "err", err)
		}
	}
}

func (p *pgxDispatcher) runSeriesEpochSync() {
	epoch, err := p.refreshSeriesEpoch(model.InvalidSeriesEpoch)
	// we don't have any great place to report errors, and if the
	// connection recovers we can still make progress, so we'll just log it
	// and continue execution
	if err != nil {
		log.Error("msg", "error refreshing the series cache", "err", err)
	}
	for {
		select {
		case <-p.seriesEpochRefresh.C:
			epoch, err = p.refreshSeriesEpoch(epoch)
			if err != nil {
				log.Error("msg", "error refreshing the series cache", "err", err)
			}
		case <-p.doneChannel:
			return
		}
	}
}

func (p *pgxDispatcher) refreshSeriesEpoch(existingEpoch model.SeriesEpoch) (model.SeriesEpoch, error) {
	dbEpoch, err := p.getServerEpoch()
	if err != nil {
		// Trash the cache just in case an epoch change occurred, seems safer
		p.scache.Reset()
		return model.InvalidSeriesEpoch, err
	}
	if existingEpoch == model.InvalidSeriesEpoch || dbEpoch != existingEpoch {
		p.scache.Reset()
	}
	return dbEpoch, nil
}

func (p *pgxDispatcher) getServerEpoch() (model.SeriesEpoch, error) {
	var newEpoch int64
	row := p.conn.QueryRow(context.Background(), getEpochSQL)
	err := row.Scan(&newEpoch)
	if err != nil {
		return -1, err
	}

	return model.SeriesEpoch(newEpoch), nil
}

func (p *pgxDispatcher) CompleteMetricCreation() error {
	_, err := p.conn.Exec(
		context.Background(),
		finalizeMetricCreation,
	)
	return err
}

func (p *pgxDispatcher) Close() {
	close(p.completeMetricCreation)
	p.batchers.Range(func(key, value interface{}) bool {
		close(value.(chan *insertDataRequest))
		return true
	})
	close(p.copierReadRequestCh)
	close(p.doneChannel)
	p.doneWG.Wait()
}

// Insert a batch of data into the DB.
// The data should be grouped by metric name.
// returns the number of rows we intended to insert (_not_ how many were
// actually inserted) and any error.
// Though we may insert data to multiple tables concurrently, if asyncAcks is
// unset this function will wait until _all_ the insert attempts have completed.
func (p *pgxDispatcher) InsertTs(dataTS model.Data) (uint64, error) {
	var (
		numRows      uint64
		maxt         int64
		rows         = dataTS.Rows
		workFinished = new(sync.WaitGroup)
	)
	workFinished.Add(len(rows))
	// we only allocate enough space for a single error message here as we only
	// report one error back upstream. The inserter should not block on this
	// channel, but only insert if it's empty, anything else can deadlock.
	errChan := make(chan error, 1)
	for metricName, data := range rows {
		for _, si := range data {
			numRows += uint64(si.CountSamples())
			ls := si.LastSample()
			if maxt < ls.Timestamp {
				// Since by default, samples are expected to arrive in sorted order with time,
				// the last sample should be the maxt of that series. This assumption helps avoid costly inner loops.
				maxt = ls.Timestamp
			}
		}
		// the following is usually non-blocking, just a channel insert
		p.getMetricBatcher(metricName) <- &insertDataRequest{metric: metricName, data: data, finished: workFinished, errChan: errChan}
	}
	reportIncomingBatch(numRows)
	reportOutgoing := func() {
		reportOutgoingBatch(numRows)
		reportBatchProcessingTime(dataTS.ReceivedTime)
	}

	var err error
	if !p.asyncAcks {
		workFinished.Wait()
		reportOutgoing()
		select {
		case err = <-errChan:
		default:
		}
		postIngestTasks(maxt, numRows, 0)
		close(errChan)
	} else {
		go func() {
			workFinished.Wait()
			reportOutgoing()
			select {
			case err = <-errChan:
			default:
			}
			close(errChan)
			if err != nil {
				log.Error("msg", fmt.Sprintf("error on async send, dropping %d datapoints", numRows), "err", err)
			}
			postIngestTasks(maxt, numRows, 0)
		}()
	}

	return numRows, err
}

func (p *pgxDispatcher) InsertMetadata(metadata []model.Metadata) (uint64, error) {
	totalRows := uint64(len(metadata))
	insertedRows, err := insertMetadata(p.conn, metadata)
	if err != nil {
		return insertedRows, err
	}
	postIngestTasks(0, 0, insertedRows)
	if totalRows != insertedRows {
		return insertedRows, fmt.Errorf("failed to insert all metadata: inserted %d rows out of %d rows in total", insertedRows, totalRows)
	}
	return insertedRows, nil
}

// postIngestTasks performs a set of tasks that are due after ingesting series data.
func postIngestTasks(maxTs int64, numSamples, numMetadata uint64) {
	tput.ReportDataProcessed(maxTs, numSamples, numMetadata)

	// Max_sent_timestamp stats.
	if maxTs < atomic.LoadInt64(&MaxSentTimestamp) {
		return
	}
	atomic.StoreInt64(&MaxSentTimestamp, maxTs)
	metrics.MaxSentTimestamp.Set(float64(maxTs))
}

// Get the handler for a given metric name, creating a new one if none exists
func (p *pgxDispatcher) getMetricBatcher(metric string) chan<- *insertDataRequest {
	batcher, ok := p.batchers.Load(metric)
	if !ok {
		// The ordering is important here: we need to ensure that every call
		// to getMetricInserter() returns the same inserter. Therefore, we can
		// only start up the inserter routine if we know that we won the race
		// to create the inserter, anything else will leave a zombie inserter
		// lying around.
		c := make(chan *insertDataRequest, MetricBatcherChannelCap)
		actual, old := p.batchers.LoadOrStore(metric, c)
		batcher = actual
		if !old {
			go runMetricBatcher(p.conn, c, metric, p.completeMetricCreation, p.metricTableNames, p.copierReadRequestCh, p.labelArrayOID)
		}
	}
	ch := batcher.(chan *insertDataRequest)
	MetricBatcherChLen.Observe(float64(len(ch)))
	return ch
}

type insertDataRequest struct {
	metric   string
	data     []model.Samples
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
