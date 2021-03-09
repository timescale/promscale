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
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
)

type pgxInserter struct {
	conn                   pgxconn.PgxConn
	metricTableNames       cache.MetricCache
	scache                 cache.SeriesCache
	inserters              sync.Map
	completeMetricCreation chan struct{}
	asyncAcks              bool
	insertedDatapoints     *int64
	toCopiers              chan copyRequest
	seriesEpochRefresh     *time.Ticker
	doneChannel            chan bool
	doneWG                 sync.WaitGroup
	labelArrayOID          uint32
}

func newPgxInserter(conn pgxconn.PgxConn, cache cache.MetricCache, scache cache.SeriesCache, cfg *Cfg) (*pgxInserter, error) {
	cmc := make(chan struct{}, 1)

	numCopiers := cfg.NumCopiers
	if numCopiers < 1 {
		log.Warn("msg", "num copiers less than 1, setting to 1")
		numCopiers = 1
	}

	// We run inserters bus-style: all of them competing to grab requests off a
	// single channel. This should offer a decent compromise between batching
	// and balancing: if an inserter is awake and has little work, it'll be more
	// likely to win the race, while one that's busy or asleep won't.
	toCopiers := make(chan copyRequest, numCopiers*maxCopyRequestsPerTxn)
	for i := 0; i < numCopiers; i++ {
		go runInserter(conn, toCopiers)
	}

	inserter := &pgxInserter{
		conn:                   conn,
		metricTableNames:       cache,
		scache:                 scache,
		completeMetricCreation: cmc,
		asyncAcks:              cfg.AsyncAcks,
		toCopiers:              toCopiers,
		// set to run at half our deletion interval
		seriesEpochRefresh: time.NewTicker(30 * time.Minute),
		doneChannel:        make(chan bool),
	}
	if cfg.AsyncAcks && cfg.ReportInterval > 0 {
		inserter.insertedDatapoints = new(int64)
		reportInterval := int64(cfg.ReportInterval)
		go func() {
			log.Info("msg", fmt.Sprintf("outputting throughput info once every %ds", reportInterval))
			tick := time.Tick(time.Duration(reportInterval) * time.Second)
			for range tick {
				inserted := atomic.SwapInt64(inserter.insertedDatapoints, 0)
				log.Info("msg", "Samples write throughput", "samples/sec", inserted/reportInterval)
			}
		}()
	}

	err := conn.QueryRow(context.Background(), `select '`+schema.Prom+`.label_array'::regtype::oid`).Scan(&inserter.labelArrayOID)
	if err != nil {
		return nil, err
	}

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

func (p *pgxInserter) runCompleteMetricCreationWorker() {
	for range p.completeMetricCreation {
		err := p.CompleteMetricCreation()
		if err != nil {
			log.Warn("msg", "Got an error finalizing metric", "err", err)
		}
	}
}

func (p *pgxInserter) runSeriesEpochSync() {
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
			log.Error("msg", "error refreshing the series cache", "err", err)
		case <-p.doneChannel:
			return
		}
	}
}

func (p *pgxInserter) refreshSeriesEpoch(existingEpoch model.SeriesEpoch) (model.SeriesEpoch, error) {
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

func (p *pgxInserter) getServerEpoch() (model.SeriesEpoch, error) {
	var newEpoch int64
	row := p.conn.QueryRow(context.Background(), getEpochSQL)
	err := row.Scan(&newEpoch)
	if err != nil {
		return -1, err
	}

	return model.SeriesEpoch(newEpoch), nil
}

func (p *pgxInserter) CompleteMetricCreation() error {
	_, err := p.conn.Exec(
		context.Background(),
		finalizeMetricCreation,
	)
	return err
}

func (p *pgxInserter) Close() {
	close(p.completeMetricCreation)
	p.inserters.Range(func(key, value interface{}) bool {
		close(value.(chan *insertDataRequest))
		return true
	})
	close(p.toCopiers)
	close(p.doneChannel)
	p.doneWG.Wait()
}

func (p *pgxInserter) InsertNewData(rows map[string][]model.Samples) (uint64, error) {
	return p.InsertData(rows)
}

// Insert a batch of data into the DB.
// The data should be grouped by metric name.
// returns the number of rows we intended to insert (_not_ how many were
// actually inserted) and any error.
// Though we may insert data to multiple tables concurrently, if asyncAcks is
// unset this function will wait until _all_ the insert attempts have completed.
func (p *pgxInserter) InsertData(rows map[string][]model.Samples) (uint64, error) {
	var numRows uint64
	workFinished := &sync.WaitGroup{}
	workFinished.Add(len(rows))
	// we only allocate enough space for a single error message here as we only
	// report one error back upstream. The inserter should not block on this
	// channel, but only insert if it's empty, anything else can deadlock.
	errChan := make(chan error, 1)
	for metricName, data := range rows {
		for _, si := range data {
			numRows += uint64(si.CountSamples())
		}
		// the following is usually non-blocking, just a channel insert
		p.getMetricInserter(metricName) <- &insertDataRequest{metric: metricName, data: data, finished: workFinished, errChan: errChan}
	}

	var err error
	if !p.asyncAcks {
		workFinished.Wait()
		select {
		case err = <-errChan:
		default:
		}
		close(errChan)
	} else {
		go func() {
			workFinished.Wait()
			select {
			case err = <-errChan:
			default:
			}
			close(errChan)
			if err != nil {
				log.Error("msg", fmt.Sprintf("error on async send, dropping %d datapoints", numRows), "err", err)
			} else if p.insertedDatapoints != nil {
				atomic.AddInt64(p.insertedDatapoints, int64(numRows))
			}
		}()
	}

	return numRows, err
}

// Get the handler for a given metric name, creating a new one if none exists
func (p *pgxInserter) getMetricInserter(metric string) chan *insertDataRequest {
	inserter, ok := p.inserters.Load(metric)
	if !ok {
		// The ordering is important here: we need to ensure that every call
		// to getMetricInserter() returns the same inserter. Therefore, we can
		// only start up the inserter routine if we know that we won the race
		// to create the inserter, anything else will leave a zombie inserter
		// lying around.
		c := make(chan *insertDataRequest, 1000)
		actual, old := p.inserters.LoadOrStore(metric, c)
		inserter = actual
		if !old {
			go runInserterRoutine(p.conn, c, metric, p.completeMetricCreation, p.metricTableNames, p.toCopiers, p.labelArrayOID)
		}
	}
	return inserter.(chan *insertDataRequest)
}

//nolint
func (p *pgxInserter) createMetricTable(metric string) (string, error) {
	res, err := p.conn.Query(
		context.Background(),
		getCreateMetricsTableSQL,
		metric,
	)

	if err != nil {
		return "", err
	}

	var tableName string
	defer res.Close()
	if !res.Next() {
		err = res.Err()
		if err != nil {
			return "", err
		}
		return "", errors.ErrMissingTableName
	}

	if err := res.Scan(&tableName); err != nil {
		return "", err
	}

	return tableName, nil
}

//nolint
func (p *pgxInserter) getMetricTableName(metric string) (string, error) {
	var err error
	var tableName string

	tableName, err = p.metricTableNames.Get(metric)

	if err == nil {
		return tableName, nil
	}

	if err != errors.ErrEntryNotFound {
		return "", err
	}

	tableName, err = p.createMetricTable(metric)

	if err != nil {
		return "", err
	}

	err = p.metricTableNames.Set(metric, tableName)

	return tableName, err
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

type insertDataTask struct {
	finished *sync.WaitGroup
	errChan  chan error
}

// Report that this task is completed, along with any error that may have
// occurred. Since this is a back-edge on the goroutine graph, it
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
