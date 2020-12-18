// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
)

type insertHandler struct {
	conn               pgxconn.PgxConn
	input              chan insertDataRequest
	pending            *pendingBuffer
	seriesCache        map[string]model.SeriesID
	seriesCacheEpoch   Epoch
	seriesCacheRefresh *time.Ticker
	metricTableName    string
	toCopiers          chan copyRequest
}

func (h *insertHandler) hasPendingReqs() bool {
	return len(h.pending.batch.SampleInfos) > 0
}

func (h *insertHandler) blockingHandleReq() bool {
	for {
		select {
		case req, ok := <-h.input:
			if !ok {
				return false
			}

			h.handleReq(req)

			return true
		case <-h.seriesCacheRefresh.C:
			h.refreshSeriesCache()
		}
	}
}

func (h *insertHandler) nonblockingHandleReq() bool {
	select {
	case req := <-h.input:
		h.handleReq(req)
		return true
	default:
		return false
	}
}

func (h *insertHandler) handleReq(req insertDataRequest) bool {
	// we fill in any SeriesIds we have in cache now so we can free any Labels
	// that are no longer needed, and because the SeriesIds might get flushed.
	// (neither of these are that critical at the moment)
	_, epoch := h.fillKnownSeriesIds(req.data)
	needsFlush := h.pending.addReq(req, epoch)
	if needsFlush {
		h.flushPending()
		return true
	}
	return false
}

// Fill in any SeriesIds we already have in cache.
// This must be idempotent: if called a second time it should not affect any
// sampleInfo whose series was already set.
func (h *insertHandler) fillKnownSeriesIds(sampleInfos []model.SamplesInfo) (numMissingSeries int, epoch Epoch) {
	epoch = h.seriesCacheEpoch
	for i, series := range sampleInfos {
		// When we first create the sampleInfos we should have set the seriesID
		// to -1 for any series whose labels field is nil. Real seriesIds must
		// always be greater than 0.
		if series.SeriesID > -1 {
			continue
		}
		id, ok := h.seriesCache[series.Labels.String()]
		if ok {
			sampleInfos[i].SeriesID = id
			series.Labels = nil
		} else {
			numMissingSeries++
		}
	}
	return
}

func (h *insertHandler) flush() {
	if !h.hasPendingReqs() {
		return
	}
	h.flushPending()
}

// Set all unset SeriesIds and flush to the next layer
func (h *insertHandler) flushPending() {
	_, epoch, err := h.setSeriesIds(h.pending.batch.SampleInfos)
	if err != nil {
		h.pending.reportResults(err)
		h.pending.release()
		h.pending = pendingBuffers.Get().(*pendingBuffer)
		return
	}

	h.pending.addEpoch(epoch)

	h.toCopiers <- copyRequest{h.pending, h.metricTableName}
	h.pending = pendingBuffers.Get().(*pendingBuffer)
}

// Set all seriesIds for a samplesInfo, fetching any missing ones from the DB,
// and repopulating the cache accordingly.
// returns: the tableName for the metric being inserted into
// TODO move up to the rest of insertHandler
func (h *insertHandler) setSeriesIds(sampleInfos []model.SamplesInfo) (string, Epoch, error) {
	numMissingSeries, epoch := h.fillKnownSeriesIds(sampleInfos)

	if numMissingSeries == 0 {
		return "", epoch, nil
	}

	seriesToInsert := make([]*model.SamplesInfo, 0, numMissingSeries)
	for i, series := range sampleInfos {
		if series.SeriesID < 0 {
			seriesToInsert = append(seriesToInsert, &sampleInfos[i])
		}
	}
	var lastSeenLabel *model.Labels

	batch := h.conn.NewBatch()

	// The epoch will never decrease, so we can check it once at the beginning,
	// at worst we'll store too small an epoch, which is always safe
	batch.Queue("BEGIN;")
	batch.Queue(getEpochSQL)
	batch.Queue("COMMIT;")

	numSQLFunctionCalls := 0
	// Sort and remove duplicates. The sort is needed to remove duplicates. Each series is inserted
	// in a different transaction, thus deadlocks are not an issue.
	sort.Slice(seriesToInsert, func(i, j int) bool {
		return seriesToInsert[i].Labels.Compare(seriesToInsert[j].Labels) < 0
	})

	batchSeries := make([][]*model.SamplesInfo, 0, len(seriesToInsert))
	// group the seriesToInsert by labels, one slice array per unique labels
	for _, curr := range seriesToInsert {
		if lastSeenLabel != nil && lastSeenLabel.Equal(curr.Labels) {
			batchSeries[len(batchSeries)-1] = append(batchSeries[len(batchSeries)-1], curr)
			continue
		}

		batch.Queue("BEGIN;")
		batch.Queue(getSeriesIDForLabelSQL, curr.Labels.MetricName, curr.Labels.Names, curr.Labels.Values)
		batch.Queue("COMMIT;")
		numSQLFunctionCalls++
		batchSeries = append(batchSeries, []*model.SamplesInfo{curr})

		lastSeenLabel = curr.Labels
	}

	if numSQLFunctionCalls != len(batchSeries) {
		return "", epoch, fmt.Errorf("unexpected difference in numQueries and batchSeries")
	}

	br, err := h.conn.SendBatch(context.Background(), batch)
	if err != nil {
		return "", epoch, err
	}
	defer br.Close()

	// BEGIN;
	_, err = br.Exec()
	if err != nil {
		return "", epoch, err
	}

	var newEpoch int64
	row := br.QueryRow()
	err = row.Scan(&newEpoch)
	if err != nil {
		return "", epoch, err
	}
	if epoch == -1 || newEpoch < epoch {
		epoch = newEpoch
	}
	if h.seriesCacheEpoch == -1 || newEpoch < h.seriesCacheEpoch {
		h.seriesCacheEpoch = newEpoch
	}

	// COMMIT;
	_, err = br.Exec()
	if err != nil {
		return "", epoch, err
	}

	var tableName string
	for i := 0; i < numSQLFunctionCalls; i++ {
		// BEGIN;
		_, err = br.Exec()
		if err != nil {
			return "", epoch, err
		}

		var id model.SeriesID
		row = br.QueryRow()
		err = row.Scan(&tableName, &id)
		if err != nil {
			return "", epoch, err
		}
		seriesString := batchSeries[i][0].Labels.String()
		h.seriesCache[seriesString] = id
		for _, lsi := range batchSeries[i] {
			lsi.SeriesID = id
		}

		// COMMIT;
		_, err = br.Exec()
		if err != nil {
			return "", epoch, err
		}
	}

	return tableName, epoch, nil
}

func (h *insertHandler) refreshSeriesCache() {
	newEpoch, err := h.getServerEpoch()
	if err != nil {
		// we don't have any great place to report this error, and if the
		// connection recovers we can still make progress, so we'll just log it
		// and continue execution
		msg := fmt.Sprintf("error refreshing the series cache for %s", h.metricTableName)
		log.Error("msg", msg, "metric_table", h.metricTableName, "err", err)
		// Trash the cache just in case an epoch change occurred, seems safer
		h.seriesCache = map[string]model.SeriesID{}
		h.seriesCacheEpoch = -1
		return
	}

	if newEpoch != h.seriesCacheEpoch {
		h.seriesCache = map[string]model.SeriesID{}
		h.seriesCacheEpoch = newEpoch
	}
}

func (h *insertHandler) getServerEpoch() (Epoch, error) {
	var newEpoch int64
	row := h.conn.QueryRow(context.Background(), getEpochSQL)
	err := row.Scan(&newEpoch)
	if err != nil {
		return -1, err
	}

	return newEpoch, nil
}
