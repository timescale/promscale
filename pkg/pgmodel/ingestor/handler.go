// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"context"
	"fmt"
	"sort"

	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
)

type insertHandler struct {
	conn            pgxconn.PgxConn
	input           chan *insertDataRequest
	pending         *pendingBuffer
	metricTableName string
	toCopiers       chan copyRequest
}

func (h *insertHandler) blockingHandleReq() bool {
	req, ok := <-h.input
	if !ok {
		return false
	}

	h.handleReq(req)

	return true
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

func (h *insertHandler) handleReq(req *insertDataRequest) bool {
	h.pending.addReq(req)
	if h.pending.IsFull() {
		h.flushPending()
		return true
	}
	return false
}

func (h *insertHandler) flush() {
	if h.pending.IsEmpty() {
		return
	}
	h.flushPending()
}

// Set all unset SeriesIds and flush to the next layer
func (h *insertHandler) flushPending() {
	err := h.setSeriesIds(h.pending.batch.GetSeriesSamples())
	if err != nil {
		h.pending.reportResults(err)
		h.pending.release()
		h.pending = NewPendingBuffer()
		return
	}

	h.toCopiers <- copyRequest{h.pending, h.metricTableName}
	h.pending = NewPendingBuffer()
}

// Set all seriesIds for a samplesInfo, fetching any missing ones from the DB,
// and repopulating the cache accordingly.
// returns: the tableName for the metric being inserted into
// TODO move up to the rest of insertHandler
func (h *insertHandler) setSeriesIds(seriesSamples []model.Samples) error {
	seriesToInsert := make([]model.Samples, 0, len(seriesSamples))
	for i, series := range seriesSamples {
		if !series.GetSeries().IsSeriesIDSet() {
			seriesToInsert = append(seriesToInsert, seriesSamples[i])
		}
	}
	if len(seriesToInsert) == 0 {
		return nil
	}

	var lastSeenLabel *model.Series
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
		return seriesToInsert[i].GetSeries().Compare(seriesToInsert[j].GetSeries()) < 0
	})

	batchSeries := make([][]model.Samples, 0, len(seriesToInsert))
	// group the seriesToInsert by labels, one slice array per unique labels
	for _, curr := range seriesToInsert {
		names, values, ok := curr.GetSeries().NameValues()
		if !ok {
			//was already set
			continue
		}
		if lastSeenLabel != nil && lastSeenLabel.Equal(curr.GetSeries()) {
			batchSeries[len(batchSeries)-1] = append(batchSeries[len(batchSeries)-1], curr)
			continue
		}

		batch.Queue("BEGIN;")
		batch.Queue(getSeriesIDForLabelSQL, curr.GetSeries().MetricName(), names, values)
		batch.Queue("COMMIT;")
		numSQLFunctionCalls++
		batchSeries = append(batchSeries, []model.Samples{curr})

		lastSeenLabel = curr.GetSeries()
	}

	if numSQLFunctionCalls != len(batchSeries) {
		return fmt.Errorf("Error setting series ids: Unexpected difference in numQueries and batchSeries")
	}

	br, err := h.conn.SendBatch(context.Background(), batch)
	if err != nil {
		return fmt.Errorf("Error setting series ids: %w", err)
	}
	defer br.Close()

	// BEGIN;
	_, err = br.Exec()
	if err != nil {
		return fmt.Errorf("Error setting series ids: %w", err)
	}

	var dbEpoch model.SeriesEpoch
	row := br.QueryRow()
	err = row.Scan(&dbEpoch)
	if err != nil {
		return fmt.Errorf("Error setting series ids: %w", err)
	}

	// COMMIT;
	_, err = br.Exec()
	if err != nil {
		return fmt.Errorf("Error setting series ids: %w", err)
	}

	var tableName string
	for i := 0; i < numSQLFunctionCalls; i++ {
		// BEGIN;
		_, err = br.Exec()
		if err != nil {
			return fmt.Errorf("Error setting series ids: %w", err)
		}

		var id model.SeriesID
		row = br.QueryRow()
		err = row.Scan(&tableName, &id)
		if err != nil {
			return fmt.Errorf("Error setting series ids: %w", err)
		}

		for _, si := range batchSeries[i] {
			si.GetSeries().SetSeriesID(id, dbEpoch)
		}

		// COMMIT;
		_, err = br.Exec()
		if err != nil {
			return fmt.Errorf("Error setting series ids: %w", err)
		}
	}

	return nil
}
