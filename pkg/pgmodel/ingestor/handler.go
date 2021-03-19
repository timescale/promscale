// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"context"
	"fmt"
	"github.com/jackc/pgtype"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgmodel/model/pgsafetype"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const seriesInsertSQL = "SELECT (_prom_catalog.get_or_create_series_id_for_label_array($1, l.elem)).series_id, l.nr FROM unnest($2::prom_api.label_array[]) WITH ORDINALITY l(elem, nr) ORDER BY l.elem"

type insertHandler struct {
	conn            pgxconn.PgxConn
	input           chan *insertDataRequest
	pending         *pendingBuffer
	metricTableName string
	toCopiers       chan copyRequest
	labelArrayOID   uint32
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

type labelInfo struct {
	labelID int32
	Pos     int32
}

func labelArrayTranscoder() pgtype.ValueTranscoder { return &pgtype.Int4Array{} }

// Set all seriesIds for a samplesInfo, fetching any missing ones from the DB,
// and repopulating the cache accordingly.
// returns: the tableName for the metric being inserted into
// TODO move up to the rest of insertHandler
func (h *insertHandler) setSeriesIds(seriesSamples []model.Samples) error {
	seriesToInsert := make([]*model.Series, 0, len(seriesSamples))
	for i, series := range seriesSamples {
		if !series.GetSeries().IsSeriesIDSet() {
			seriesToInsert = append(seriesToInsert, seriesSamples[i].GetSeries())
		}
	}
	if len(seriesToInsert) == 0 {
		return nil
	}

	metricName := seriesToInsert[0].MetricName()
	labelMap := make(map[labels.Label]labelInfo, len(seriesToInsert))
	labelList := model.NewLabelList(len(seriesToInsert))
	//logically should be a separate function but we want
	//to prevent labelMap from escaping, so keeping inline.
	{
		for _, series := range seriesToInsert {
			names, values, ok := series.NameValues()
			if !ok {
				//was already set
				continue
			}
			for i := range names {
				key := labels.Label{Name: names[i], Value: values[i]}
				_, ok = labelMap[key]
				if !ok {
					labelMap[key] = labelInfo{}
					labelList.Add(names[i], values[i])
				}
			}
		}
	}
	if len(labelMap) == 0 {
		return nil
	}
	safeListName := new(pgsafetype.TextArray)
	if err := safeListName.Set(labelList.Names); err != nil {
		return err
	}
	safeListValue := new(pgsafetype.TextArray)
	if err := safeListValue.Set(labelList.Values); err != nil {
		return err
	}

	//labels have to be created before series are since we need a canonical
	//ordering for label creation to avoid deadlocks. Otherwise, if we create
	//the labels for multiple series in same txn as we are creating the series,
	//the ordering of label creation can only be canonical within a series and
	//not across series.
	dbEpoch, maxPos, err := h.fillLabelIDs(metricName, safeListName, safeListValue, labelMap)
	if err != nil {
		return fmt.Errorf("Error setting series ids: %w", err)
	}

	//create the label arrays
	labelArraySet, seriesToInsert, err := createLabelArrays(seriesToInsert, labelMap, maxPos)
	if err != nil {
		return fmt.Errorf("Error setting series ids: %w", err)
	}
	if len(labelArraySet) == 0 {
		return nil
	}

	labelArrayArray := pgtype.NewArrayType("prom_api.label_array[]", h.labelArrayOID, labelArrayTranscoder)
	err = labelArrayArray.Set(labelArraySet)
	if err != nil {
		return fmt.Errorf("Error setting series id: cannot set label_array: %w", err)
	}
	res, err := h.conn.Query(context.Background(), seriesInsertSQL, metricName, labelArrayArray)
	if err != nil {
		return fmt.Errorf("Error setting series_id: cannot query for series_id: %w", err)
	}
	defer res.Close()

	count := 0
	for res.Next() {
		var (
			id         model.SeriesID
			ordinality int64
		)
		err := res.Scan(&id, &ordinality)
		if err != nil {
			return fmt.Errorf("Error setting series_id: cannot scan series_id: %w", err)
		}
		seriesToInsert[int(ordinality)-1].SetSeriesID(id, dbEpoch)
		count++
	}
	if err := res.Err(); err != nil {
		return fmt.Errorf("Error setting series_id: reading series id rows: %w", err)
	}
	if count != len(seriesToInsert) {
		//This should never happen according to the logic. This is purely defensive.
		//panic since we may have set the seriesID incorrectly above and may
		//get data corruption if we continue.
		panic(fmt.Sprintf("number series returned %d doesn't match expected series %d", count, len(seriesToInsert)))
	}
	return nil
}

func (h *insertHandler) fillLabelIDs(metricName string, names *pgsafetype.TextArray, values *pgsafetype.TextArray, labelMap map[labels.Label]labelInfo) (model.SeriesEpoch, int, error) {
	//we cannot use the label cache here because that maps label ids => name, value.
	//what we need here is name, value => id.
	//we may want a new cache for that, at a later time.

	batch := h.conn.NewBatch()
	var dbEpoch model.SeriesEpoch
	maxPos := 0

	items := len(names.Elements)
	if items != len(labelMap) {
		return dbEpoch, 0, fmt.Errorf("Error filling labels: number of items in labelList and labelMap doesn't match")
	}
	// The epoch will never decrease, so we can check it once at the beginning,
	// at worst we'll store too small an epoch, which is always safe
	batch.Queue(getEpochSQL)
	batch.Queue("SELECT * FROM "+schema.Catalog+".get_or_create_label_ids($1, $2, $3)", metricName, names, values)
	br, err := h.conn.SendBatch(context.Background(), batch)
	if err != nil {
		return dbEpoch, 0, fmt.Errorf("Error filling labels: %w", err)
	}
	defer br.Close()

	err = br.QueryRow().Scan(&dbEpoch)
	if err != nil {
		return dbEpoch, 0, fmt.Errorf("Error filling labels: %w", err)
	}
	rows, err := br.Query()
	if err != nil {
		return dbEpoch, 0, fmt.Errorf("Error filling labels: %w", err)
	}
	defer rows.Close()

	count := 0
	labelName := new(pgsafetype.Text)
	labelValue := new(pgsafetype.Text)
	for rows.Next() {
		res := labelInfo{}
		err := rows.Scan(&res.Pos, &res.labelID, &labelName.String, &labelValue.String)
		if err != nil {
			return dbEpoch, 0, fmt.Errorf("Error filling labels in scan: %w", err)
		}
		key := labels.Label{Name: labelName.Get().(string), Value: labelValue.Get().(string)}
		_, ok := labelMap[key]
		if !ok {
			return dbEpoch, 0, fmt.Errorf("Error filling labels: getting a key never sent to the db")
		}
		labelMap[key] = res
		if int(res.Pos) > maxPos {
			maxPos = int(res.Pos)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return dbEpoch, 0, fmt.Errorf("Error filling labels: error reading label id rows: %w", err)
	}
	if count != items {
		return dbEpoch, 0, fmt.Errorf("Error filling labels: not filling as many items as expected: %v vs %v", count, items)
	}
	return dbEpoch, maxPos, nil
}

func createLabelArrays(series []*model.Series, labelMap map[labels.Label]labelInfo, maxPos int) ([][]int32, []*model.Series, error) {
	labelArraySet := make([][]int32, 0, len(series))
	dest := 0
	for src := 0; src < len(series); src++ {
		names, values, ok := series[src].NameValues()
		if !ok {
			continue
		}
		lArray := make([]int32, maxPos)
		maxIndex := 0
		for i := range names {
			key := labels.Label{Name: names[i], Value: values[i]}
			res, ok := labelMap[key]
			if !ok {
				return nil, nil, fmt.Errorf("Error generating label array: missing key in map")
			}
			if res.labelID == 0 {
				return nil, nil, fmt.Errorf("Error generating label array: missing id for label %v=>%v", names[i], values[i])
			}
			//Pos is 1-indexed, slices are 0-indexed
			sliceIndex := int(res.Pos) - 1
			lArray[sliceIndex] = int32(res.labelID)
			if sliceIndex > maxIndex {
				maxIndex = sliceIndex
			}
		}
		lArray = lArray[:maxIndex+1]
		labelArraySet = append(labelArraySet, lArray)
		//this logic is needed for when continue is hit above
		if src != dest {
			series[dest] = series[src]
		}
		dest++
	}
	if len(labelArraySet) != len(series[:dest]) {
		return nil, nil, fmt.Errorf("Error generating label array: lengths not equal")
	}
	return labelArraySet, series[:dest], nil
}
