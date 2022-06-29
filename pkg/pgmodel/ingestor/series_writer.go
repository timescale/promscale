// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"context"
	"fmt"

	"github.com/jackc/pgtype"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	pgmodel "github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgmodel/model/pgutf8str"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/tracer"
	"go.opentelemetry.io/otel/attribute"
)

const (
	seriesInsertSQL = "SELECT _prom_catalog.get_or_create_series_id_for_label_array($1, $2, l.elem), l.nr FROM unnest($3::prom_api.label_array[]) WITH ORDINALITY l(elem, nr) ORDER BY l.elem"
)

type seriesWriter struct {
	conn          pgxconn.PgxConn
	labelArrayOID uint32
	labelsCache   *cache.InvertedLabelsCache
}

type SeriesVisitor interface {
	VisitSeries(func(info *pgmodel.MetricInfo, s *model.Series) error) error
}

func labelArrayTranscoder() pgtype.ValueTranscoder { return &pgtype.Int4Array{} }

func NewSeriesWriter(conn pgxconn.PgxConn, labelArrayOID uint32, labelsCache *cache.InvertedLabelsCache) *seriesWriter {
	return &seriesWriter{conn, labelArrayOID, labelsCache}
}

type perMetricInfo struct {
	metricName             string
	series                 []*model.Series
	labelsToFetch          *model.LabelList // labels we haven't found in our cache and need to fetch from DB
	cachedLabels           []cache.LabelKey
	maxPos                 int
	labelArraySet          *pgtype.ArrayType
	labelArraySetNumLabels int
	metricInfo             *pgmodel.MetricInfo
}

// Set all seriesIds for a samples, fetching any missing ones from the DB,
// and repopulating the cache accordingly.
func (h *seriesWriter) WriteSeries(ctx context.Context, sv SeriesVisitor) error {
	ctx, span := tracer.Default().Start(ctx, "write-series")
	defer span.End()
	infos := make(map[string]*perMetricInfo)
	seriesCount := 0
	err := sv.VisitSeries(func(metricInfo *pgmodel.MetricInfo, series *model.Series) error {
		if !series.IsSeriesIDSet() {
			metricName := series.MetricName()
			info, ok := infos[metricName]
			if !ok {
				info = &perMetricInfo{
					metricInfo:    metricInfo,
					metricName:    metricName,
					labelsToFetch: model.NewLabelList(10),
					cachedLabels:  make([]cache.LabelKey, 0),
				}
				infos[metricName] = info
			}
			info.series = append(info.series, series)
			seriesCount++
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(infos) == 0 {
		return nil
	}

	span.SetAttributes(attribute.Int("series_count", seriesCount))

	labelMap := make(map[cache.LabelKey]cache.LabelInfo, seriesCount)
	//logically should be a separate function but we want
	//to prevent labelMap from escaping, so keeping inline.
	{
		for _, info := range infos {
			for _, series := range info.series {
				names, values, ok := series.NameValues()
				if !ok {
					//was already set
					continue
				}
				for i := range names {
					key := cache.LabelKey{MetricName: series.MetricName(), Name: names[i], Value: values[i]}
					_, added := labelMap[key]
					if !added {
						labelInfo, cached := h.labelsCache.GetLabelsId(cache.NewLabelKey(series.MetricName(), names[i], values[i]))
						labelMap[key] = labelInfo
						if cached {
							info.cachedLabels = append(info.cachedLabels, key)
							continue
						}
						if err := info.labelsToFetch.Add(names[i], values[i]); err != nil {
							return fmt.Errorf("failed to add label to labelList: %w", err)
						}
					}
				}
			}
		}
	}
	if len(labelMap) == 0 {
		return nil
	}

	//labels have to be created before series are since we need a canonical
	//ordering for label creation to avoid deadlocks. Otherwise, if we create
	//the labels for multiple series in same txn as we are creating the series,
	//the ordering of label creation can only be canonical within a series and
	//not across series.
	dbEpoch, err := h.fillLabelIDs(ctx, infos, labelMap)
	if err != nil {
		return fmt.Errorf("error setting series ids: %w", err)
	}

	//create the label arrays
	err = h.buildLabelArrays(ctx, infos, labelMap)
	if err != nil {
		return fmt.Errorf("error setting series ids: %w", err)
	}

	batch := h.conn.NewBatch()
	batchInfos := make([]*perMetricInfo, 0, len(infos))
	for _, info := range infos {
		if info.labelArraySetNumLabels == 0 {
			continue
		}

		//transaction per metric to avoid cross-metric locks
		batch.Queue("BEGIN;")
		batch.Queue(seriesInsertSQL, info.metricInfo.MetricID, info.metricInfo.TableName, info.labelArraySet)
		batch.Queue("COMMIT;")
		batchInfos = append(batchInfos, info)
	}
	br, err := h.conn.SendBatch(context.Background(), batch)
	if err != nil {
		return fmt.Errorf("error inserting series: %w", err)
	}
	defer br.Close()

	for _, info := range batchInfos {
		//begin
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("error setting series_id begin: %w", err)
		}

		res, err := br.Query()
		if err != nil {
			return fmt.Errorf("error setting series_id: cannot query for series_id: %w", err)
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
				return fmt.Errorf("error setting series_id: cannot scan series_id: %w", err)
			}
			info.series[int(ordinality)-1].SetSeriesID(id, dbEpoch)
			count++
		}
		if err := res.Err(); err != nil {
			return fmt.Errorf("error setting series_id: reading series id rows: %w", err)
		}
		if count != len(info.series) {
			//This should never happen according to the logic. This is purely defensive.
			//panic since we may have set the seriesID incorrectly above and may
			//get data corruption if we continue.
			panic(fmt.Sprintf("number series returned %d doesn't match expected series %d", count, len(info.series)))
		}
		//commit
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("error setting series_id commit: %w", err)
		}
	}
	return nil
}

func (h *seriesWriter) fillLabelIDs(ctx context.Context, infos map[string]*perMetricInfo, labelMap map[cache.LabelKey]cache.LabelInfo) (model.SeriesEpoch, error) {
	_, span := tracer.Default().Start(ctx, "fill-label-ids")
	defer span.End()
	//we cannot use the label cache here because that maps label ids => name, value.
	//what we need here is name, value => id.
	//we may want a new cache for that, at a later time.

	batch := h.conn.NewBatch()
	var dbEpoch model.SeriesEpoch

	// The epoch will never decrease, so we can check it once at the beginning,
	// at worst we'll store too small an epoch, which is always safe
	batch.Queue("BEGIN;")
	batch.Queue(getEpochSQL)
	batch.Queue("COMMIT;")

	infoBatches := make([]*perMetricInfo, 0)
	items := 0
	for metricName, info := range infos {
		names, values := info.labelsToFetch.Get()
		//getLabels in batches of 1000 to prevent locks on label creation
		//from being taken for too long.
		itemsPerBatch := 1000
		for i := 0; i < len(names.Elements); i += itemsPerBatch {
			high := i + itemsPerBatch
			if len(names.Elements) < high {
				high = len(names.Elements)
			}
			namesSlice, err := names.Slice(i, high)
			if err != nil {
				return dbEpoch, fmt.Errorf("error filling labels: slicing names: %w", err)
			}
			valuesSlice, err := values.Slice(i, high)
			if err != nil {
				return dbEpoch, fmt.Errorf("error filling labels: slicing values: %w", err)
			}
			batch.Queue("BEGIN;")
			batch.Queue("SELECT * FROM _prom_catalog.get_or_create_label_ids($1, $2, $3, $4)", metricName, info.metricInfo.TableName, namesSlice, valuesSlice)
			batch.Queue("COMMIT;")
			infoBatches = append(infoBatches, info)
			items += len(namesSlice.Elements)
		}
		// since info.maxPos is only updated with fetched labels we need to iterate through cached as well
		for _, cachedLabel := range info.cachedLabels {
			if val, ok := labelMap[cachedLabel]; ok {
				if int(val.Pos) > info.maxPos {
					info.maxPos = int(val.Pos)
				}
			}
		}
	}

	br, err := h.conn.SendBatch(context.Background(), batch)
	if err != nil {
		return dbEpoch, fmt.Errorf("error filling labels: %w", err)
	}
	defer br.Close()

	if _, err := br.Exec(); err != nil {
		return dbEpoch, fmt.Errorf("error filling labels on begin: %w", err)
	}
	err = br.QueryRow().Scan(&dbEpoch)
	if err != nil {
		return dbEpoch, fmt.Errorf("error filling labels: %w", err)
	}
	if _, err := br.Exec(); err != nil {
		return dbEpoch, fmt.Errorf("error filling labels on commit: %w", err)
	}

	var count int
	for _, info := range infoBatches {
		if _, err := br.Exec(); err != nil {
			return dbEpoch, fmt.Errorf("error filling labels on begin label batch: %w", err)
		}

		err := func() error {
			var (
				pos         []int32
				labelIDs    []int32
				labelNames  pgutf8str.TextArray
				labelValues pgutf8str.TextArray
				names       []string
				values      []string
			)
			err := br.QueryRow().Scan(&pos, &labelIDs, &labelNames, &labelValues)
			if err != nil {
				return fmt.Errorf("error filling labels: %w", err)
			}
			names = labelNames.Get().([]string)
			values = labelValues.Get().([]string)

			for i := range pos {
				res := cache.NewLabelInfo(labelIDs[i], pos[i])
				key := cache.NewLabelKey(info.metricName, names[i], values[i])
				if !h.labelsCache.Put(key, res) {
					log.Warn("failed to add label ID to inverted cache")
				}
				_, ok := labelMap[key]
				if !ok {
					return fmt.Errorf("error filling labels: getting a key never sent to the db")
				}
				labelMap[key] = res
				if int(res.Pos) > info.maxPos {
					info.maxPos = int(res.Pos)
				}
				count++
			}
			return nil
		}()
		if err != nil {
			return dbEpoch, err
		}
		if _, err := br.Exec(); err != nil {
			return dbEpoch, fmt.Errorf("error filling labels on commit label batch: %w", err)
		}
	}
	if count != items {
		return dbEpoch, fmt.Errorf("error filling labels: not filling as many items as expected: %v vs %v", count, items)
	}
	return dbEpoch, nil
}

func (h *seriesWriter) buildLabelArrays(ctx context.Context, infos map[string]*perMetricInfo, labelMap map[cache.LabelKey]cache.LabelInfo) error {
	_, span := tracer.Default().Start(ctx, "build-label-arrays")
	defer span.End()
	for _, info := range infos {
		labelArraySet, newSeries, err := createLabelArrays(info.series, labelMap, info.maxPos)
		if err != nil {
			return fmt.Errorf("error building label array: cannot create label_array: %w", err)
		}
		info.series = newSeries
		info.labelArraySet = pgtype.NewArrayType("prom_api.label_array[]", h.labelArrayOID, labelArrayTranscoder)
		err = info.labelArraySet.Set(labelArraySet)
		if err != nil {
			return fmt.Errorf("error setting series id: cannot set label_array: %w", err)
		}
		info.labelArraySetNumLabels = len(labelArraySet)

	}

	return nil
}

func createLabelArrays(series []*model.Series, labelMap map[cache.LabelKey]cache.LabelInfo, maxPos int) ([][]int32, []*model.Series, error) {
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
			key := cache.LabelKey{MetricName: series[src].MetricName(), Name: names[i], Value: values[i]}
			res, ok := labelMap[key]
			if !ok {
				return nil, nil, fmt.Errorf("error generating label array: missing key in map: %v", key)
			}
			if res.LabelID == 0 {
				return nil, nil, fmt.Errorf("error generating label array: missing id for label %v=>%v", names[i], values[i])
			}
			//Pos is 1-indexed, slices are 0-indexed
			sliceIndex := int(res.Pos) - 1
			lArray[sliceIndex] = int32(res.LabelID)
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
	series = series[:dest]
	if len(labelArraySet) != len(series) {
		return nil, nil, fmt.Errorf("error generating label array: lengths not equal")
	}
	return labelArraySet, series, nil
}
