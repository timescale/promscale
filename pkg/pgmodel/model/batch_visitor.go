package model

import (
	"fmt"
	"math"
	"time"

	"github.com/prometheus/common/model"
	"github.com/timescale/promscale/pkg/prompb"
)

type batchVisitor struct {
	batch       *Batch
	lowestEpoch SeriesEpoch
	minTime     int64
}

func getBatchVisitor(batch *Batch) *batchVisitor {
	return &batchVisitor{batch, SeriesEpoch(math.MaxInt64), math.MaxInt64}
}

// LowestEpoch returns the lowest epoch value encountered while visiting insertables.
// It must be called after Visit() has completed.
func (vtr *batchVisitor) LowestEpoch() SeriesEpoch {
	return vtr.lowestEpoch
}

func (vtr *batchVisitor) MinTime() int64 {
	return vtr.minTime
}

func (vtr *batchVisitor) Visit(
	visitSamples func(seriesId int64, t time.Time, v map[string]interface{}),
	visitExemplars func(t time.Time, v float64, seriesId int64, lvalues []string),
) error {
	var (
		seriesId    SeriesID
		seriesEpoch SeriesEpoch
		err         error
	)
	updateEpoch := func(epoch SeriesEpoch) {
		if epoch < vtr.lowestEpoch {
			vtr.lowestEpoch = epoch
		}
	}
	for _, insertable := range vtr.batch.data {
		updateMinTs := func(t int64) {
			if t < vtr.minTime {
				vtr.minTime = t
			}
		}
		seriesId, seriesEpoch, err = insertable.Series().GetSeriesID()
		if err != nil {
			return fmt.Errorf("get series-id: %w", err)
		}

		updateEpoch(seriesEpoch)

		switch insertable.Type() {
		case Sample:
			itr := insertable.Iterator().(SamplesIterator)
			for itr.HasNext() {
				t, v := itr.Value()
				updateMinTs(t)
				visitSamples(int64(seriesId), model.Time(t).Time(), v)
			}
		case Exemplar:
			itr := insertable.Iterator().(ExemplarsIterator)
			for itr.HasNext() {
				l, t, v := itr.Value()
				updateMinTs(t)
				visitExemplars(model.Time(t).Time(), v, int64(seriesId), labelsToStringSlice(l))
			}
		}
	}
	return nil
}

func labelsToStringSlice(lbls []prompb.Label) []string {
	s := make([]string, len(lbls))
	for i := range lbls {
		s[i] = lbls[i].Value
	}
	return s
}
