package model

import (
	"fmt"
	"math"
	"time"

	"github.com/prometheus/common/model"
	"github.com/timescale/promscale/pkg/prompb"
)

type batchVisitor struct {
	batch   *Batch
	minTime int64
}

var MaxDate int64 = math.MaxInt64

func getBatchVisitor(batch *Batch) *batchVisitor {
	return &batchVisitor{batch, math.MaxInt64}
}

func (vtr *batchVisitor) MinTime() int64 {
	return vtr.minTime
}

func (vtr *batchVisitor) Visit(
	visitSamples func(t time.Time, v float64, seriesId int64),
	visitExemplars func(t time.Time, v float64, seriesId int64, lvalues []string),
) error {
	var (
		seriesId SeriesID
		err      error
	)
	for _, insertable := range vtr.batch.data {
		updateMinTs := func(t int64) {
			if t < vtr.minTime {
				vtr.minTime = t
			}
		}
		seriesId, err = insertable.Series().GetSeriesID()
		if err != nil {
			return fmt.Errorf("get series-id: %w", err)
		}

		switch insertable.Type() {
		case Sample:
			itr := insertable.Iterator().(SamplesIterator)
			for itr.HasNext() {
				t, v := itr.Value()
				updateMinTs(t)
				visitSamples(model.Time(t).Time(), v, int64(seriesId))
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
