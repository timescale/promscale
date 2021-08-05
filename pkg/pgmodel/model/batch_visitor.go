package model

import (
	"math"
	"sync"
	"time"

	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/timescale/promscale/pkg/prompb"
)

type batchVisitor struct {
	data        []Insertable
	lowestEpoch SeriesEpoch
	batchCopy   *Batch // Maintain a copy of batch to update the stats.
	err         error
}

var batchVisitorPool = sync.Pool{New: func() interface{} { return new(batchVisitor) }}

func putBatchVisitor(vtr *batchVisitor) {
	vtr.data = vtr.data[:0]
	vtr.lowestEpoch = SeriesEpoch(math.MaxInt64)
	vtr.batchCopy = nil
	vtr.err = nil
	batchVisitorPool.Put(vtr)
}

// Close preserves the visitor to be re-used.
func (vtr *batchVisitor) Close() {
	putBatchVisitor(vtr)
}

// Error returns the most recent error, that is encountered while iterating over insertables.
// It must be called after Visit() has completed.
func (vtr *batchVisitor) Error() error {
	return vtr.err
}

// LowestEpoch returns the lowest epoch value encountered while visiting insertables.
// It must be called after Visit() has completed.
func (vtr *batchVisitor) LowestEpoch() SeriesEpoch {
	return vtr.lowestEpoch
}

func (vtr *batchVisitor) Visit(
	appendSamples func(t time.Time, v float64, seriesId int64),
	appendExemplars func(t time.Time, v float64, seriesId int64, lvalues []string),
) {
	var (
		seriesId    SeriesID
		seriesEpoch SeriesEpoch
		err         error
	)
	updateStats := func(epoch SeriesEpoch, t int64) {
		if epoch < vtr.lowestEpoch {
			vtr.lowestEpoch = epoch
		}
		if vtr.batchCopy.MinSeen > t {
			vtr.batchCopy.MinSeen = t
		}
	}
	for _, insertable := range vtr.data {
		minTs := int64(math.MaxInt64)
		updateMinTs := func(t int64) {
			if t < minTs {
				minTs = t
			}
		}
		seriesId, seriesEpoch, err = insertable.Series().GetSeriesID()
		if err != nil {
			vtr.err = err
			continue
		}
		switch insertable.Type() {
		case Sample:
			itr := insertable.Iterator().(SamplesIterator)
			for itr.HasNext() {
				t, v := itr.Value()
				updateMinTs(t)
				appendSamples(timestamp.Time(t), v, int64(seriesId))
			}
		case Exemplar:
			itr := insertable.Iterator().(ExemplarsIterator)
			for itr.HasNext() {
				l, t, v := itr.Value()
				updateMinTs(t)
				appendExemplars(timestamp.Time(t), v, int64(seriesId), labelsToStringSlice(l))
			}
		}
		updateStats(seriesEpoch, minTs)
	}
}

func labelsToStringSlice(lbls []prompb.Label) []string {
	s := make([]string, len(lbls))
	for i := range lbls {
		s[i] = lbls[i].Value
	}
	return s
}
