package model

import (
	"fmt"
	"math"
	"time"

	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/timescale/promscale/pkg/prompb"
)

type batchVisitor struct {
	data        []Insertable
	lowestEpoch SeriesEpoch
	batchCopy   *Batch // Maintain a copy of batch to update the stats.
}

func getBatchVisitor(batch *Batch) *batchVisitor {
	return &batchVisitor{batch.data, SeriesEpoch(math.MaxInt64), batch}
}

func (vtr *batchVisitor) Close() {
	// TODO: Is this explanation right?
	// Remove the reference from the batch so that GC can collect the visitor alloc.
	// Otherwise, this may lead to mem leak, as batch itself cannot be GCed since its being
	// linked with the visitor. Hence, now we have both *Batch and *batchVisitor unable to
	// GCed.
	vtr.data = nil
	vtr.batchCopy = nil
}

// LowestEpoch returns the lowest epoch value encountered while visiting insertables.
// It must be called after Visit() has completed.
func (vtr *batchVisitor) LowestEpoch() SeriesEpoch {
	return vtr.lowestEpoch
}

func (vtr *batchVisitor) Visit(
	appendSamples func(t time.Time, v float64, seriesId int64),
	appendExemplars func(t time.Time, v float64, seriesId int64, lvalues []string),
) error {
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
			return fmt.Errorf("get series-id: %w", err)
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
	return nil
}

func labelsToStringSlice(lbls []prompb.Label) []string {
	s := make([]string, len(lbls))
	for i := range lbls {
		s[i] = lbls[i].Value
	}
	return s
}
