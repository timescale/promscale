package bench

import (
	"strconv"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/record"
)

type DataModifier struct {
	conf           *BenchConfig
	build          *labels.Builder
	init           bool
	firstDataRawTs int64
	firstDataOutTs int64
	firstWallTs    int64
}

func NewDataModifier(conf *BenchConfig) *DataModifier {
	return &DataModifier{
		conf:  conf,
		build: labels.NewBuilder(nil),
	}
}

func (d *DataModifier) VisitSeries(rawSeriesID uint64, lbls labels.Labels,
	visitor func(rec record.RefSeries)) {
	if d.conf.SeriesMultiplier == 1 && d.conf.MetricMultiplier == 1 {
		rs := record.RefSeries{
			Ref:    rawSeriesID,
			Labels: lbls,
		}
		visitor(rs)
	} else {
		seriesID := rawSeriesID
		for seriesMultiplierIndex := 0; seriesMultiplierIndex < d.conf.SeriesMultiplier; seriesMultiplierIndex++ {
			for metricMultiplierIndex := 0; metricMultiplierIndex < d.conf.MetricMultiplier; metricMultiplierIndex++ {
				d.build.Reset(lbls)
				if seriesMultiplierIndex != 0 {
					d.build.Set("multiplier", strconv.Itoa(seriesMultiplierIndex))
				}
				if metricMultiplierIndex != 0 {
					d.build.Set("__name__", lbls.Get("__name__")+"_"+strconv.Itoa(metricMultiplierIndex))
				}
				rs := record.RefSeries{
					Ref:    seriesID,
					Labels: d.build.Labels(),
				}
				visitor(rs)
				seriesID++
			}
		}
	}
}

func (dm *DataModifier) VisitSamples(rawSeriesID uint64, rawTs int64, rawVal float64,
	visitor func([]record.RefSample, int64) error) error {
	if !dm.init {
		now := time.Now()
		dm.firstWallTs = int64(model.TimeFromUnixNano(now.UnixNano()))
		dm.firstDataRawTs = rawTs
		dm.firstDataOutTs = dm.firstDataRawTs
		if dm.conf.UseWallClockForDataTime {
			dm.firstDataOutTs = dm.firstWallTs
		}
		dm.init = true
	}

	timestampDelta := int64(float64(rawTs-dm.firstDataRawTs) / dm.conf.RateMultiplier)
	dataOutTs := dm.firstDataOutTs + timestampDelta
	wallTs := dm.firstWallTs + timestampDelta

	seriesID := rawSeriesID
	for seriesMultiplierIndex := 0; seriesMultiplierIndex < dm.conf.SeriesMultiplier; seriesMultiplierIndex++ {
		for metricMultiplierIndex := 0; metricMultiplierIndex < dm.conf.MetricMultiplier; metricMultiplierIndex++ {
			samples := []record.RefSample{
				{
					Ref: seriesID,
					T:   dataOutTs,
					V:   rawVal,
				},
			}
			err := visitor(samples, wallTs)
			if err != nil {
				return err
			}
			seriesID++
		}
	}

	return nil
}
