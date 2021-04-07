// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package samples_parser

import (
	"github.com/timescale/promscale/pkg/ha"
	"github.com/timescale/promscale/pkg/multi-tenancy/write"
	multi_tenancy_write "github.com/timescale/promscale/pkg/multi-tenancy/write"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

// Data parser is responsible for parsing incoming request data into the
// appropriate format and filter it using the appropriate filter.
type Parser struct {
	filter *ha.Filter
	sCache cache.SeriesCache
	multiTenancy write.Authorizer
}

func DefaultParser(seriesCache cache.SeriesCache, authr multi_tenancy_write.Authorizer) Parser {
	return Parser{
		scache:       seriesCache,
		multiTenancy: authr,
	}
}

// SetFilter sets the HA filter used for filtering the data before parsing
// it into the correct output format.
func (d *Parser) SetFilter(filter *ha.Filter) {
	d.filter = filter
}

// ParseData filters and parses data into a set of samplesInfo infos per-metric.
// returns: map[metric name][]SamplesInfo, total rows to insert
func (d *Parser) ParseData(tenant string, tts []prompb.TimeSeries) (map[string][]model.Samples, int, error) {
	if d.filter != nil {
		canProceed, err := d.filter.FilterData(tts)
		if err != nil {
			return nil, 0, err
		}
		if !canProceed {
			return nil, 0, nil
		}
	}
	dataSamples := make(map[string][]model.Samples)
	rows := 0

	for i := range tts {
		t := &tts[i]
		if len(t.Samples) == 0 {
			continue
		}

		var (
			ok     bool
			labels = t.Labels
		)
		if d.multiTenancy != nil {
			labels, ok = d.multiTenancy.VerifyAndApplyTenantLabel(tenant, t.Labels)
			if !ok {
				return nil, -1, multi_tenancy_write.ErrInvalidLabels
			}
		}

		// Normalize and canonicalize t.Labels.
		// After this point t.Labels should never be used again.
		seriesLabels, metricName, err := d.sCache.GetSeriesFromProtos(labels)
		if err != nil {
			return nil, rows, err
		}
		if metricName == "" {
			return nil, rows, errors.ErrNoMetricName
		}
		sample := model.NewPromSample(seriesLabels, t.Samples)
		rows += len(t.Samples)

		dataSamples[metricName] = append(dataSamples[metricName], sample)
		// we're going to free req after this, but we still need the samples,
		// so nil the field
		t.Samples = nil
	}

	return dataSamples, rows, nil
}
