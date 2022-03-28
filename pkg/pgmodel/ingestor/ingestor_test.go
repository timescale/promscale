// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/timescale/promscale/pkg/pgmodel/cache"
	pgmodelcommon "github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

func TestDBIngestorIngest(t *testing.T) {
	testCases := []struct {
		name            string
		metrics         []prompb.TimeSeries
		metadata        []prompb.MetricMetadata
		countSamples    uint64
		count           int64
		countSeries     int
		countMetadata   uint64
		insertSeriesErr error
		insertDataErr   error
		getSeriesErr    error
		setSeriesErr    error
	}{
		{
			name:    "Zero metrics",
			metrics: []prompb.TimeSeries{},
		},
		{
			name: "One metric",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			countSamples: 1,
			countSeries:  1,
		},
		{
			name:    "One metadata",
			metrics: []prompb.TimeSeries{},
			metadata: []prompb.MetricMetadata{
				{
					MetricFamilyName: "random_metric",
					Unit:             "units",
					Type:             1,
					Help:             "random test metric",
				},
			},
			countMetadata: 1,
		},
		{
			name: "One metric & one metadata",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			metadata: []prompb.MetricMetadata{
				{
					MetricFamilyName: "random_metric",
					Unit:             "units",
					Type:             1,
					Help:             "random test metric",
				},
			},
			countMetadata: 1,
			countSamples:  1,
			countSeries:   1,
		},
		{
			name: "One metric, no sample",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
				},
			},
		},
		{
			name: "Two metrics",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
						{Name: "foo", Value: "bar"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			countSamples: 2,
			countSeries:  2,
		},
		{
			name: "Two metrics & two metadata",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
						{Name: "foo", Value: "bar"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			metadata: []prompb.MetricMetadata{
				{
					MetricFamilyName: "random_metric",
					Unit:             "units",
					Type:             1,
					Help:             "random test metric",
				},
				{
					MetricFamilyName: "random_metric_2",
					Unit:             "units",
					Type:             2,
					Help:             "random test metric 2",
				},
			},
			countMetadata: 2,
			countSamples:  2,
			countSeries:   2,
		},
		{
			name: "Two samples",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
						{Timestamp: 2, Value: 0.2},
					},
				},
			},
			countSamples: 2,
			countSeries:  1,
		},
		{
			name: "Two metrics, one series",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 2, Value: 0.2},
					},
				},
			},
			countSamples: 2,
			countSeries:  1,
		},
		{
			name: "Insert series error",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			countSamples:    0,
			countSeries:     1,
			insertSeriesErr: fmt.Errorf("some error"),
		},
		{
			name: "Insert data error",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			countSamples:  0,
			countSeries:   1,
			insertDataErr: fmt.Errorf("some error"),
		},
		{
			name: "Missing metric name",
			metrics: []prompb.TimeSeries{
				{
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			countSamples: 0,
			countSeries:  0,
		},
	}

	ctx := context.Background()

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			sCache := cache.NewSeriesCache(cache.DefaultConfig, nil)
			inserter := model.MockInserter{
				InsertSeriesErr: c.insertSeriesErr,
				InsertDataErr:   c.insertDataErr,
				InsertedSeries:  make(map[string]model.SeriesID),
			}
			i := DBIngestor{
				dispatcher: &inserter,
				sCache:     sCache,
			}

			wr := NewWriteRequest()
			wr.Timeseries = c.metrics
			wr.Metadata = c.metadata
			countSamples, countMetadata, err := i.Ingest(ctx, wr)
			if err != nil {
				if c.insertSeriesErr != nil && !errors.Is(err, c.insertSeriesErr) {
					t.Errorf("wrong error returned: got\n%s\nwant\n%s\n", err, c.insertSeriesErr)
				}
				if c.insertDataErr != nil && err.Error() != c.insertDataErr.Error() {
					t.Errorf("wrong error returned: got\n%s\nwant\n%s\n", err, c.insertDataErr)
				}
				if c.getSeriesErr != nil && !errors.Is(err, c.getSeriesErr) {
					t.Errorf("wrong error returned: got\n%s\nwant\n%s\n", err, c.getSeriesErr)
				}
				if c.setSeriesErr != nil && !errors.Is(err, c.setSeriesErr) {
					t.Errorf("wrong error returned: got\n%s\nwant\n%s\n", err, c.setSeriesErr)
				}
				if errors.Is(err, pgmodelcommon.ErrNoMetricName) {
					for _, ts := range c.metrics {
						for _, label := range ts.Labels {
							if label.Name == model.MetricNameLabelName {
								t.Errorf("returning missing metric name when one was found for metric name: %s\n", label.Name)
							}
						}
					}
				}
			}

			if countSamples != c.countSamples {
				t.Errorf("invalid number of metrics inserted: got %d, want %d\n", countSamples, c.countSamples)
			}

			if countMetadata != c.countMetadata {
				t.Errorf("invalid number of metadata inserted: got %d, want %d\n", countMetadata, c.countMetadata)
			}

			if c.countSeries != len(inserter.InsertedSeries) {
				t.Errorf("invalid number of series inserted, all series that are not cached must be sent for insertion: got %d, want %d\n%+v\n",
					len(inserter.InsertedSeries),
					c.countSeries,
					inserter.InsertedSeries,
				)
			}
		})
	}
}
