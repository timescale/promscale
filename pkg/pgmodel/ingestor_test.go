package pgmodel

import (
	"fmt"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

type mockCache struct {
	seriesCache  map[uint64]SeriesID
	getSeriesErr error
	setSeriesErr error
}

func (m *mockCache) GetSeries(fp uint64) (SeriesID, error) {
	if m.getSeriesErr != nil {
		return 0, m.getSeriesErr
	}

	val, ok := m.seriesCache[fp]
	if !ok {
		return 0, ErrEntryNotFound
	}

	return val, nil
}

func (m *mockCache) SetSeries(fp uint64, id SeriesID) error {
	m.seriesCache[fp] = id
	return m.setSeriesErr
}

type mockInserter struct {
	seriesToInsert  []*seriesWithFP
	fpToInsert      []uint64
	insertedSeries  []*seriesWithFP
	insertedData    []map[string][][]interface{}
	insertSeriesErr error
	insertDataErr   error
}

func (m *mockInserter) AddSeries(fingerprint uint64, series *model.LabelSet) {
	m.seriesToInsert = append(m.seriesToInsert, &seriesWithFP{series, fingerprint})
	m.fpToInsert = append(m.fpToInsert, fingerprint)
}

func (m *mockInserter) InsertSeries() ([]SeriesID, []uint64, error) {
	defer func(m *mockInserter) { m.fpToInsert = make([]uint64, 0) }(m)
	m.insertedSeries = append(m.insertedSeries, m.seriesToInsert...)
	m.seriesToInsert = make([]*seriesWithFP, 0)
	ids := make([]SeriesID, len(m.fpToInsert))
	return ids, m.fpToInsert, m.insertSeriesErr
}

func (m *mockInserter) InsertData(rows map[string][][]interface{}) (uint64, error) {
	m.insertedData = append(m.insertedData, rows)
	ret := 0
	for _, data := range rows {
		ret = ret + len(data)
	}
	if m.insertDataErr != nil {
		ret = 0
	}
	return uint64(ret), m.insertDataErr
}

func TestDBIngestorIngest(t *testing.T) {
	testCases := []struct {
		name            string
		metrics         []*prompb.TimeSeries
		count           uint64
		countSeries     int
		insertSeriesErr error
		insertDataErr   error
		getSeriesErr    error
		setSeriesErr    error
	}{
		{
			name:    "Zero metrics",
			metrics: []*prompb.TimeSeries{},
		},
		{
			name: "One metric",
			metrics: []*prompb.TimeSeries{
				&prompb.TimeSeries{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			count:       1,
			countSeries: 1,
		},
		{
			name: "One metric, no sample",
			metrics: []*prompb.TimeSeries{
				&prompb.TimeSeries{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
				},
			},
		},
		{
			name: "Two metrics",
			metrics: []*prompb.TimeSeries{
				&prompb.TimeSeries{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "foo", Value: "bar"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
				&prompb.TimeSeries{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			count:       2,
			countSeries: 2,
		},
		{
			name: "Two samples",
			metrics: []*prompb.TimeSeries{
				&prompb.TimeSeries{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
						{Timestamp: 2, Value: 0.2},
					},
				},
			},
			count:       2,
			countSeries: 1,
		},
		{
			name: "Two metrics, one series",
			metrics: []*prompb.TimeSeries{
				&prompb.TimeSeries{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
				&prompb.TimeSeries{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 2, Value: 0.2},
					},
				},
			},
			count:       2,
			countSeries: 2,
		},
		{
			name: "Insert series error",
			metrics: []*prompb.TimeSeries{
				&prompb.TimeSeries{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			count:           0,
			countSeries:     1,
			insertSeriesErr: fmt.Errorf("some error"),
		},
		{
			name: "Insert data error",
			metrics: []*prompb.TimeSeries{
				&prompb.TimeSeries{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			count:         0,
			countSeries:   1,
			insertDataErr: fmt.Errorf("some error"),
		},
		{
			name: "Set series error",
			metrics: []*prompb.TimeSeries{
				&prompb.TimeSeries{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			count:        0,
			countSeries:  1,
			setSeriesErr: fmt.Errorf("some error"),
		},
		{
			name: "Get series error",
			metrics: []*prompb.TimeSeries{
				&prompb.TimeSeries{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			count:        0,
			countSeries:  0,
			getSeriesErr: fmt.Errorf("some error"),
		},
		{
			name: "Missing metric name",
			metrics: []*prompb.TimeSeries{
				&prompb.TimeSeries{
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			count:       0,
			countSeries: 0,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			cache := &mockCache{
				seriesCache:  make(map[uint64]SeriesID, 0),
				setSeriesErr: c.setSeriesErr,
				getSeriesErr: c.getSeriesErr,
			}
			inserter := mockInserter{
				insertSeriesErr: c.insertSeriesErr,
				insertDataErr:   c.insertDataErr,
			}

			i := DBIngestor{
				cache: cache,
				db:    &inserter,
			}

			count, err := i.Ingest(c.metrics)

			if err != nil {
				if c.insertSeriesErr != nil && err != c.insertSeriesErr {
					t.Errorf("wrong error returned: got\n%s\nwant\n%s\n", err, c.insertSeriesErr)
				}
				if c.insertDataErr != nil && err != c.insertDataErr {
					t.Errorf("wrong error returned: got\n%s\nwant\n%s\n", err, c.insertDataErr)
				}
				if c.getSeriesErr != nil && err != c.getSeriesErr {
					t.Errorf("wrong error returned: got\n%s\nwant\n%s\n", err, c.getSeriesErr)
				}
				if c.setSeriesErr != nil && err != c.setSeriesErr {
					t.Errorf("wrong error returned: got\n%s\nwant\n%s\n", err, c.setSeriesErr)
				}
				if err == errNoMetricName {
					for _, ts := range c.metrics {
						for _, label := range ts.Labels {
							if label.Name == metricNameLabelName {
								t.Errorf("returning missing metric name when one was found for metric name: %s\n", label.Name)
							}
						}
					}
				}
			}

			if count != c.count {
				t.Errorf("invalid number of metrics inserted (test %v) : got %d, want %d\n", c.name, count, c.count)
			}

			if c.countSeries != len(inserter.insertedSeries) {
				t.Errorf("invalid number of series inserted, all series that are not cached must be sent for insertion: got %d, want %d\n",
					len(inserter.insertedSeries),
					c.countSeries,
				)
			}
		})
	}
}
