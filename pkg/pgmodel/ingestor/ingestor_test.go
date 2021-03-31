// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

func TestDBIngestorIngestProto(t *testing.T) {
	testCases := []struct {
		name            string
		metrics         []prompb.TimeSeries
		count           uint64
		countSeries     int
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
			count:       1,
			countSeries: 1,
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
			count:       2,
			countSeries: 2,
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
			count:       2,
			countSeries: 1,
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
			count:       2,
			countSeries: 1,
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
			count:           0,
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
			count:         0,
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
			count:       0,
			countSeries: 0,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			scache := cache.NewSeriesCache(cache.DefaultConfig, nil)
			inserter := model.MockInserter{
				InsertSeriesErr: c.insertSeriesErr,
				InsertDataErr:   c.insertDataErr,
				InsertedSeries:  make(map[string]model.SeriesID),
			}

			i := DBIngestor{
				db:     &inserter,
				scache: scache,
			}

			count, err := i.IngestProto(c.metrics, NewWriteRequest())

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
				if err == errors.ErrNoMetricName {
					for _, ts := range c.metrics {
						for _, label := range ts.Labels {
							if label.Name == model.MetricNameLabelName {
								t.Errorf("returning missing metric name when one was found for metric name: %s\n", label.Name)
							}
						}
					}
				}
			}

			if count != c.count {
				t.Errorf("invalid number of metrics inserted: got %d, want %d\n", count, c.count)
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

func TestDBIngestorIngestText(t *testing.T) {
	testCases := []struct {
		name            string
		input           string
		contentType     string
		scrapeTime      time.Time
		count           uint64
		countSeries     int
		returnErr       string
		insertSeriesErr error
		insertDataErr   error
		getSeriesErr    error
		setSeriesErr    error
	}{
		{
			name:  "Zero metrics",
			input: "",
		},
		{
			name:        "One metric no timestamp",
			input:       "test 0.1 ",
			count:       1,
			countSeries: 1,
		},
		{
			name:        "One metric with timestamp",
			input:       "test 0.1 1",
			count:       1,
			countSeries: 1,
		},
		{
			name:      "One metric, no sample",
			input:     `test{test="test"}`,
			returnErr: `error parsing text entries: expected value after metric, got "MNAME"`,
		},
		{
			name: "Two metrics",
			input: `test 0.1
foo{test="test"} 0.1`,
			count:       2,
			countSeries: 2,
		},
		{
			name: "Two samples",
			input: `test 0.1 1
test 0.1 2`,
			count:       2,
			countSeries: 1,
		},
		{
			name:            "Insert series error",
			input:           "test 0.1 ",
			count:           0,
			countSeries:     1,
			insertSeriesErr: fmt.Errorf("some error"),
		},
		{
			name:          "Insert data error",
			input:         "test 0.1 ",
			count:         0,
			countSeries:   1,
			insertDataErr: fmt.Errorf("some error"),
		},
		{
			name:      "Invalid metric name",
			input:     `1`,
			returnErr: `error parsing text entries: "INVALID" is not a valid start token`,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			scache := cache.NewSeriesCache(cache.DefaultConfig, nil)
			inserter := model.MockInserter{
				InsertSeriesErr: c.insertSeriesErr,
				InsertDataErr:   c.insertDataErr,
				InsertedSeries:  make(map[string]model.SeriesID),
			}

			i := DBIngestor{
				db:     &inserter,
				scache: scache,
			}

			count, err := i.IngestText(bytes.NewBuffer([]byte(c.input)), c.contentType, c.scrapeTime)

			if err != nil {
				switch {
				case c.returnErr != "":
					if err.Error() != c.returnErr {
						t.Errorf("wrong error returned: got\n%s\nwant\n%s\n", err, c.returnErr)
					}
				case c.insertSeriesErr != nil:
					if err != c.insertSeriesErr {
						t.Errorf("wrong error returned: got\n%s\nwant\n%s\n", err, c.insertSeriesErr)
					}
				case c.insertDataErr != nil:
					if err != c.insertDataErr {
						t.Errorf("wrong error returned: got\n%s\nwant\n%s\n", err, c.insertDataErr)
					}
				case c.getSeriesErr != nil:
					if err != c.getSeriesErr {
						t.Errorf("wrong error returned: got\n%s\nwant\n%s\n", err, c.getSeriesErr)
					}
				case c.setSeriesErr != nil:
					if err != c.setSeriesErr {
						t.Errorf("wrong error returned: got\n%s\nwant\n%s\n", err, c.setSeriesErr)
					}
				default:
					t.Fatalf("unexpected error returned: %s", err)
				}

				return

			}

			if count != c.count {
				t.Errorf("invalid number of metrics inserted: got %d, want %d\n", count, c.count)
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

var scache mockCache

func getModelSamples(l labels.Labels, s []prompb.Sample) []model.Samples {
	sl, _, _ := scache.GetSeriesFromLabels(l)

	return []model.Samples{
		model.NewPromSample(sl, s),
	}
}

func TestDBIngestorParseTextData(t *testing.T) {
	defaultScrapeTime := time.Now()

	testCases := []struct {
		name          string
		input         string
		reader        io.Reader
		contentType   string
		scrapeTime    time.Time
		returnErr     string
		cacheErr      error
		returnSamples map[string][]model.Samples
		count         int
	}{
		{
			name: "happy path",
			input: `# HELP go_gc_duration_seconds A summary of the GC invocation durations.
#   TYPE go_gc_duration_seconds summary
# Hrandom comment starting with prefix of HELP
#
# comment with escaped \n newline
# comment with escaped \ escape character
# HELP nohelp1
# HELP nohelp2
# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
_metric_starting_with_underscore 1`,
			count: 1,
			returnSamples: map[string][]model.Samples{
				"_metric_starting_with_underscore": getModelSamples(
					labels.Labels{
						{
							Name:  model.MetricNameLabelName,
							Value: "_metric_starting_with_underscore",
						},
					},
					[]prompb.Sample{
						{
							Timestamp: timestamp.FromTime(defaultScrapeTime),
							Value:     1,
						},
					},
				),
			},
		},
		{
			name: "reader error",
			reader: &errReader{
				err: fmt.Errorf("some error"),
			},
			returnErr: "error reading request body: some error",
		},
		{
			name:      "cache error",
			input:     "test 1",
			cacheErr:  fmt.Errorf("cache error"),
			returnErr: "cache error",
		},
		{
			name:       "check scrape time",
			input:      "test_metric 1",
			count:      1,
			scrapeTime: defaultScrapeTime.Add(time.Hour),
			returnSamples: map[string][]model.Samples{
				"test_metric": getModelSamples(
					labels.Labels{
						{
							Name:  model.MetricNameLabelName,
							Value: "test_metric",
						},
					},
					[]prompb.Sample{
						{
							Timestamp: timestamp.FromTime(defaultScrapeTime.Add(time.Hour)),
							Value:     1,
						},
					},
				),
			},
		},
		{
			name:        "open metrics content type",
			input:       "test_metric 1\n# EOF",
			count:       1,
			contentType: "application/openmetrics-text",
			returnSamples: map[string][]model.Samples{
				"test_metric": getModelSamples(
					labels.Labels{
						{
							Name:  model.MetricNameLabelName,
							Value: "test_metric",
						},
					},
					[]prompb.Sample{
						{
							Timestamp: timestamp.FromTime(defaultScrapeTime),
							Value:     1,
						},
					},
				),
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			inserter := model.MockInserter{}

			scache.err = nil
			if c.cacheErr != nil {
				scache.err = c.cacheErr
			}

			i := DBIngestor{
				db:     &inserter,
				scache: &scache,
			}

			var r io.Reader = bytes.NewBuffer([]byte(c.input))
			if c.reader != nil {
				r = c.reader
			}

			now := defaultScrapeTime

			if !c.scrapeTime.IsZero() {
				now = c.scrapeTime
			}

			result, count, err := i.parseTextData(r, c.contentType, now)

			if err != nil {
				if c.returnErr != err.Error() {
					t.Errorf("wrong error returned: got\n%s\nwant\n%s\n", err, c.returnErr)
				}
				return
			} else {
				if c.returnErr != "" {
					t.Fatalf("unexpected error returned: %s", err)
				}
			}

			if count != c.count {
				t.Errorf("invalid number of metrics parsed: got %d, want %d\n", count, c.count)
			}

			if len(result) != len(c.returnSamples) {
				t.Errorf("invalid count of samples returned: got %d want %d", len(result), len(c.returnSamples))
			}

			for k, v := range c.returnSamples {
				expected, ok := result[k]

				if !ok {
					t.Fatalf("missing return samples for metric %s", k)
				}

				if len(expected) != len(v) {
					t.Fatalf("invalid count of return samples for metric %s: got %d wanted %d", k, len(v), len(expected))
				}

				for i, s := range expected {
					if !reflect.DeepEqual(s, v[i]) {
						t.Fatalf("unexpected value returned for metric %s:\ngot\n%+v\nwanted\n%+v\n", k, v[i], s)
					}
				}
			}
		})
	}
}

type mockCache struct {
	initialized bool
	series      map[string]*model.Series
	metricNames map[string]string
	err         error
}

func (m *mockCache) Evictions() uint64 {
	return 0
}

func (m *mockCache) Reset() {
	m.series = make(map[string]*model.Series)
	m.metricNames = make(map[string]string)
}

func (m *mockCache) GetSeriesFromProtos(labelPairs []prompb.Label) (*model.Series, string, error) {
	return nil, "", nil
}

func (m *mockCache) GetSeriesFromLabels(ll labels.Labels) (*model.Series, string, error) {
	if !m.initialized {
		m.Reset()
	}
	key := ll.String()
	if series, ok := m.series[key]; ok {
		return series, m.metricNames[key], m.err
	}

	series := model.NewSeries(key, []prompb.Label{})
	m.series[key] = series

	metricName := ""

	for _, v := range ll {
		if v.Name == model.MetricNameLabelName {
			metricName = v.Value
			break
		}
	}

	if metricName == "" {
		return nil, "", fmt.Errorf("no metric name")
	}

	m.metricNames[key] = metricName

	return series, metricName, m.err
}

func (m *mockCache) Len() int {
	return len(m.series)
}

func (m *mockCache) Cap() int {
	return len(m.series)
}

type errReader struct {
	err error
}

func (e *errReader) Read(p []byte) (n int, err error) {
	return 0, e.err
}
