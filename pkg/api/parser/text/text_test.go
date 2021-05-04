// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package text

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

func TestDBIngestorParseTextData(t *testing.T) {
	defaultScrapeTime := time.Now()

	timeProvider = func() time.Time {
		return defaultScrapeTime
	}

	testCases := []struct {
		name        string
		input       string
		result      prompb.WriteRequest
		readerError bool
		err         string
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
			result: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{

						Labels: []prompb.Label{
							{
								Name:  model.MetricNameLabelName,
								Value: "_metric_starting_with_underscore",
							},
						},
						Samples: []prompb.Sample{
							{
								Timestamp: timestamp.FromTime(defaultScrapeTime),
								Value:     1,
							},
						},
					},
				},
			},
		},
		{
			name: "multi metric",
			input: `foo{custom_label="bar"} 1
foo{custom_label="baz"} 2
bar{custom_label="foo"} 3
bat{custom_label="foo"} 4`,
			result: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{
								Name:  model.MetricNameLabelName,
								Value: "foo",
							},
							{
								Name:  "custom_label",
								Value: "bar",
							},
						},
						Samples: []prompb.Sample{
							{
								Timestamp: timestamp.FromTime(defaultScrapeTime),
								Value:     1,
							},
						},
					},
					{
						Labels: []prompb.Label{
							{
								Name:  model.MetricNameLabelName,
								Value: "foo",
							},
							{
								Name:  "custom_label",
								Value: "baz",
							},
						},
						Samples: []prompb.Sample{
							{
								Timestamp: timestamp.FromTime(defaultScrapeTime),
								Value:     2,
							},
						},
					},
					{
						Labels: []prompb.Label{
							{
								Name:  model.MetricNameLabelName,
								Value: "bar",
							},
							{
								Name:  "custom_label",
								Value: "foo",
							},
						},
						Samples: []prompb.Sample{
							{
								Timestamp: timestamp.FromTime(defaultScrapeTime),
								Value:     3,
							},
						},
					},
					{
						Labels: []prompb.Label{
							{
								Name:  model.MetricNameLabelName,
								Value: "bat",
							},
							{
								Name:  "custom_label",
								Value: "foo",
							},
						},
						Samples: []prompb.Sample{
							{
								Timestamp: timestamp.FromTime(defaultScrapeTime),
								Value:     4,
							},
						},
					},
				},
			},
		},
		{
			name:  "invalid input",
			input: "invalid",
			err:   `error parsing text entries: expected value after metric, got "MNAME"`,
		},
		{
			name:  "input with time",
			input: "test_metric 1 1000",
			result: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{
								Name:  model.MetricNameLabelName,
								Value: "test_metric",
							},
						},
						Samples: []prompb.Sample{
							{
								Timestamp: 1000,
								Value:     1,
							},
						},
					},
				},
			},
		},
		{
			name:  "open metrics content type",
			input: "test_metric 1\n# EOF",
			result: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{
								Name:  model.MetricNameLabelName,
								Value: "test_metric",
							},
						},
						Samples: []prompb.Sample{
							{
								Timestamp: timestamp.FromTime(defaultScrapeTime),
								Value:     1,
							},
						},
					},
				},
			},
		},
		{
			name:        "body reader error",
			readerError: true,
			err:         "error reading request body: some error",
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {

			var r io.Reader = bytes.NewBuffer([]byte(c.input))
			if c.readerError {
				r = &errorReader{}
			}
			req, _ := http.NewRequest("", "", r)
			wr := ingestor.NewWriteRequest()
			defer ingestor.FinishWriteRequest(wr)

			err := ParseRequest(req, wr)

			if err != nil {
				if c.err != err.Error() {
					t.Errorf("wrong error returned: got\n%s\nwant\n%s\n", err.Error(), c.err)
				}
				return
			}

			if c.err != "" {
				t.Fatalf("unexpected error returned: %s", err)
			}

			if wr.String() != c.result.String() {
				t.Fatalf("unexpected result returned:\ngot\n%s\nwanted\n%s\n", wr.String(), c.result.String())
			}
		})
	}
}

type errorReader struct{}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("some error")
}
