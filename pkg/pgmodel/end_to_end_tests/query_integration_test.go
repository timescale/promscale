package end_to_end_tests

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/util/testutil"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/promql"
	"github.com/timescale/promscale/pkg/query"

	. "github.com/timescale/promscale/pkg/pgmodel"
)

// PromClient is a wrapper around http.Client for sending read requests.
type PromClient struct {
	url        *url.URL
	httpClient *http.Client
}

// NewPromClient ..
func NewPromClient(urlStr string, timeout time.Duration) (*PromClient, error) {
	url, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	httpClient := &http.Client{Timeout: timeout}
	return &PromClient{url: url, httpClient: httpClient}, nil
}

// Read sends a read request to Prometheus remote read endpoint.
func (c *PromClient) Read(rr *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	data, err := proto.Marshal(rr)
	if err != nil {
		return nil, err
	}
	compressed := snappy.Encode(nil, data)
	httpReq, err := http.NewRequest("POST", c.url.String(), bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("Prometheus returned status: %s", httpResp.Status)
	}

	compressed, err = ioutil.ReadAll(httpResp.Body)
	if err != nil {
		return nil, err
	}

	resBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, err
	}

	res := &prompb.ReadResponse{}
	if err := proto.Unmarshal(resBuf, res); err != nil {
		return nil, err
	}

	return res, nil
}

func TestSQLQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	testCases := []struct {
		name           string
		readRequest    prompb.ReadRequest
		expectResponse prompb.ReadResponse
		expectErr      error
	}{
		{
			name:        "empty request",
			readRequest: prompb.ReadRequest{},
			expectResponse: prompb.ReadResponse{
				Results: make([]*prompb.QueryResult, 0),
			},
		},
		{
			name: "empty response",
			readRequest: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  MetricNameLabelName,
								Value: "nonExistantMetric",
							},
						},
						StartTimestampMs: 1,
						EndTimestampMs:   3,
					},
				},
			},
			expectResponse: prompb.ReadResponse{
				Results: createQueryResult([]*prompb.TimeSeries{}),
			},
		},
		{
			name: "one matcher, exact metric",
			readRequest: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  MetricNameLabelName,
								Value: "firstMetric",
							},
						},
						StartTimestampMs: 1,
						EndTimestampMs:   3,
					},
				},
			},
			expectResponse: prompb.ReadResponse{
				Results: createQueryResult([]*prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: MetricNameLabelName, Value: "firstMetric"},
							{Name: "common", Value: "tag"},
							{Name: "empty", Value: ""},
							{Name: "foo", Value: "bar"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 1, Value: 0.1},
							{Timestamp: 2, Value: 0.2},
							{Timestamp: 3, Value: 0.3},
						},
					},
				}),
			},
		},
		{
			name: "one matcher, regex metric",
			readRequest: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_RE,
								Name:  MetricNameLabelName,
								Value: "first.*",
							},
						},
						StartTimestampMs: 1,
						EndTimestampMs:   3,
					},
				},
			},
			expectResponse: prompb.ReadResponse{
				Results: createQueryResult([]*prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: MetricNameLabelName, Value: "firstMetric"},
							{Name: "common", Value: "tag"},
							{Name: "empty", Value: ""},
							{Name: "foo", Value: "bar"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 1, Value: 0.1},
							{Timestamp: 2, Value: 0.2},
							{Timestamp: 3, Value: 0.3},
						},
					},
				}),
			},
		},
		{
			name: "one matcher, no exact metric",
			readRequest: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_NEQ,
								Name:  MetricNameLabelName,
								Value: "firstMetric",
							},
						},
						StartTimestampMs: 1,
						EndTimestampMs:   1,
					},
				},
			},
			expectResponse: prompb.ReadResponse{
				Results: createQueryResult([]*prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: MetricNameLabelName, Value: "secondMetric"},
							{Name: "common", Value: "tag"},
							{Name: "foo", Value: "baz"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 1, Value: 1.1},
						},
					},
				}),
			},
		},
		{
			name: "one matcher, not regex metric",
			readRequest: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_NRE,
								Name:  MetricNameLabelName,
								Value: "first.*",
							},
						},
						StartTimestampMs: 1,
						EndTimestampMs:   2,
					},
				},
			},
			expectResponse: prompb.ReadResponse{
				Results: createQueryResult([]*prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: MetricNameLabelName, Value: "secondMetric"},
							{Name: "common", Value: "tag"},
							{Name: "foo", Value: "baz"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 1, Value: 1.1},
							{Timestamp: 2, Value: 1.2},
						},
					},
				}),
			},
		},
		{
			name: "one matcher, both metrics",
			readRequest: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  "common",
								Value: "tag",
							},
						},
						StartTimestampMs: 2,
						EndTimestampMs:   3,
					},
				},
			},
			expectResponse: prompb.ReadResponse{
				Results: createQueryResult([]*prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: MetricNameLabelName, Value: "firstMetric"},
							{Name: "common", Value: "tag"},
							{Name: "empty", Value: ""},
							{Name: "foo", Value: "bar"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 2, Value: 0.2},
							{Timestamp: 3, Value: 0.3},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: MetricNameLabelName, Value: "secondMetric"},
							{Name: "common", Value: "tag"},
							{Name: "foo", Value: "baz"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 2, Value: 1.2},
							{Timestamp: 3, Value: 1.3},
						},
					},
				}),
			},
		},
		{
			name: "two matchers",
			readRequest: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  "common",
								Value: "tag",
							},
							{
								Type:  prompb.LabelMatcher_RE,
								Name:  MetricNameLabelName,
								Value: ".*Metric",
							},
						},
						StartTimestampMs: 2,
						EndTimestampMs:   3,
					},
				},
			},
			expectResponse: prompb.ReadResponse{
				Results: createQueryResult([]*prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: MetricNameLabelName, Value: "firstMetric"},
							{Name: "common", Value: "tag"},
							{Name: "empty", Value: ""},
							{Name: "foo", Value: "bar"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 2, Value: 0.2},
							{Timestamp: 3, Value: 0.3},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: MetricNameLabelName, Value: "secondMetric"},
							{Name: "common", Value: "tag"},
							{Name: "foo", Value: "baz"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 2, Value: 1.2},
							{Timestamp: 3, Value: 1.3},
						},
					},
				}),
			},
		},
		{
			name: "three matchers",
			readRequest: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  "common",
								Value: "tag",
							},
							{
								Type:  prompb.LabelMatcher_RE,
								Name:  "foo",
								Value: "bar|baz",
							},
							{
								Type:  prompb.LabelMatcher_NRE,
								Name:  MetricNameLabelName,
								Value: "non-existent",
							},
						},
						StartTimestampMs: 2,
						EndTimestampMs:   3,
					},
				},
			},
			expectResponse: prompb.ReadResponse{
				Results: createQueryResult([]*prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: MetricNameLabelName, Value: "firstMetric"},
							{Name: "common", Value: "tag"},
							{Name: "empty", Value: ""},
							{Name: "foo", Value: "bar"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 2, Value: 0.2},
							{Timestamp: 3, Value: 0.3},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: MetricNameLabelName, Value: "secondMetric"},
							{Name: "common", Value: "tag"},
							{Name: "foo", Value: "baz"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 2, Value: 1.2},
							{Timestamp: 3, Value: 1.3},
						},
					},
				}),
			},
		},
		{
			name: "one matcher, match empty value",
			readRequest: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  "empty",
								Value: "",
							},
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  "common",
								Value: "tag",
							},
						},
						StartTimestampMs: 1,
						EndTimestampMs:   3,
					},
				},
			},
			expectResponse: prompb.ReadResponse{
				Results: createQueryResult([]*prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: MetricNameLabelName, Value: "firstMetric"},
							{Name: "common", Value: "tag"},
							{Name: "empty", Value: ""},
							{Name: "foo", Value: "bar"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 1, Value: 0.1},
							{Timestamp: 2, Value: 0.2},
							{Timestamp: 3, Value: 0.3},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: MetricNameLabelName, Value: "secondMetric"},
							{Name: "common", Value: "tag"},
							{Name: "foo", Value: "baz"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 1, Value: 1.1},
							{Timestamp: 2, Value: 1.2},
							{Timestamp: 3, Value: 1.3},
						},
					},
				}),
			},
		},
		{
			name: "one matcher, regex match empty value",
			readRequest: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_RE,
								Name:  "empty",
								Value: ".*",
							},
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  "common",
								Value: "tag",
							},
						},
						StartTimestampMs: 1,
						EndTimestampMs:   3,
					},
				},
			},
			expectResponse: prompb.ReadResponse{
				Results: createQueryResult([]*prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: MetricNameLabelName, Value: "firstMetric"},
							{Name: "common", Value: "tag"},
							{Name: "empty", Value: ""},
							{Name: "foo", Value: "bar"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 1, Value: 0.1},
							{Timestamp: 2, Value: 0.2},
							{Timestamp: 3, Value: 0.3},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: MetricNameLabelName, Value: "secondMetric"},
							{Name: "common", Value: "tag"},
							{Name: "foo", Value: "baz"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 1, Value: 1.1},
							{Timestamp: 2, Value: 1.2},
							{Timestamp: 3, Value: 1.3},
						},
					},
				}),
			},
		},
		// https://github.com/timescale/promscale/issues/125
		{
			name: "min start and max end timestamps",
			readRequest: prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_RE,
								Name:  "empty",
								Value: ".*",
							},
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  "common",
								Value: "tag",
							},
						},
						// Prometheus setting start and end timestamp to min/max values when missing:
						// https://github.com/prometheus/prometheus/blob/master/web/api/v1/api.go#L555-L556
						StartTimestampMs: timestamp.FromTime(time.Unix(math.MinInt64/1000+62135596801, 0).UTC()),
						EndTimestampMs:   timestamp.FromTime(time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()),
					},
				},
			},
			expectResponse: prompb.ReadResponse{
				Results: createQueryResult([]*prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: MetricNameLabelName, Value: "firstMetric"},
							{Name: "common", Value: "tag"},
							{Name: "empty", Value: ""},
							{Name: "foo", Value: "bar"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 1, Value: 0.1},
							{Timestamp: 2, Value: 0.2},
							{Timestamp: 3, Value: 0.3},
							{Timestamp: 4, Value: 0.4},
							{Timestamp: 5, Value: 0.5},
						},
					},
					{
						Labels: []prompb.Label{
							{Name: MetricNameLabelName, Value: "secondMetric"},
							{Name: "common", Value: "tag"},
							{Name: "foo", Value: "baz"},
						},
						Samples: []prompb.Sample{
							{Timestamp: 1, Value: 1.1},
							{Timestamp: 2, Value: 1.2},
							{Timestamp: 3, Value: 1.3},
							{Timestamp: 4, Value: 1.4},
							{Timestamp: 5, Value: 1.5},
						},
					},
				}),
			},
		},
	}

	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		// Ingest test dataset.
		ingestQueryTestDataset(db, t, generateSmallTimeseries())
		// Getting a read-only connection to ensure read path is idempotent.
		readOnly := testhelpers.GetReadOnlyConnection(t, *testDatabase)
		defer readOnly.Close()

		var tester *testing.T
		var ok bool
		if tester, ok = t.(*testing.T); !ok {
			t.Fatalf("Cannot run test, not an instance of testing.T")
		}

		r := NewPgxReader(readOnly, nil, 100)
		for _, c := range testCases {
			tester.Run(c.name, func(t *testing.T) {
				resp, err := r.Read(&c.readRequest)

				if err != nil && err != c.expectErr {
					t.Fatalf("unexpected error returned:\ngot\n%s\nwanted\n%s", err, c.expectErr)
				}

				if !reflect.DeepEqual(resp, &c.expectResponse) {
					t.Fatalf("unexpected response:\ngot\n%+v\nwanted\n%+v", resp, &c.expectResponse)
				}

			})
		}
	})
}

func createQueryResult(ts []*prompb.TimeSeries) []*prompb.QueryResult {
	return []*prompb.QueryResult{
		{
			Timeseries: ts,
		},
	}
}

func ingestQueryTestDataset(db *pgxpool.Pool, t testing.TB, metrics []prompb.TimeSeries) {
	ingestor, err := NewPgxIngestor(db)
	if err != nil {
		t.Fatal(err)
	}
	cnt, err := ingestor.Ingest(copyMetrics(metrics), NewWriteRequest())

	if err != nil {
		t.Fatalf("unexpected error while ingesting test dataset: %s", err)
	}

	expectedCount := 0

	for _, ts := range metrics {
		expectedCount = expectedCount + len(ts.Samples)
	}

	if cnt != uint64(expectedCount) {
		t.Fatalf("wrong amount of metrics ingested: got %d, wanted %d", cnt, expectedCount)
	}
}

func TestPromQL(t *testing.T) {
	if testing.Short() || !*useDocker {
		t.Skip("skipping integration test")
	}

	promClient, err := NewPromClient(fmt.Sprintf("http://%s:%d/api/v1/read", testhelpers.PromHost, testhelpers.PromPort.Int()), 10*time.Second)

	if err != nil {
		t.Fatalf("unable to create read client for Prometheus: %s", err)
	}

	testCases := []struct {
		name        string
		readRequest *prompb.ReadRequest
	}{
		{
			name: "Simple metric name matcher",
			readRequest: &prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  MetricNameLabelName,
								Value: "metric_1",
							},
						},
						StartTimestampMs: 30000,
						EndTimestampMs:   31000,
					},
				},
			},
		},
		{
			name: "Regex metric name matcher",
			readRequest: &prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_RE,
								Name:  MetricNameLabelName,
								Value: "metric_.*",
							},
						},
						StartTimestampMs: 30000,
						EndTimestampMs:   1160000,
					},
				},
			},
		},
		{
			name: "Metrics without foo label or value empty",
			readRequest: &prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_RE,
								Name:  MetricNameLabelName,
								Value: "metric_.*",
							},
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  "foo",
								Value: "",
							},
						},
						StartTimestampMs: 30000,
						EndTimestampMs:   160000,
					},
				},
			},
		},
		{
			name: "Instance 1 or 2",
			readRequest: &prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_RE,
								Name:  "instance",
								Value: "(1|2)",
							},
						},
						StartTimestampMs: 70000,
						EndTimestampMs:   160000,
					},
				},
			},
		},
		{
			name: "Test match empty on EQ",
			readRequest: &prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  "foo",
								Value: "",
							},
						},
						StartTimestampMs: 90000,
						EndTimestampMs:   160000,
					},
				},
			},
		},
		{
			name: "Test match empty on NEQ",
			readRequest: &prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_NEQ,
								Name:  "foo",
								Value: "bar",
							},
						},
						StartTimestampMs: 90000,
						EndTimestampMs:   160000,
					},
				},
			},
		},
		{
			name: "Test match empty on RE",
			readRequest: &prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_RE,
								Name:  "foo",
								Value: "",
							},
						},
						StartTimestampMs: 90000,
						EndTimestampMs:   160000,
					},
				},
			},
		},
		{
			name: "Test match empty on NRE",
			readRequest: &prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_NRE,
								Name:  "foo",
								Value: "bar",
							},
						},
						StartTimestampMs: 90000,
						EndTimestampMs:   160000,
					},
				},
			},
		},
		{
			name: "Test error regex matcher",
			readRequest: &prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_RE,
								Name:  "foo",
								Value: "*",
							},
						},
						StartTimestampMs: 90000,
						EndTimestampMs:   160000,
					},
				},
			},
		},
		{
			name: "Complex query 1",
			readRequest: &prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_RE,
								Name:  "instance",
								Value: "(1|2)",
							},
							{
								Type:  prompb.LabelMatcher_NRE,
								Name:  "foo",
								Value: "ba.*",
							},
							{
								Type:  prompb.LabelMatcher_NEQ,
								Name:  MetricNameLabelName,
								Value: "metric_1",
							},
						},
						StartTimestampMs: 90000,
						EndTimestampMs:   160000,
					},
				},
			},
		},
		{
			name: "Complex query 2",
			readRequest: &prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_RE,
								Name:  "instance",
								Value: "(1|2)",
							},
							{
								Type:  prompb.LabelMatcher_NEQ,
								Name:  MetricNameLabelName,
								Value: "metric_1",
							},
						},
						StartTimestampMs: 9000,
						EndTimestampMs:   160000,
					},
				},
			},
		},
		{
			name: "Complex query 3",
			readRequest: &prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_RE,
								Name:  "instance",
								Value: "(1|2)",
							},
							{
								Type:  prompb.LabelMatcher_NEQ,
								Name:  "instance",
								Value: "3",
							},
							{
								Type:  prompb.LabelMatcher_RE,
								Name:  "foo",
								Value: ".*r",
							},
						},
						StartTimestampMs: 190000,
						EndTimestampMs:   260000,
					},
				},
			},
		},
		{
			name: "Complex query 4",
			readRequest: &prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_RE,
								Name:  "instance",
								Value: "(1|3)",
							},
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  "instance",
								Value: "1",
							},
							{
								Type:  prompb.LabelMatcher_NRE,
								Name:  MetricNameLabelName,
								Value: "metric_3",
							},
						},
						StartTimestampMs: 90000,
						EndTimestampMs:   160000,
					},
				},
			},
		},
		{
			name: "Empty results complex query",
			readRequest: &prompb.ReadRequest{
				Queries: []*prompb.Query{
					{
						Matchers: []*prompb.LabelMatcher{
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  MetricNameLabelName,
								Value: "metric_1",
							},
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  MetricNameLabelName,
								Value: "metric_2",
							},
							{
								Type:  prompb.LabelMatcher_EQ,
								Name:  MetricNameLabelName,
								Value: "metric_3",
							},
						},
						StartTimestampMs: 90000,
						EndTimestampMs:   160000,
					},
				},
			},
		},
	}

	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		// Ingest test dataset.
		ingestQueryTestDataset(db, t, generateLargeTimeseries())
		// Getting a read-only connection to ensure read path is idempotent.
		readOnly := testhelpers.GetReadOnlyConnection(t, *testDatabase)
		defer readOnly.Close()

		var tester *testing.T
		var ok bool
		if tester, ok = t.(*testing.T); !ok {
			t.Fatalf("Cannot run test, not an instance of testing.T")
			return
		}

		r := NewPgxReader(readOnly, nil, 100)
		for _, c := range testCases {
			tester.Run(c.name, func(t *testing.T) {
				connResp, connErr := r.Read(c.readRequest)
				promResp, promErr := promClient.Read(c.readRequest)

				// If a query returns an error on both sides, its considered an
				// expected result. The errors don't need to be identical.
				if connErr != nil && promErr != nil {
					return
				}

				if connErr != nil {
					t.Fatalf("unexpected error returned:\ngot\n%s\nwanted\nnil", connErr)
				}

				if promErr != nil {
					t.Fatalf("unexpected error returned:\ngot\n%v\nwanted\nnil", promErr)
				}

				// Length checking is for case when query returns an empty results,
				// DeepEqual check fails even though the values are the same.
				if !reflect.DeepEqual(connResp, promResp) &&
					(len(connResp.Results[0].Timeseries)+len(promResp.Results[0].Timeseries) > 0) {
					t.Errorf("unexpected response:\ngot\n%v\nwanted\n%v", connResp, promResp)
				}
			})
		}
	})
}

func generatePrometheusWALFile() (string, error) {
	tmpDir := ""

	if runtime.GOOS == "darwin" {
		// Docker on Mac lacks access to default os tmp dir - "/var/folders/random_number"
		// so switch to cross-user tmp dir
		tmpDir = "/tmp"
	}
	path, err := ioutil.TempDir(tmpDir, "prom_test_storage")
	if err != nil {
		return "", err
	}

	st, err := tsdb.Open(path, nil, nil, &tsdb.Options{
		RetentionDuration: 15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
		NoLockfile:        true,
	})

	if err != nil {
		return "", err
	}

	app := st.Appender(context.Background())

	tts := generateLargeTimeseries()
	if *extendedTest {
		tts = append(tts, generateRealTimeseries()...)
	}
	var ref *uint64

	for _, ts := range tts {
		ref = nil
		builder := labels.Builder{}

		for _, l := range ts.Labels {
			builder.Set(l.Name, l.Value)
		}

		labels := builder.Labels()

		for _, s := range ts.Samples {
			if ref == nil {
				tempRef, err := app.Add(labels, s.Timestamp, s.Value)
				if err != nil {
					return "", err
				}
				ref = &tempRef
				continue
			}

			err = app.AddFast(*ref, s.Timestamp, s.Value)

			if err != nil {
				return "", err
			}
		}
	}

	if err := app.Commit(); err != nil {
		return "", err
	}

	st.Close()

	return path, nil
}

func TestPushdown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	testCases := []struct {
		name    string
		query   string
		startMs int64
		endMs   int64
		stepMs  int64
		res     promql.Result
	}{
		{
			name:    "Simple metric name matcher",
			query:   `delta(metric_1{instance="1"}[5m])`,
			startMs: startTime + 300*1000,
			endMs:   startTime + 330*1000,
			stepMs:  30 * 1000,
			res: promql.Result{
				Value: promql.Matrix{promql.Series{
					Points: []promql.Point{{V: 20, T: startTime + 300000}, {V: 20, T: startTime + 330000}},
					Metric: labels.FromStrings("foo", "bar", "instance", "1", "aaa", "000")},
				},
			},
		},
		{
			name:  "Simple metric name matcher",
			query: `delta(metric_1{instance="1"}[5m])`,
			endMs: startTime + 300*1000,
			res: promql.Result{
				Value: promql.Vector{promql.Sample{
					Point:  promql.Point{V: 20, T: startTime + 300*1000},
					Metric: labels.FromStrings("foo", "bar", "instance", "1", "aaa", "000")},
				},
			},
		},
	}

	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		// Ingest test dataset.
		ingestQueryTestDataset(db, t, generateLargeTimeseries())
		// Getting a read-only connection to ensure read path is idempotent.
		readOnly := testhelpers.GetReadOnlyConnection(t, *testDatabase)
		defer readOnly.Close()

		var tester *testing.T
		var ok bool
		if tester, ok = t.(*testing.T); !ok {
			t.Fatalf("Cannot run test, not an instance of testing.T")
			return
		}

		r := NewPgxReader(readOnly, nil, 100)
		queryable := query.NewQueryable(r.GetQuerier())
		queryEngine := query.NewEngine(log.GetLogger(), time.Minute)

		for _, c := range testCases {
			tc := c
			tester.Run(c.name, func(t *testing.T) {
				var qry promql.Query
				var err error

				if tc.stepMs == 0 {
					qry, err = queryEngine.NewInstantQuery(queryable, c.query, model.Time(tc.endMs).Time())
				} else {
					qry, err = queryEngine.NewRangeQuery(queryable, tc.query, model.Time(tc.startMs).Time(), model.Time(tc.endMs).Time(), time.Duration(tc.stepMs)*time.Millisecond)
				}
				if err != nil {
					t.Fatal(err)
				}

				res := qry.Exec(context.Background())
				testutil.Equals(t, tc.res, *res)
			})
		}
	})
}
