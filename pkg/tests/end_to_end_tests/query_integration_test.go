// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

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
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	pgx "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgmodel/lreader"
	pgmodel "github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgmodel/querier"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/promql"
	"github.com/timescale/promscale/pkg/query"
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

func TestDroppedViewQuery(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		// Ingest test dataset.
		ingestQueryTestDataset(db, t, generateSmallTimeseries())
		// Drop the view.
		if _, err := db.Exec(context.Background(), `DROP VIEW prom_view.metric_view`); err != nil {
			t.Fatalf("unexpected error while dropping metric view: %s", err)
		}
		// Getting a read-only connection to ensure read path is idempotent.
		readOnly := testhelpers.GetReadOnlyConnection(t, *testDatabase)
		defer readOnly.Close()

		mCache := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
		lCache := clockcache.WithMax(100)
		dbConn := pgxconn.NewPgxConn(readOnly)
		labelsReader := lreader.NewLabelsReader(dbConn, lCache, noopReadAuthorizer)
		r := querier.NewQuerier(dbConn, mCache, labelsReader, nil, nil)
		_, err := r.RemoteReadQuerier().Query(&prompb.Query{
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  pgmodel.MetricNameLabelName,
					Value: "metric_view",
				},
			},
			StartTimestampMs: 1,
			EndTimestampMs:   3,
		})

		if err == nil {
			t.Fatalf("expected an error, got nil")
		}

		expectedMsg := fmt.Sprintf(errors.ErrTmplMissingUnderlyingRelation, "prom_view", "metric_view")
		require.Equal(t, expectedMsg, err.Error(), "unexpected error message")
	})
}

func TestSQLQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	testCases := []struct {
		name           string
		query          *prompb.Query
		expectResponse []*prompb.TimeSeries
		expectErr      error
	}{
		{
			name:      "empty request",
			query:     &prompb.Query{},
			expectErr: fmt.Errorf("get evaluation metadata: building multiple metric clauses: no clauses generated"),
		},
		{
			name: "empty response",
			query: &prompb.Query{
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  pgmodel.MetricNameLabelName,
						Value: "nonExistantMetric",
					},
				},
				StartTimestampMs: 1,
				EndTimestampMs:   3,
			},
			expectResponse: []*prompb.TimeSeries{},
		},
		{
			name: "one matcher, exact metric",
			query: &prompb.Query{
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  pgmodel.MetricNameLabelName,
						Value: "firstMetric",
					},
				},
				StartTimestampMs: 1,
				EndTimestampMs:   3,
			},
			expectResponse: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: pgmodel.MetricNameLabelName, Value: "firstMetric"},
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
			},
		},
		{
			name: "one matcher, view metric",
			query: &prompb.Query{
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  pgmodel.MetricNameLabelName,
						Value: "metric_view",
					},
				},
				StartTimestampMs: 1,
				EndTimestampMs:   3,
			},
			expectResponse: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: pgmodel.MetricNameLabelName, Value: "metric_view"},
						{Name: pgmodel.SchemaNameLabelName, Value: "prom_view"},
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
			},
		},
		{
			name: "two matcher, duplicate name",
			query: &prompb.Query{
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  pgmodel.MetricNameLabelName,
						Value: "firstMetric",
					},
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  pgmodel.MetricNameLabelName,
						Value: "firstMetric",
					},
				},
				StartTimestampMs: 1,
				EndTimestampMs:   3,
			},
			expectResponse: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: pgmodel.MetricNameLabelName, Value: "firstMetric"},
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
			},
		},
		{
			name: "two matcher, contradictory name",
			query: &prompb.Query{
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  pgmodel.MetricNameLabelName,
						Value: "firstMetric",
					},
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  pgmodel.MetricNameLabelName,
						Value: "secondMetric",
					},
				},
				StartTimestampMs: 1,
				EndTimestampMs:   3,
			},
			expectResponse: []*prompb.TimeSeries{},
		},
		{
			name: "two matcher, non-contradictory name",
			query: &prompb.Query{
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  pgmodel.MetricNameLabelName,
						Value: "firstMetric",
					},
					{
						Type:  prompb.LabelMatcher_NEQ,
						Name:  pgmodel.MetricNameLabelName,
						Value: "secondMetric",
					},
				},
				StartTimestampMs: 1,
				EndTimestampMs:   3,
			},
			expectResponse: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: pgmodel.MetricNameLabelName, Value: "firstMetric"},
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
			},
		},
		{
			name: "one matcher, regex metric",
			query: &prompb.Query{
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_RE,
						Name:  pgmodel.MetricNameLabelName,
						Value: "first.*",
					},
				},
				StartTimestampMs: 1,
				EndTimestampMs:   3,
			},
			expectResponse: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: pgmodel.MetricNameLabelName, Value: "firstMetric"},
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
			},
		},
		{
			name: "one matcher, no exact metric",
			query: &prompb.Query{
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_NEQ,
						Name:  pgmodel.MetricNameLabelName,
						Value: "firstMetric",
					},
				},
				StartTimestampMs: 1,
				EndTimestampMs:   1,
			},
			expectResponse: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: pgmodel.MetricNameLabelName, Value: "secondMetric"},
						{Name: "common", Value: "tag"},
						{Name: "foo", Value: "baz"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 1.1},
					},
				},
			},
		},
		{
			name: "one matcher, not regex metric",
			query: &prompb.Query{
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_NRE,
						Name:  pgmodel.MetricNameLabelName,
						Value: "first.*",
					},
				},
				StartTimestampMs: 1,
				EndTimestampMs:   2,
			},
			expectResponse: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: pgmodel.MetricNameLabelName, Value: "secondMetric"},
						{Name: "common", Value: "tag"},
						{Name: "foo", Value: "baz"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 1.1},
						{Timestamp: 2, Value: 1.2},
					},
				},
			},
		},
		{
			name: "one matcher, both metrics",
			query: &prompb.Query{
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
			expectResponse: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: pgmodel.MetricNameLabelName, Value: "firstMetric"},
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
						{Name: pgmodel.MetricNameLabelName, Value: "secondMetric"},
						{Name: "common", Value: "tag"},
						{Name: "foo", Value: "baz"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 2, Value: 1.2},
						{Timestamp: 3, Value: 1.3},
					},
				},
			},
		},
		{
			name: "two matchers",
			query: &prompb.Query{
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  "common",
						Value: "tag",
					},
					{
						Type:  prompb.LabelMatcher_RE,
						Name:  pgmodel.MetricNameLabelName,
						Value: ".*Metric",
					},
				},
				StartTimestampMs: 2,
				EndTimestampMs:   3,
			},
			expectResponse: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: pgmodel.MetricNameLabelName, Value: "firstMetric"},
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
						{Name: pgmodel.MetricNameLabelName, Value: "secondMetric"},
						{Name: "common", Value: "tag"},
						{Name: "foo", Value: "baz"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 2, Value: 1.2},
						{Timestamp: 3, Value: 1.3},
					},
				},
			},
		},
		{
			name: "three matchers",
			query: &prompb.Query{
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
						Name:  pgmodel.MetricNameLabelName,
						Value: "non-existent",
					},
				},
				StartTimestampMs: 2,
				EndTimestampMs:   3,
			},
			expectResponse: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: pgmodel.MetricNameLabelName, Value: "firstMetric"},
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
						{Name: pgmodel.MetricNameLabelName, Value: "secondMetric"},
						{Name: "common", Value: "tag"},
						{Name: "foo", Value: "baz"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 2, Value: 1.2},
						{Timestamp: 3, Value: 1.3},
					},
				},
			},
		},
		{
			name: "one matcher, match empty value",
			query: &prompb.Query{
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
			expectResponse: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: pgmodel.MetricNameLabelName, Value: "firstMetric"},
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
						{Name: pgmodel.MetricNameLabelName, Value: "secondMetric"},
						{Name: "common", Value: "tag"},
						{Name: "foo", Value: "baz"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 1.1},
						{Timestamp: 2, Value: 1.2},
						{Timestamp: 3, Value: 1.3},
					},
				},
			},
		},
		{
			name: "one matcher, regex match empty value",
			query: &prompb.Query{
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
			expectResponse: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: pgmodel.MetricNameLabelName, Value: "firstMetric"},
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
						{Name: pgmodel.MetricNameLabelName, Value: "secondMetric"},
						{Name: "common", Value: "tag"},
						{Name: "foo", Value: "baz"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 1.1},
						{Timestamp: 2, Value: 1.2},
						{Timestamp: 3, Value: 1.3},
					},
				},
			},
		},
		// https://github.com/timescale/promscale/issues/125
		{
			name: "min start and max end timestamps",
			query: &prompb.Query{
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
			expectResponse: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: pgmodel.MetricNameLabelName, Value: "firstMetric"},
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
						{Name: pgmodel.MetricNameLabelName, Value: "secondMetric"},
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

		mCache := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
		lCache := clockcache.WithMax(100)
		dbConn := pgxconn.NewPgxConn(readOnly)
		labelsReader := lreader.NewLabelsReader(dbConn, lCache, noopReadAuthorizer)
		r := querier.NewQuerier(dbConn, mCache, labelsReader, nil, nil)
		for _, c := range testCases {
			tester.Run(c.name, func(t *testing.T) {
				resp, err := r.RemoteReadQuerier().Query(c.query)

				if err != nil && (c.expectErr == nil || err.Error() != c.expectErr.Error()) {
					t.Fatalf("unexpected error returned:\ngot\n%s\nwanted\n%s", err, c.expectErr)
				}

				if !reflect.DeepEqual(resp, c.expectResponse) {
					t.Fatalf("unexpected response:\ngot\n%+v\nwanted\n%+v", resp, c.expectResponse)
				}

			})
		}
	})
}

func ingestQueryTestDataset(db *pgxpool.Pool, t testing.TB, metrics []prompb.TimeSeries) {
	ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ingestor.Close()
	cnt, _, err := ingestor.Ingest(context.Background(), newWriteRequestWithTs(copyMetrics(metrics)))

	if err != nil {
		t.Fatalf("unexpected error while ingesting test dataset: %s", err)
	}

	expectedCount := 0
	firstMetricName := ""

	for _, ts := range metrics {
		expectedCount = expectedCount + len(ts.Samples) + len(ts.Exemplars)

		if firstMetricName == "" {
			for _, l := range ts.Labels {
				if l.Name == labels.MetricName {
					firstMetricName = l.Value
					break
				}
			}
		}
	}

	if cnt != uint64(expectedCount) {
		t.Fatalf("wrong amount of metrics ingested: got %d, wanted %d", cnt, expectedCount)
	}

	// Create view and register as metric view based on the first metric.
	if firstMetricName == "" {
		t.Fatal("unexpected error while ingesting test dataset: missing first metric name")
	}

	row := db.QueryRow(context.Background(), "SELECT table_name FROM _prom_catalog.get_metric_table_name_if_exists('prom_data', $1)", firstMetricName)

	if err = row.Scan(&firstMetricName); err != nil {
		t.Fatalf("unexpected error while creating metric view: couldn't get first metric table name: %v", err)
	}

	createMetricView(db, t, "prom_view", "metric_view", firstMetricName)
}

func createMetricView(db *pgxpool.Pool, t testing.TB, schemaName, viewName, metricName string) {
	if _, err := db.Exec(context.Background(), "CALL _prom_catalog.finalize_metric_creation()"); err != nil {
		t.Fatalf("unexpected error while ingesting test dataset: %s", err)
	}
	if _, err := db.Exec(context.Background(), fmt.Sprintf("CREATE SCHEMA %s", schemaName)); err != nil {
		t.Fatalf("unexpected error while creating view schema: %s", err)
	}
	if _, err := db.Exec(context.Background(), fmt.Sprintf(`CREATE VIEW "%s"."%s" AS SELECT * FROM prom_data."%s"`, schemaName, viewName, metricName)); err != nil {
		t.Fatalf("unexpected error while creating metric view: %s", err)
	}
	if _, err := db.Exec(context.Background(), fmt.Sprintf("SELECT prom_api.register_metric_view('%s', '%s')", schemaName, viewName)); err != nil {
		t.Fatalf("unexpected error while registering metric view: %s", err)
	}
}

func TestPromQL(t *testing.T) {
	if testing.Short() || !*useDocker {
		t.Skip("skipping integration test")
	}

	promClient, err := NewPromClient(fmt.Sprintf("http://%s:%d/api/v1/read", promHost, promPort.Int()), 10*time.Second)

	if err != nil {
		t.Fatalf("unable to create read client for Prometheus: %s", err)
	}

	testCases := []struct {
		name  string
		query *prompb.Query
	}{
		{
			name: "Simple metric name matcher",
			query: &prompb.Query{
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  pgmodel.MetricNameLabelName,
						Value: "metric_1",
					},
				},
				StartTimestampMs: 30000,
				EndTimestampMs:   31000,
			},
		},
		{
			name: "Simple metric name matcher, capital letter metric name",
			query: &prompb.Query{
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  pgmodel.MetricNameLabelName,
						Value: "METRIC_4",
					},
				},
				StartTimestampMs: 30000,
				EndTimestampMs:   31000,
			},
		},
		{
			name: "Regex metric name matcher",
			query: &prompb.Query{
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_RE,
						Name:  pgmodel.MetricNameLabelName,
						Value: "metric_.*",
					},
				},
				StartTimestampMs: 30000,
				EndTimestampMs:   1160000,
			},
		},
		{
			name: "Regex metric name matcher RE2",
			query: &prompb.Query{
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_RE,
						Name:  pgmodel.MetricNameLabelName,
						Value: "((?i)MeTrIc)_4",
					},
				},
				StartTimestampMs: 30000,
				EndTimestampMs:   1160000,
			},
		},
		{
			name: "Not regex metric name matcher RE2",
			query: &prompb.Query{
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_NRE,
						Name:  pgmodel.MetricNameLabelName,
						Value: "((?i)mEtRiC)_4",
					},
					{
						Type:  prompb.LabelMatcher_RE,
						Name:  "instance",
						Value: "(1|2)",
					},
				},
				StartTimestampMs: 30000,
				EndTimestampMs:   1160000,
			},
		},
		{
			name: "Metrics without foo label or value empty",
			query: &prompb.Query{
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_RE,
						Name:  pgmodel.MetricNameLabelName,
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
		{
			name: "Instance 1 or 2",
			query: &prompb.Query{
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
		{
			name: "Test match empty on EQ",
			query: &prompb.Query{
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
		{
			name: "Test match empty on NEQ",
			query: &prompb.Query{
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
		{
			name: "Test match empty on RE",
			query: &prompb.Query{
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
		{
			name: "Test match empty on NRE",
			query: &prompb.Query{
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
		{
			name: "Test error regex matcher",
			query: &prompb.Query{
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
		{
			name: "Complex query 1",
			query: &prompb.Query{
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
						Name:  pgmodel.MetricNameLabelName,
						Value: "metric_1",
					},
				},
				StartTimestampMs: 90000,
				EndTimestampMs:   160000,
			},
		},
		{
			name: "Complex query 2",
			query: &prompb.Query{
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_RE,
						Name:  "instance",
						Value: "(1|2)",
					},
					{
						Type:  prompb.LabelMatcher_NEQ,
						Name:  pgmodel.MetricNameLabelName,
						Value: "metric_1",
					},
				},
				StartTimestampMs: 9000,
				EndTimestampMs:   160000,
			},
		},
		{
			name: "Complex query 3",
			query: &prompb.Query{
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
		{
			name: "Complex query 4",
			query: &prompb.Query{
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
						Name:  pgmodel.MetricNameLabelName,
						Value: "metric_3",
					},
				},
				StartTimestampMs: 90000,
				EndTimestampMs:   160000,
			},
		},
		{
			name: "Empty results complex query",
			query: &prompb.Query{
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  pgmodel.MetricNameLabelName,
						Value: "metric_1",
					},
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  pgmodel.MetricNameLabelName,
						Value: "metric_2",
					},
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  pgmodel.MetricNameLabelName,
						Value: "metric_3",
					},
				},
				StartTimestampMs: 90000,
				EndTimestampMs:   160000,
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

		mCache := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
		lCache := clockcache.WithMax(100)
		dbConn := pgxconn.NewPgxConn(readOnly)
		labelsReader := lreader.NewLabelsReader(dbConn, lCache, noopReadAuthorizer)
		r := querier.NewQuerier(dbConn, mCache, labelsReader, nil, nil)
		for _, c := range testCases {
			tester.Run(c.name, func(t *testing.T) {
				connResp, connErr := r.RemoteReadQuerier().Query(c.query)
				promResp, promErr := promClient.Read(&prompb.ReadRequest{
					Queries: []*prompb.Query{c.query},
				})

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
				if !reflect.DeepEqual(connResp, promResp.Results[0].Timeseries) &&
					len(promResp.Results[0].Timeseries)+len(connResp) > 0 {
					t.Errorf("unexpected response:\ngot\n%v\nwanted\n%v", connResp, promResp)
				}
			})
		}
	})
}

func TestMetricNameResolutionFromMultipleSchemas(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		// Ingest test dataset.
		ingestQueryTestDataset(db, t, generateLargeTimeseries())
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()

		var tableSchema, tableName string
		// Fetch metric view with specific schema name.
		row := db.QueryRow(context.Background(), "SELECT table_schema, table_name FROM _prom_catalog.get_metric_table_name_if_exists('prom_view', 'metric_view')")

		if err = row.Scan(&tableSchema, &tableName); err != nil {
			t.Fatalf("unexpected error fetching view schema and name: %v", err)
		}

		if tableSchema != "prom_view" || tableName != "metric_view" {
			t.Fatalf("unexpected view schema and name: got (%s, %s) wanted (prom_view, metric_view)", tableSchema, tableName)
		}

		// Fetch metric view with empty schema name.
		row = db.QueryRow(context.Background(), "SELECT table_schema, table_name FROM _prom_catalog.get_metric_table_name_if_exists('', 'metric_view')")

		if err = row.Scan(&tableSchema, &tableName); err != nil {
			t.Fatalf("unexpected error fetching view schema and name: %v", err)
		}

		if tableSchema != "prom_view" || tableName != "metric_view" {
			t.Fatalf("unexpected view schema and name: got (%s, %s) wanted (prom_view, metric_view)", tableSchema, tableName)
		}

		// Add another metric view in a different schema
		createMetricView(db, t, "view_schema", "metric_view", "metric_1")

		// Fetch metric view with empty schema name should return an error since there are multiple metric views in different schemas with the same name..
		row = db.QueryRow(context.Background(), "SELECT table_schema, table_name FROM _prom_catalog.get_metric_table_name_if_exists('', 'metric_view')")
		if err = row.Scan(&tableSchema, &tableName); err == nil {
			t.Fatalf("Expected error to occur because of multiple views in different non-default schemas: got %s %s", tableSchema, tableName)
		}

		// Fetch metric view with new schema name.
		row = db.QueryRow(context.Background(), "SELECT table_schema, table_name FROM _prom_catalog.get_metric_table_name_if_exists('view_schema', 'metric_view')")

		if err = row.Scan(&tableSchema, &tableName); err != nil {
			t.Fatalf("unexpected error fetching view schema and name: %v", err)
		}

		if tableSchema != "view_schema" || tableName != "metric_view" {
			t.Fatalf("unexpected view schema and name: got (%s, %s) wanted (view_schema, metric_view)", tableSchema, tableName)
		}

		// Ingest metric with same name as metric view.
		_, _, err = ingestor.Ingest(context.Background(), newWriteRequestWithTs([]prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: pgmodel.MetricNameLabelName, Value: "metric_view"},
					{Name: "commonkey", Value: "test"},
					{Name: "key1", Value: "test"},
					{Name: "key2", Value: "val1"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.1},
				},
			},
		}))

		if err != nil {
			t.Fatalf("got an unexpected error %v", err)
		}

		// Fetch metric view with specific schema name.
		row = db.QueryRow(context.Background(), "SELECT table_schema, table_name FROM _prom_catalog.get_metric_table_name_if_exists('prom_data', 'metric_view')")
		if err = row.Scan(&tableSchema, &tableName); err != nil {
			t.Fatalf("unexpected error fetching view schema and name: %v", err)
		}

		if tableSchema != "prom_data" || tableName != "metric_view" {
			t.Fatalf("unexpected view schema and name: got (%s, %s) wanted (prom_data, metric_view)", tableSchema, tableName)
		}

		// Fetch metric view with empty schema name should return the metric from default data schema.
		row = db.QueryRow(context.Background(), "SELECT table_schema, table_name FROM _prom_catalog.get_metric_table_name_if_exists('', 'metric_view')")
		if err = row.Scan(&tableSchema, &tableName); err != nil {
			t.Fatalf("unexpected error fetching view schema and name: %v", err)
		}

		if tableSchema != "prom_data" || tableName != "metric_view" {
			t.Fatalf("unexpected view schema and name: got (%s, %s) wanted (prom_data, metric_view)", tableSchema, tableName)
		}

		// Fetch non-existant metric name
		row = db.QueryRow(context.Background(), "SELECT table_schema, table_name FROM _prom_catalog.get_metric_table_name_if_exists('', 'dummy_metric')")
		if err = row.Scan(&tableSchema, &tableName); err != pgx.ErrNoRows {
			t.Fatalf("unexpected error fetching view schema and name: %v", err)
		}
	})
}

func TestPushdownDelta(t *testing.T) {
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
		{
			name:  "View metric matcher which matches metric_1",
			query: `delta(metric_view{instance="1"}[5m])`,
			endMs: startTime + 300*1000,
			res: promql.Result{
				Value: promql.Vector{promql.Sample{
					Point:  promql.Point{V: 20, T: startTime + 300*1000},
					Metric: labels.FromStrings(pgmodel.SchemaNameLabelName, "prom_view", "foo", "bar", "instance", "1", "aaa", "000")},
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

		mCache := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
		lCache := clockcache.WithMax(100)
		dbConn := pgxconn.NewPgxConn(readOnly)
		labelsReader := lreader.NewLabelsReader(dbConn, lCache, noopReadAuthorizer)
		r := querier.NewQuerier(dbConn, mCache, labelsReader, nil, nil)
		queryable := query.NewQueryable(r, labelsReader)
		queryEngine, err := query.NewEngine(log.GetLogger(), time.Minute, time.Minute*5, time.Minute, 50000000, nil)
		if err != nil {
			t.Fatal(err)
		}

		for _, c := range testCases {
			tc := c
			tester.Run(c.name, func(t *testing.T) {
				var qry promql.Query
				var err error

				if tc.stepMs == 0 {
					qry, err = queryEngine.NewInstantQuery(queryable, nil, c.query, model.Time(tc.endMs).Time())
				} else {
					qry, err = queryEngine.NewRangeQuery(queryable, nil, tc.query, model.Time(tc.startMs).Time(), model.Time(tc.endMs).Time(), time.Duration(tc.stepMs)*time.Millisecond)
				}
				if err != nil {
					t.Fatal(err)
				}

				res := qry.Exec(context.Background())
				require.Equal(t, tc.res, *res)
			})
		}
	})
}

func TestPushdownVecSel(t *testing.T) {
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
			query:   `metric_1{instance="1"}`,
			startMs: startTime + 300*1000,
			endMs:   startTime + 330*1000,
			stepMs:  30 * 1000,
			res: promql.Result{
				Value: promql.Matrix{promql.Series{
					Points: []promql.Point{{V: 20, T: startTime + 300000}, {V: 22, T: startTime + 330000}},
					Metric: labels.FromStrings("__name__", "metric_1", "foo", "bar", "instance", "1", "aaa", "000")},
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

		mCache := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
		lCache := clockcache.WithMax(100)
		dbConn := pgxconn.NewPgxConn(readOnly)
		labelsReader := lreader.NewLabelsReader(dbConn, lCache, noopReadAuthorizer)
		r := querier.NewQuerier(dbConn, mCache, labelsReader, nil, nil)
		queryable := query.NewQueryable(r, labelsReader)
		queryEngine, err := query.NewEngine(log.GetLogger(), time.Minute, time.Minute*5, time.Minute, 50000000, nil)
		if err != nil {
			t.Fatal(err)
		}

		for _, c := range testCases {
			tc := c
			tester.Run(c.name, func(t *testing.T) {
				var qry promql.Query
				var err error

				if tc.stepMs == 0 {
					qry, err = queryEngine.NewInstantQuery(queryable, nil, c.query, model.Time(tc.endMs).Time())
				} else {
					qry, err = queryEngine.NewRangeQuery(queryable, nil, tc.query, model.Time(tc.startMs).Time(), model.Time(tc.endMs).Time(), time.Duration(tc.stepMs)*time.Millisecond)
				}
				if err != nil {
					t.Fatal(err)
				}

				res := qry.Exec(context.Background())
				require.Equal(t, tc.res, *res)
			})
		}
	})
}
