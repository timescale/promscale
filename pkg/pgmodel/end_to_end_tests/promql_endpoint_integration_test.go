package end_to_end_tests

import (
	"encoding/json"
	"fmt"
	"github.com/prometheus/common/route"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/common/model"
	"github.com/timescale/timescale-prometheus/pkg/api"
	"github.com/timescale/timescale-prometheus/pkg/internal/testhelpers"
	"github.com/timescale/timescale-prometheus/pkg/log"
	"github.com/timescale/timescale-prometheus/pkg/pgmodel"
	"github.com/timescale/timescale-prometheus/pkg/query"
)

type queryResponse struct {
	Status   string    `json:"status"`
	Data     queryData `json:"data,omitempty"`
	Warnings []string  `json:"warnings,omitempty"`
}

type queryData struct {
	ResultType string  `json:"resultType"`
	Result     samples `json:"result"`
}

type sample struct {
	Metric model.Metric       `json:"metric"`
	Values []model.SamplePair `json:"values"`
	Value  model.SamplePair   `json:"value"`
}

type labelsResponse struct {
	Status string
	Data   []string
}

func genInstantRequest(apiURL, query string, start time.Time) (*http.Request, error) {
	u, err := url.Parse(fmt.Sprintf("%s/query", apiURL))

	if err != nil {
		return nil, err
	}

	val := url.Values{}

	val.Add("query", query)
	val.Add("time", fmt.Sprintf("%d", start.Unix()))

	u.RawQuery = val.Encode()

	return http.NewRequest(
		"GET",
		u.String(),
		nil,
	)
}

func genRangeRequest(apiURL, query string, start, end time.Time, step time.Duration) (*http.Request, error) {
	u, err := url.Parse(fmt.Sprintf("%s/query_range", apiURL))

	if err != nil {
		return nil, err
	}

	val := url.Values{}

	val.Add("query", query)
	val.Add("start", fmt.Sprintf("%d", start.Unix()))
	val.Add("end", fmt.Sprintf("%d", end.Unix()))
	val.Add("step", fmt.Sprintf("%f", step.Seconds()))

	u.RawQuery = val.Encode()

	return http.NewRequest(
		"GET",
		u.String(),
		nil,
	)
}

func getLabelsRequest(apiUrl string) (*http.Request, error) {
	u, err := url.Parse(fmt.Sprintf("%s/labels", apiUrl))

	if err != nil {
		return nil, err
	}

	return http.NewRequest(
		"GET",
		u.String(),
		nil,
	)
}

func getLabelNamesRequest(apiUrl string) (*http.Request, error) {
	u, err := url.Parse(fmt.Sprintf("%s/labels", apiUrl))

	if err != nil {
		return nil, err
	}

	return http.NewRequest(
		"GET",
		u.String(),
		nil,
	)
}

func getLabelValuesRequest(apiUrl string, labelName string) (*http.Request, error) {
	u, err := url.Parse(fmt.Sprintf("%s/label/%s/values", apiUrl, labelName))

	if err != nil {
		return nil, err
	}

	return http.NewRequest(
		"GET",
		u.String(),
		nil,
	)
}

type samples []sample

func (s samples) Len() int           { return len(s) }
func (s samples) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s samples) Less(i, j int) bool { return s[i].Metric.Before(s[j].Metric) }

func TestPromQLQueryEndpoint(t *testing.T) {
	if testing.Short() || !*useDocker {
		t.Skip("skipping integration test")
	}

	steps := []time.Duration{10 * time.Second, 30 * time.Second, time.Minute, 5 * time.Minute, 30 * time.Minute}

	testCases := []struct {
		name  string
		query string
	}{
		{
			name:  "basic query",
			query: "metric_1",
		},
		{
			name:  "basic query, not regex match metric name",
			query: `{__name__!~".*_1", instance="1"}`,
		},
		{
			name:  "basic query, regex match metric name",
			query: `{__name__=~"metric_.*"}`,
		},
		{
			name:  "basic query, regex no wildcards",
			query: `{__name__=~"metric_1"}`,
		},
		{
			name:  "basic query, no metric name matchers",
			query: `{instance="1", foo=""}`,
		},
		{
			name:  "basic query, multiple matchers",
			query: `{__name__!="metric_1", instance="1"}`,
		},
		{
			name:  "basic query, non-existant metric",
			query: `nonexistant_metric_name`,
		},
		{
			name:  "basic query, with offset",
			query: `metric_3 offset 5m`,
		},
		{
			name:  "basic aggregator",
			query: `sum (metric_3)`,
		},
		{
			name:  "aggregator by empty",
			query: `avg by() (metric_2)`,
		},
		{
			name:  "aggregator by instance",
			query: `max by(instance) (metric_1)`,
		},
		{
			name:  "aggregator by instance and foo",
			query: `min by(instance, foo) (metric_3)`,
		},
		{
			name:  "aggregator by non-existant",
			query: `count by(nonexistant) (metric_2)`,
		},
		{
			name:  "aggregator without empty",
			query: `avg without() (metric_2)`,
		},
		{
			name:  "aggregator without instance",
			query: `max without(instance) (metric_1)`,
		},
		{
			name:  "aggregator without instance and foo",
			query: `min without(instance, foo) (metric_3)`,
		},
		{
			name:  "aggregator without non-existant",
			query: `count without(nonexistant) (metric_2)`,
		},
		{
			name:  "topk",
			query: `topk (3, metric_2)`,
		},
		{
			name:  "bottomk",
			query: `bottomk (3, metric_1)`,
		},
		{
			name:  "topk by instance",
			query: `topk by(instance) (2, metric_3)`,
		},
		{
			name:  "quantile 0.5",
			query: `quantile(0.5, metric_1)`,
		},
		{
			name:  "quantile 0.1",
			query: `quantile(0.1, metric_2)`,
		},
		{
			name:  "quantile 0.95",
			query: `quantile(0.95, metric_3)`,
		},
		{
			name:  "sum_over_time",
			query: `sum_over_time(metric_1[5m])`,
		},
		{
			name:  "count_over_time",
			query: `count_over_time(metric_2[5m])`,
		},
		{
			name:  "avg_over_time",
			query: `avg_over_time(metric_3[5m])`,
		},
		{
			name:  "min_over_time",
			query: `min_over_time(metric_1[5m])`,
		},
		{
			name:  "max_over_time",
			query: `max_over_time(metric_2[5m])`,
		},
		{
			name:  "stddev_over_time",
			query: `stddev_over_time(metric_2[5m])`,
		},
		{
			name:  "delta",
			query: `delta(metric_3[5m])`,
		},
		{
			name:  "increase",
			query: `increase(metric_1[5m])`,
		},
		{
			name:  "rate",
			query: `rate(metric_2[5m])`,
		},
		{
			name:  "resets",
			query: `resets(metric_3[5m])`,
		},
		{
			name:  "changes",
			query: `changes(metric_1[5m])`,
		},
		{
			name:  "idelta",
			query: `idelta(metric_2[5m])`,
		},
		{
			name:  "predict_linear",
			query: `predict_linear(metric_3[5m], 100)`,
		},
		{
			name:  "deriv",
			query: `deriv(metric_1[5m])`,
		},
		{
			name:  "timestamp",
			query: `timestamp(metric_2)`,
		},
		{
			name:  "timestamp timestamp",
			query: `timestamp(timestamp(metric_2))`,
		},
		{
			name:  "vector",
			query: `vector(1)`,
		},
		{
			name:  "vector time",
			query: `vector(time())`,
		},
		{
			name:  "histogram quantile non-existent",
			query: `histogram_quantile(0.9, nonexistent_metric)`,
		},
		{
			name:  "histogram quantile complex",
			query: `histogram_quantile(0.5, rate(metric_1[1m]))`,
		},
		{
			name:  "complex query 1",
			query: `sum by(instance) (metric_1) + on(foo) group_left(instance) metric_2`,
		},
		{
			name:  "complex query 2",
			query: `max_over_time((time() - max(metric_3) < 1000)[5m:10s] offset 5m)`,
		},
		{
			name:  "complex query 3",
			query: `holt_winters(metric_1[10m], 0.1, 0.5)`,
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

		r := pgmodel.NewPgxReader(readOnly, nil)
		queryable := query.NewQueryable(r.GetQuerier())
		queryEngine := query.NewEngine(log.GetLogger(), time.Minute)

		instantQuery := api.Query(queryEngine, queryable)
		rangeQuery := api.QueryRange(queryEngine, queryable)

		apiURL := fmt.Sprintf("http://%s:%d/api/v1", testhelpers.PromHost, testhelpers.PromPort.Int())
		client := &http.Client{Timeout: 10 * time.Second}

		start := time.Unix(startTime/1000, 0)
		end := time.Unix(endTime/1000, 0)
		var (
			req *http.Request
			err error
		)
		for _, c := range testCases {
			req, err = genInstantRequest(apiURL, c.query, start)
			if err != nil {
				t.Fatalf("unable to create PromQL query request: %s", err)
			}
			testMethod := testRequest(req, instantQuery, client, queryResultComparator)
			tester.Run(fmt.Sprintf("%s (instant query)", c.name), testMethod)

			for _, step := range steps {
				req, err = genRangeRequest(apiURL, c.query, start, end, step)
				if err != nil {
					t.Fatalf("unable to create PromQL range query request: %s", err)
				}
				testMethod := testRequest(req, rangeQuery, client, queryResultComparator)
				tester.Run(fmt.Sprintf("%s (range query, step size: %s)", c.name, step.String()), testMethod)
			}
		}
	})
}

func TestPromQLLabelEndpoints(t *testing.T) {
	if testing.Short() || !*useDocker {
		t.Skip("skipping integration test")
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

		r := pgmodel.NewPgxReader(readOnly, nil)
		queryable := query.NewQueryable(r.GetQuerier())

		labelNamesHandler := api.Labels(queryable)
		labelValuesHandler := api.LabelValues(queryable)
		router := route.New()
		router.Get("/api/v1/label/:name/values", labelValuesHandler.ServeHTTP)
		router.Get("/api/v1/labels", labelNamesHandler.ServeHTTP)
		apiURL := fmt.Sprintf("http://%s:%d/api/v1", testhelpers.PromHost, testhelpers.PromPort.Int())
		client := &http.Client{Timeout: 10 * time.Second}

		var (
			req *http.Request
			err error
		)
		req, err = getLabelNamesRequest(apiURL)
		if err != nil {
			t.Fatalf("unable to create PromQL label names request: %v", err)
		}

		testMethod := testRequest(req, router, client, labelsResultComparator)
		tester.Run("get label names", testMethod)

		labelNames, err := r.GetQuerier().LabelNames()
		if err != nil {
			t.Fatalf("could not get label names from querier")
		}
		for _, label := range labelNames {
			req, err = getLabelValuesRequest(apiURL, label)
			if err != nil {
				t.Fatalf("unable to create PromQL label values request: %v", err)
			}
			testMethod := testRequest(req, router, client, labelsResultComparator)
			tester.Run(fmt.Sprintf("get label values for %s", label), testMethod)
		}
	})
}

type resultComparator func(promContent []byte, tsContent []byte) error

func queryResultComparator(promContent []byte, tsContent []byte) error {
	var got, wanted queryResponse

	err := json.Unmarshal(tsContent, &got)
	if err != nil {
		return fmt.Errorf("unexpected error returned when reading connector response body:\n%s\nbody:\n%s\n", err.Error(), tsContent)
	}

	err = json.Unmarshal(promContent, &wanted)
	if err != nil {
		return fmt.Errorf("unexpected error returned when reading Prometheus response body:\n%s\nbody:\n%s\n", err.Error(), promContent)
	}

	// Sorting to make sure
	sort.Sort(got.Data.Result)
	sort.Sort(wanted.Data.Result)

	if !reflect.DeepEqual(got, wanted) {
		return fmt.Errorf("unexpected response:\ngot\n%v\nwanted\n%v", got, wanted)
	}

	return nil
}

func labelsResultComparator(promContent []byte, tsContent []byte) error {
	var got, wanted labelsResponse

	err := json.Unmarshal(tsContent, &got)
	if err != nil {
		return fmt.Errorf("unexpected error returned when reading connector response body:\n%s\nbody:\n%s\n", err.Error(), tsContent)
	}

	err = json.Unmarshal(promContent, &wanted)
	if err != nil {
		return fmt.Errorf("unexpected error returned when reading Prometheus response body:\n%s\nbody:\n%s\n", err.Error(), promContent)
	}

	if !reflect.DeepEqual(got, wanted) {
		return fmt.Errorf("unexpected response:\ngot\n%v\nwanted\n%v", got, wanted)
	}

	return nil
}

func testRequest(req *http.Request, handler http.Handler, client *http.Client, comparator resultComparator) func(*testing.T) {
	return func(t *testing.T) {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		tsResp := rec.Result()
		promResp, promErr := client.Do(req)

		if promErr != nil {
			t.Fatalf("unexpected error returned from Prometheus client:\n%s\n", promErr.Error())
		}

		promContent, err := ioutil.ReadAll(promResp.Body)

		if err != nil {
			t.Fatalf("unexpected error returned when reading Prometheus response body:\n%s\n", err.Error())
		}
		defer promResp.Body.Close()

		tsContent, err := ioutil.ReadAll(tsResp.Body)

		if err != nil {
			t.Fatalf("unexpected error returned when reading connector response body:\n%s\n", err.Error())
		}
		defer tsResp.Body.Close()

		err = comparator(promContent, tsContent)
		if err != nil {
			t.Fatal(err)
		}
	}
}
