package end_to_end_tests

import (
	"encoding/json"
	"fmt"
    "math/rand"
	"net/http"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/timescale/timescale-prometheus/pkg/api"
	"github.com/timescale/timescale-prometheus/pkg/internal/testhelpers"
	"github.com/timescale/timescale-prometheus/pkg/pgmodel"
	"github.com/timescale/timescale-prometheus/pkg/query"
)

type seriesResponse struct {
	Status string
	Data   []labels.Labels
}

type seriesTestCase struct {
    name     string
	matchers []string
}

func genSeriesRequest(apiURL string, matchers []string, start, end time.Time) (*http.Request, error) {
	u, err := url.Parse(fmt.Sprintf("%s/series", apiURL))

	if err != nil {
		return nil, err
	}

	val := url.Values{}

	for _, m := range matchers {
		val.Add("match[]", m)
	}
	val.Add("start", fmt.Sprintf("%d", start.Unix()))
	val.Add("end", fmt.Sprintf("%d", end.Unix()))
	u.RawQuery = val.Encode()

	return http.NewRequest(
		"GET",
		u.String(),
		nil,
	)
}

func genSeriesNoTimeRequest(apiURL string, matchers []string) (*http.Request, error) {
	u, err := url.Parse(fmt.Sprintf("%s/series", apiURL))

	if err != nil {
		return nil, err
	}

	val := url.Values{}

	for _, m := range matchers {
		val.Add("match[]", m)
	}
	u.RawQuery = val.Encode()

	return http.NewRequest(
		"GET",
		u.String(),
		nil,
	)
}

func getSeries(num int) ([]seriesTestCase) {
    numCases := 5
	testCases := []seriesTestCase {
		{
			name:     "metric name matcher",
			matchers: []string{"metric_1"},
		},
		{
			name:     "not regex match metric name",
			matchers: []string{`{__name__!~".*_1", instance="1"}`},
		},
		{
			name:     "metric name and eq match",
			matchers: []string{`metric_1{instance="1"}`},
		},
		{
			name:     "metric name and neq match",
			matchers: []string{`metric_1{instance!="1"}`},
		},
		{
			name:     "single matcher, non-existant metric",
			matchers: []string{`nonexistant_metric_name`},
		},
	}

    if num == 0 {
        return testCases
    }

    samples := make([]seriesTestCase, num)
    for i := range samples {
        samples[i] = testCases[rand.Intn(numCases)]
    }

    return samples
}

func TestPromQLSeriesEndpoint(t *testing.T) {
	if testing.Short() || !*useDocker {
		t.Skip("skipping integration test")
	}

    testCases := getSeries(0)

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

		r := pgmodel.NewPgxReader(readOnly, nil, 100)
		queryable := query.NewQueryable(r.GetQuerier())

		series := api.Series(queryable)

		apiURL := fmt.Sprintf("http://%s:%d/api/v1", testhelpers.PromHost, testhelpers.PromPort.Int())
		client := &http.Client{Timeout: 10 * time.Second}

		start := time.Unix(startTime/1000, 0)
		end := time.Unix(endTime/1000, 0)
		var (
			req *http.Request
			err error
		)
		for _, c := range testCases {
			req, err = genSeriesRequest(apiURL, c.matchers, start, end)
			if err != nil {
				t.Fatalf("unable to create PromQL query request: %s", err)
			}
			testMethod := testRequest(req, series, client, seriesResultComparator)
			tester.Run(fmt.Sprintf("%s (instant query)", c.name), testMethod)

			req, err = genSeriesNoTimeRequest(apiURL, c.matchers)
			if err != nil {
				t.Fatalf("unable to create PromQL query request: %s", err)
			}
			testMethod = testRequest(req, series, client, seriesResultComparator)
			tester.Run(fmt.Sprintf("%s (instant query)", c.name), testMethod)
		}
	})
}

func seriesResultComparator(promContent []byte, tsContent []byte) error {
	var got, wanted seriesResponse

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
