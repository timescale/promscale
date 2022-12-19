package end_to_end_tests

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sergi/go-diff/diffmatchpatch"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
)

func TestExemplarQueryEndpoint(t *testing.T) {
	testCases := []testCase{
		{
			name:  "empty expr",
			query: "",
		},
		{
			name:  "expr",
			query: "metric_1",
		},
		{
			name:  "binary expr 1",
			query: "metric_1 + metric_1",
		},
		{
			name:  "binary expr 2",
			query: "metric_1 + metric_2",
		},
		{
			name:  "not existent",
			query: "metric_not_available",
		},
		{
			name:  "binary expr with non existent",
			query: "metric_1 + metric_not_available",
		},
		{
			name:  "binary expr with arithmetic operators",
			query: "2*(metric_1/5 + 2*metric_2)",
		},
	}
	start := time.Unix(startTimeRecent/1000, 0)
	end := time.Unix(endTimeRecent/1000, 0)
	runExemplarQueryTests(t, testCases, start, end, false)
}

func runExemplarQueryTests(t *testing.T, cases []testCase, start, end time.Time, failOnStatusErrors bool) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		// Ingest test dataset.
		dataset := timeseriesWithExemplars[:]
		ingestQueryTestDataset(db, t, dataset)
		// Getting a read-only connection to ensure read path is idempotent.
		readOnly := testhelpers.GetReadOnlyConnection(t, *testDatabase)
		defer readOnly.Close()

		var tester *testing.T
		var ok bool
		if tester, ok = t.(*testing.T); !ok {
			t.Fatalf("Cannot run test, not an instance of testing.T")
			return
		}

		router, pgClient, err := buildRouter(readOnly)
		if err != nil {
			t.Fatalf("Cannot run test, unable to build router: %s", err)
			return
		}
		defer pgClient.Close()

		ts := httptest.NewServer(router)
		defer ts.Close()

		tsURL := fmt.Sprintf("%s/api/v1", ts.URL)
		promURL := fmt.Sprintf("http://%s:%d/api/v1", promExemplarHost, promExemplarPort.Int())
		client := &http.Client{Timeout: 300 * time.Second}

		var (
			requestCases []requestCase
			tsReq        *http.Request
			promReq      *http.Request
		)
		for _, c := range cases {
			tsReq, err = genExemplarRequest(tsURL, c.query, start, end)
			if err != nil {
				t.Fatalf("unable to create TS PromQL query request: %s", err)
			}
			promReq, err = genExemplarRequest(promURL, c.query, start, end)
			if err != nil {
				t.Fatalf("unable to create Prometheus PromQL query request: %s", err)
			}
			requestCases = append(requestCases, requestCase{tsReq, promReq, fmt.Sprintf("%s (exemplar query, [start to start] query=%v)", c.name, c.query)})

			// Exemplars Query, till 30 seconds from start.
			tsReq, err = genExemplarRequest(tsURL, c.query, start, end.Add(time.Second*30))
			if err != nil {
				t.Fatalf("unable to create TS exemplar query request: %s", err)
			}
			promReq, err = genExemplarRequest(promURL, c.query, start, end.Add(time.Second*30))
			if err != nil {
				t.Fatalf("unable to create Prometheus exemplar query request: %s", err)
			}
			requestCases = append(requestCases, requestCase{tsReq, promReq, fmt.Sprintf("%s (exempar query, [start to start+30] query=%v)", c.name, c.query)})
		}
		testMethod := testRequestConcurrent(requestCases, client, queryExemplarsResultComparator, failOnStatusErrors)
		tester.Run("test exemplars query endpoint", testMethod)
	})
}

func queryExemplarsResultComparator(promContent []byte, tsContent []byte, _ string) error {
	tsStr := string(tsContent)
	promStr := string(promContent)

	if !reflect.DeepEqual(tsStr, promStr) {
		dmp := diffmatchpatch.New()
		diffs := dmp.DiffMain(fmt.Sprintf("%+v", tsStr), fmt.Sprintf("%+v", promStr), false)
		return fmt.Errorf("unexpected response:\ntimescale\n%+v\nprom\n%+v\ndiff\n%v", tsStr, promStr, dmp.DiffPrettyText(diffs))
	}
	return nil
}
