// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"reflect"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/common/model"
	"github.com/timescale/promscale/pkg/api"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/ha"
	"github.com/timescale/promscale/pkg/pgxconn"
)

type requestCase struct {
	tsReq   *http.Request
	promReq *http.Request
	log     string
}

type sample struct {
	Timestamp int64
	Raw       string
	Metric    model.Metric       `json:"metric"`
	Values    []model.SamplePair `json:"values"`
	Value     model.SamplePair   `json:"value"`
}

type dummySample sample

func (s *sample) UnmarshalJSON(data []byte) error {
	var temp dummySample
	err := json.Unmarshal(data, &temp)
	if err == nil {
		*s = sample(temp)
		return nil
	}

	err = json.Unmarshal(data, &s.Timestamp)
	if err == nil {
		*s = sample(temp)
		return nil
	}

	return json.Unmarshal(data, &s.Raw)
}

func (s sample) Equal(other sample) bool {
	if s.Timestamp != other.Timestamp {
		return false
	}
	if s.Raw != other.Raw {
		return false
	}

	if !reflect.DeepEqual(s.Metric, other.Metric) {
		return false
	}

	valuesAreWiithinThreshold := func(left model.SamplePair, right model.SamplePair) bool {
		if left.Value.Equal(right.Value) {
			return true
		}

		diff := float64(left.Value) - float64(right.Value)
		return math.Abs(diff) < valueDiffThreshold
	}

	if !valuesAreWiithinThreshold(s.Value, other.Value) {
		return false
	}

	if len(s.Values) != len(other.Values) {
		return false
	}
	for i, v := range s.Values {
		if v.Timestamp != other.Values[i].Timestamp {
			return false
		}

		if !valuesAreWiithinThreshold(v, other.Values[i]) {
			return false
		}
	}

	return true
}

type samples []sample

func (s samples) Len() int           { return len(s) }
func (s samples) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s samples) Less(i, j int) bool { return s[i].Metric.Before(s[j].Metric) }

func (s samples) Equal(other samples) bool {
	if len(s) != len(other) {
		return false
	}
	for i, cur := range s {
		if !cur.Equal(other[i]) {
			return false
		}
	}

	return true
}

type resultComparator func(promContent []byte, tsContent []byte, log string) error

func testRequest(tsReq, promReq *http.Request, client *http.Client, comparator resultComparator, log string) func(*testing.T) {
	return func(t *testing.T) {
		tsResp, tsErr := client.Do(tsReq)

		if tsErr != nil {
			t.Fatalf("unexpected error returned from TS client:\n%s\n", tsErr.Error())
		}
		promResp, promErr := client.Do(promReq)

		if promErr != nil {
			t.Fatalf("unexpected error returned from Prometheus client:\n%s\n", promErr.Error())
		}

		compareHTTPHeaders(t, promResp.Header, tsResp.Header)

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

		err = comparator(promContent, tsContent, log)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func testRequestConcurrent(requestCases []requestCase, client *http.Client, comparator resultComparator) func(*testing.T) {
	return func(t *testing.T) {

		perm := rand.Perm(len(requestCases))

		wg := sync.WaitGroup{}

		for i := 0; i < len(perm); i++ {
			tsReq := requestCases[perm[i]].tsReq
			promReq := requestCases[perm[i]].promReq
			log := requestCases[perm[i]].log
			wg.Add(1)
			go func() {
				defer wg.Done()

				tsResp, tsErr := client.Do(tsReq)
				if tsErr != nil {
					t.Errorf("unexpected error returned from TS client:\n%s\n", tsErr.Error())
					return
				}
				promResp, promErr := client.Do(promReq)

				if promErr != nil {
					t.Errorf("unexpected error returned from Prometheus client:\n%s\n", promErr.Error())
					return
				}

				// If we get a 422 Unprocessable Entity HTTP status code from Prometheus,
				// let's try one more time.
				if promResp.StatusCode == http.StatusUnprocessableEntity {
					promResp, promErr = client.Do(promReq)

					if promErr != nil {
						t.Errorf("unexpected error returned from Prometheus client when retrying:\n%s\n", promErr.Error())
						return
					}
				}

				compareHTTPHeaders(t, promResp.Header, tsResp.Header)

				promContent, err := ioutil.ReadAll(promResp.Body)

				if err != nil {
					t.Errorf("unexpected error returned when reading Prometheus response body:\n%s\n", err.Error())
					return
				}
				defer promResp.Body.Close()

				tsContent, err := ioutil.ReadAll(tsResp.Body)

				if err != nil {
					t.Errorf("unexpected error returned when reading connector response body:\n%s\n", err.Error())
					return
				}
				defer tsResp.Body.Close()

				err = comparator(promContent, tsContent, log)
				if err != nil {
					t.Errorf("%s gives %s", log, err)
					return
				}
			}()

			// Batching 100 requests at a time not to overwhelm the endpoint.
			if i%100 == 0 {
				wg.Wait()
			}
		}
		wg.Wait()
	}
}

func compareHTTPHeaders(t *testing.T, expected, actual http.Header) {
	for k, v := range expected {
		if !reflect.DeepEqual(v, actual[k]) {
			if k == "Date" {
				if dateHeadersMatch(v, actual[k]) {
					continue
				}
			}
			t.Errorf("unexpected HTTP header value for header \"%s\":\ngot\n%v\nwanted\n%v\n", k, actual[k], v)
			return
		}
	}

}

// dateHeadersMatch checks if the date headers from two HTTP responses match
// and are within a tolerance of 10 seconds.
func dateHeadersMatch(expected, actual []string) bool {
	if len(expected) != 1 {
		return false
	}

	if len(actual) != 1 {
		return false
	}
	expectedDate, expectedErr := http.ParseTime(expected[0])
	actualDate, actualErr := http.ParseTime(actual[0])

	if expectedErr != actualErr {
		return false
	}

	return expectedDate.Sub(actualDate) <= 10*time.Second
}

// buildRouter builds a testing router from a connection pool.
func buildRouter(pool *pgxpool.Pool) (http.Handler, *pgclient.Client, error) {
	apiConfig := &api.Config{
		AllowedOrigin:        regexp.MustCompile(".*"),
		TelemetryPath:        "/metrics",
		MaxQueryTimeout:      time.Minute * 2,
		SubQueryStepInterval: time.Minute,
		EnabledFeaturesList:  []string{"promql-at-modifier"},
	}

	return buildRouterWithAPIConfig(pool, apiConfig)
}

// buildRouterWithAPIConfig builds a testing router from a connection pool and
// an API config.
func buildRouterWithAPIConfig(pool *pgxpool.Pool, cfg *api.Config) (http.Handler, *pgclient.Client, error) {
	metrics := api.InitMetrics(0)
	conf := &pgclient.Config{
		AsyncAcks:               false,
		ReportInterval:          0,
		LabelsCacheSize:         10000,
		MetricsCacheSize:        cache.DefaultMetricCacheSize,
		WriteConnectionsPerProc: 4,
		MaxConnections:          -1,
	}

	pgClient, err := pgclient.NewClientWithPool(conf, 1, pgxconn.NewPgxConn(pool))

	if err != nil {
		return nil, pgClient, errors.New("Cannot run test, cannot instantiate pgClient")
	}

	hander, err := api.GenerateRouter(cfg, metrics, pgClient, nil, ha.NewHAState())
	if err != nil {
		return nil, nil, fmt.Errorf("generate router: %w", err)
	}
	return hander, pgClient, nil
}
