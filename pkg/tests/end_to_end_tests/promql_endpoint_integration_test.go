// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/grafana/regexp"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	"github.com/timescale/promscale/pkg/api"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/query"
	"github.com/timescale/promscale/pkg/tenancy"
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

		promContent, err := io.ReadAll(promResp.Body)
		if err != nil {
			t.Fatalf("unexpected error returned when reading Prometheus response body:\n%s\n", err.Error())
		}
		defer promResp.Body.Close()

		tsContent, err := io.ReadAll(tsResp.Body)

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

func testRequestConcurrent(requestCases []requestCase, client *http.Client, comparator resultComparator, failOnStatusErrors bool) func(*testing.T) {
	return func(t *testing.T) {

		perm := rand.Perm(len(requestCases))

		wg := sync.WaitGroup{}

		for i := 0; i < len(perm); i++ {
			tsReq := requestCases[perm[i]].tsReq
			promReq := requestCases[perm[i]].promReq
			log := requestCases[perm[i]].log

			if t.Failed() {
				break
			}

			wg.Add(1)
			//nolint
			go func() {
				defer wg.Done()

				tsResp, tsErr := client.Do(tsReq)
				if tsErr != nil {
					t.Errorf("unexpected error returned from TS client:\n%s\n", tsErr.Error())
					return
				}
				if failOnStatusErrors && tsResp.StatusCode != http.StatusOK {
					t.Errorf("promscale: expected status code %d, received %d. Info: %v", http.StatusOK, tsResp.StatusCode, log)
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

				if failOnStatusErrors && promResp.StatusCode != http.StatusOK {
					t.Errorf("prometheus: expected status code %d, received %d", http.StatusOK, tsResp.StatusCode)
				}

				compareHTTPHeaders(t, promResp.Header, tsResp.Header)

				promContent, err := io.ReadAll(promResp.Body)

				if err != nil {
					t.Errorf("unexpected error returned when reading Prometheus response body:\n%s\n", err.Error())
					return
				}
				defer promResp.Body.Close()

				tsContent, err := io.ReadAll(tsResp.Body)

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
// and are within a tolerance of 60 seconds (GH runner is slow sometimes)
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

	return expectedDate.Sub(actualDate) <= 60*time.Second
}

func defaultAPIConfig() *api.Config {
	return &api.Config{
		AllowedOrigin: regexp.MustCompile(".*"),
		TelemetryPath: "/metrics",
	}
}

func defaultQueryConfig() *query.Config {
	return &query.Config{
		MaxQueryTimeout:      time.Minute * 2,
		SubQueryStepInterval: time.Minute,
		EnabledFeatureMap:    map[string]struct{}{"promql-at-modifier": {}, "promql-negative-offset": {}},
		MaxSamples:           math.MaxInt32,
		MaxPointsPerTs:       11000,
	}
}

// buildRouter builds a testing router from a connection pool.
func buildRouter(pool *pgxpool.Pool) (*mux.Router, *pgclient.Client, error) {
	apiConfig := defaultAPIConfig()
	apiConfig.ReadOnly = true
	return buildRouterWithAPIConfig(pool, apiConfig, nil)
}

// buildRouterWithAPIConfig builds a testing router from a connection pool and
// an API config.
func buildRouterWithAPIConfig(pool *pgxpool.Pool, cfg *api.Config, authWrapper mux.MiddlewareFunc) (*mux.Router, *pgclient.Client, error) {
	api.InitMetrics()
	conf := &pgclient.Config{
		CacheConfig:    cache.DefaultConfig,
		MaxConnections: -1,
	}

	pgClient, err := pgclient.NewClientWithPool(prometheus.NewRegistry(), conf, 1, pool, pool, nil, tenancy.NewNoopAuthorizer(), cfg.ReadOnly)
	if err != nil {
		return nil, pgClient, fmt.Errorf("cannot run test, cannot instantiate pgClient: %w", err)
	}

	qryCfg := defaultQueryConfig()
	if err = pgClient.InitPromQLEngine(qryCfg); err != nil {
		return nil, nil, fmt.Errorf("init promql engine: %w", err)
	}

	router, err := api.GenerateRouter(cfg, qryCfg, pgClient, nil, authWrapper, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("generate router: %w", err)
	}
	return router, pgClient, nil
}
