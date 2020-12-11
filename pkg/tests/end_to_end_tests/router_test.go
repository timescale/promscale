package end_to_end_tests

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/timescale/promscale/pkg/api"
)

var (
	routes = map[string]string{
		"/write":                         "POST",
		"/read":                          "GET,POST",
		"/delete_series":                 "PUT,POST",
		"/api/v1/query":                  "GET,POST",
		"/api/v1/query_range":            "GET,POST",
		"/api/v1/series":                 "GET,POST",
		"/api/v1/labels":                 "GET,POST",
		"/api/v1/label/foo/values":       "GET",
		"/healthz":                       "GET",
		"/debug/pprof":                   "GET",
		"/debug/pprof/cmdline":           "GET",
		"/debug/pprof/profile?seconds=1": "GET",
		"/debug/pprof/symbol":            "GET",
		"/debug/pprof/trace":             "GET",
		"/metrics":                       "GET",
	}
)

func TestRouterAuth(t *testing.T) {
	if testing.Short() || !*useDocker {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		var tester *testing.T
		var ok bool
		if tester, ok = t.(*testing.T); !ok {
			t.Fatalf("Cannot run test, not an instance of testing.T")
			return
		}

		apiConfig := &api.Config{
			AllowedOrigin: regexp.MustCompile(".*"),
			TelemetryPath: "/metrics",
		}

		router, pgClient, err := buildRouterWithAPIConfig(db, apiConfig)
		if err != nil {
			t.Fatalf("Cannot run test, unable to build router: %s", err)
			return
		}
		defer pgClient.Close()

		ts := httptest.NewServer(router)
		defer ts.Close()

		client := &http.Client{Timeout: 10 * time.Second}

		for path, methodCSV := range routes {
			methods := strings.Split(methodCSV, ",")
			for _, method := range methods {
				req, err := http.NewRequest(method, ts.URL+path, nil)
				if err != nil {
					tester.Errorf("unexpected error while creating request: %s", err)
				}
				resp, err := client.Do(req)
				if err != nil {
					tester.Errorf("unexpected error while sending request: %s", err)
				}

				if resp.StatusCode == http.StatusUnauthorized {
					tester.Errorf("unexpected Unauthorized HTTP status code: path %s, method %s", path, method)
				}
			}
		}

		apiConfig = &api.Config{
			AllowedOrigin: regexp.MustCompile(".*"),
			TelemetryPath: "/metrics",
			Auth: &api.Auth{
				BasicAuthUsername: "foo",
				BasicAuthPassword: "foo",
			},
		}

		router, pgClient, err = buildRouterWithAPIConfig(db, apiConfig)
		if err != nil {
			t.Fatalf("Cannot run test, unable to build router: %s", err)
			return
		}
		defer pgClient.Close()

		ts = httptest.NewServer(router)
		defer ts.Close()

		for path, methodCSV := range routes {
			methods := strings.Split(methodCSV, ",")
			for _, method := range methods {
				req, err := http.NewRequest(method, ts.URL+path, nil)
				if err != nil {
					tester.Errorf("unexpected error while creating request: %s", err)
				}
				resp, err := client.Do(req)
				if err != nil {
					tester.Errorf("unexpected error while sending request: %s", err)
				}

				if resp.StatusCode != http.StatusUnauthorized {
					tester.Errorf("unexpected HTTP status code, wanted Unauthorized: path %s, method %s code %d", path, method, resp.StatusCode)
				}
			}
		}
	})
}
