package api

import (
	"context"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/timescale/promscale/pkg/log"
)

func TestCORSWrapper(t *testing.T) {
	_ = log.Init(log.Config{
		Level: "debug",
	})
	acceptSpecific, _ := regexp.Compile("^(?:" + "http://some-site.com" + ")$")
	acceptAny, _ := regexp.Compile("^(?:" + ".*" + ")$")

	testCases := []struct {
		name           string
		requestOrigin  string
		acceptedOrigin *regexp.Regexp
		expectHeaders  map[string][]string
	}{
		{
			name:           "No origin",
			requestOrigin:  "",
			acceptedOrigin: acceptSpecific,
			expectHeaders:  map[string][]string{},
		}, {
			name:           "Origin doesn't match accepted",
			requestOrigin:  "http://some-unknown-site.com",
			acceptedOrigin: acceptSpecific,
			expectHeaders: map[string][]string{
				"Access-Control-Allow-Headers":  {"Accept, Authorization, Content-Type, Origin"},
				"Access-Control-Allow-Methods":  {"GET, POST, OPTIONS"},
				"Access-Control-Expose-Headers": {"Date"},
				"Vary":                          {"Origin"},
			},
		},
		{
			name:           "Origin matches accepted",
			requestOrigin:  "http://some-site.com",
			acceptedOrigin: acceptSpecific,
			expectHeaders: map[string][]string{
				"Access-Control-Allow-Headers":  {"Accept, Authorization, Content-Type, Origin"},
				"Access-Control-Allow-Methods":  {"GET, POST, OPTIONS"},
				"Access-Control-Expose-Headers": {"Date"},
				"Access-Control-Allow-Origin":   {"http://some-site.com"},
				"Vary":                          {"Origin"},
			},
		}, {
			name:           "Wildcard allowed origin",
			requestOrigin:  "http://any-site.com",
			acceptedOrigin: acceptAny,
			expectHeaders: map[string][]string{
				"Access-Control-Allow-Headers":  {"Accept, Authorization, Content-Type, Origin"},
				"Access-Control-Allow-Methods":  {"GET, POST, OPTIONS"},
				"Access-Control-Expose-Headers": {"Date"},
				"Access-Control-Allow-Origin":   {"*"},
				"Vary":                          {"Origin"},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conf := &Config{}
			if tc.acceptedOrigin != nil {
				conf.AllowedOrigin = tc.acceptedOrigin
			} else {
				tc.acceptedOrigin = &regexp.Regexp{}
			}
			internalHandlerCalled := false
			handler := corsWrapper(conf, func(http.ResponseWriter, *http.Request) {
				internalHandlerCalled = true
			})
			w := doCORSWrapperRequest(t, handler, "http://localhost/", tc.requestOrigin)
			if !internalHandlerCalled {
				t.Fatalf("internal handler not called by CORS wrapper")
				return
			}
			returnedHeaders := w.Header()
			if len(returnedHeaders) != len(tc.expectHeaders) {
				t.Fatalf("expected %d headers, got %d", len(tc.expectHeaders), len(returnedHeaders))
				return
			}
			for hName, hValues := range tc.expectHeaders {
				returnedValues := returnedHeaders[hName]
				if !reflect.DeepEqual(hValues, returnedValues) {
					t.Errorf("expected header %s with value %v; got %v", hName, hValues, returnedValues)
				}
			}
		})

	}

}

func doCORSWrapperRequest(t *testing.T, queryHandler http.Handler, url, origin string) *httptest.ResponseRecorder {
	req, err := http.NewRequestWithContext(context.Background(), "GET", url, nil)
	if err != nil {
		t.Errorf("%v", err)
	}

	req.Header.Set("Origin", origin)
	w := httptest.NewRecorder()
	queryHandler.ServeHTTP(w, req)
	return w
}

func TestValidateConfig(t *testing.T) {
	fileContents, err := ioutil.ReadFile("common_test.go")
	if err != nil {
		t.Fatal("error reading file contents common_test.go")
	}
	fileContentsString := strings.TrimSpace(string(fileContents))
	testCases := []struct {
		name      string
		cfg       *Auth
		returnErr error
		passSet   string
		tokenSet  string
	}{
		{
			name: "empty config",
			cfg:  &Auth{},
		},
		{
			name: "basic auth and bearer token set",
			cfg: &Auth{
				BasicAuthUsername: "foo",
				BearerToken:       "foo",
			},
			returnErr: usernameAndTokenFlagsSetError,
		},
		{
			name: "basic auth missing password",
			cfg: &Auth{
				BasicAuthUsername: "foo",
			},
			returnErr: noPasswordFlagsSetError,
		},
		{
			name: "basic auth password and password file set",
			cfg: &Auth{
				BasicAuthUsername:     "foo",
				BasicAuthPassword:     "foo",
				BasicAuthPasswordFile: "foo",
			},
			returnErr: multiplePasswordFlagsSetError,
		},
		{
			name: "basic auth invalid password file",
			cfg: &Auth{
				BasicAuthUsername:     "foo",
				BasicAuthPasswordFile: "invalid filename",
			},
			returnErr: os.ErrNotExist,
		},
		{
			name: "basic auth password set",
			cfg: &Auth{
				BasicAuthUsername: "foo",
				BasicAuthPassword: "pass",
			},
			passSet: "pass",
		},
		{
			name: "basic auth no username set",
			cfg: &Auth{
				BasicAuthPassword: "pass",
			},
			returnErr: noUsernameFlagSetError,
		},
		{
			name: "basic auth password file set",
			cfg: &Auth{
				BasicAuthUsername:     "foo",
				BasicAuthPasswordFile: "common_test.go",
			},
			passSet: fileContentsString,
		},
		{
			name: "bearer token and token file set",
			cfg: &Auth{
				BearerToken:     "foo",
				BearerTokenFile: "foo",
			},
			returnErr: multipleTokenFlagsSetError,
		},
		{
			name: "bearer token set",
			cfg: &Auth{
				BearerToken: "foo",
			},
			tokenSet: "foo",
		},
		{
			name: "bearer token file set",
			cfg: &Auth{
				BearerTokenFile: "common_test.go",
			},
			tokenSet: fileContentsString,
		},
		{
			name: "bearer token file invalid file set",
			cfg: &Auth{
				BearerTokenFile: "invalid file",
			},
			returnErr: os.ErrNotExist,
		},
		{
			name: "all config options set",
			cfg: &Auth{
				BasicAuthUsername:     "set",
				BasicAuthPassword:     "set",
				BasicAuthPasswordFile: "set",
				BearerToken:           "set",
				BearerTokenFile:       "set",
			},
			returnErr: usernameAndTokenFlagsSetError,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			err := Validate(&Config{
				Auth: c.cfg,
			})
			if c.returnErr != nil {
				if !errors.Is(err, c.returnErr) {
					t.Errorf("unexpected error received: %s", err)
				}
			} else if err != nil {
				t.Errorf("unexpected error received: %s", err)
			}

			if c.passSet != "" && c.cfg.BasicAuthPassword != c.passSet {
				t.Errorf("unexpected password set: got %s wanted %s", c.cfg.BasicAuthPassword, c.passSet)
			}
			if c.tokenSet != "" && c.cfg.BearerToken != c.tokenSet {
				t.Errorf("unexpected bearer token set: got %s wanted %s", c.cfg.BearerToken, c.tokenSet)
			}
		})
	}
}
