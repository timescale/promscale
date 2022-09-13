// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package auth

import (
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strings"
	"testing"
)

func TestValidateConfig(t *testing.T) {
	fileContents, err := os.ReadFile("auth_test.go")
	if err != nil {
		t.Fatal("error reading file contents auth_test.go")
	}
	fileContentsString := strings.TrimSpace(string(fileContents))
	testCases := []struct {
		name      string
		cfg       *Config
		returnErr error
		passSet   string
		tokenSet  string
	}{
		{
			name: "empty config",
			cfg:  &Config{},
		},
		{
			name: "basic auth and bearer token set",
			cfg: &Config{
				BasicAuthUsername: "foo",
				BearerToken:       "foo",
			},
			returnErr: usernameAndTokenFlagsSetError,
		},
		{
			name: "basic auth missing password",
			cfg: &Config{
				BasicAuthUsername: "foo",
			},
			returnErr: noPasswordFlagsSetError,
		},
		{
			name: "basic auth password and password file set",
			cfg: &Config{
				BasicAuthUsername:     "foo",
				BasicAuthPassword:     "foo",
				BasicAuthPasswordFile: "foo",
			},
			returnErr: multiplePasswordFlagsSetError,
		},
		{
			name: "basic auth invalid password file",
			cfg: &Config{
				BasicAuthUsername:     "foo",
				BasicAuthPasswordFile: "invalid filename",
			},
			returnErr: os.ErrNotExist,
		},
		{
			name: "basic auth password set",
			cfg: &Config{
				BasicAuthUsername: "foo",
				BasicAuthPassword: "pass",
			},
			passSet: "pass",
		},
		{
			name: "basic auth no username set",
			cfg: &Config{
				BasicAuthPassword: "pass",
			},
			returnErr: noUsernameFlagSetError,
		},
		{
			name: "basic auth password file set",
			cfg: &Config{
				BasicAuthUsername:     "foo",
				BasicAuthPasswordFile: "auth_test.go",
			},
			passSet: fileContentsString,
		},
		{
			name: "bearer token and token file set",
			cfg: &Config{
				BearerToken:     "foo",
				BearerTokenFile: "foo",
			},
			returnErr: multipleTokenFlagsSetError,
		},
		{
			name: "bearer token set",
			cfg: &Config{
				BearerToken: "foo",
			},
			tokenSet: "foo",
		},
		{
			name: "bearer token file set",
			cfg: &Config{
				BearerTokenFile: "auth_test.go",
			},
			tokenSet: fileContentsString,
		},
		{
			name: "bearer token file invalid file set",
			cfg: &Config{
				BearerTokenFile: "invalid file",
			},
			returnErr: os.ErrNotExist,
		},
		{
			name: "all config options set",
			cfg: &Config{
				BasicAuthUsername:     "set",
				BasicAuthPassword:     "set",
				BasicAuthPasswordFile: "set",
				BearerToken:           "set",
				BearerTokenFile:       "set",
			},
			returnErr: usernameAndTokenFlagsSetError,
		},
		{
			name: "invalid ignore path",
			cfg: &Config{
				IgnorePaths: []string{
					"[",
				},
			},
			returnErr: path.ErrBadPattern,
		},
		{
			name: "valid ignore path",
			cfg: &Config{
				IgnorePaths: []string{
					"/hello",
				},
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			err := Validate(c.cfg)
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

func TestAuthHandler(t *testing.T) {
	testCases := []struct {
		name       string
		cfg        *Config
		headers    map[string]string
		authorized bool
		path       string
	}{
		{
			name:       "no auth",
			cfg:        &Config{},
			authorized: true,
		},
		{
			name: "no auth header",
			cfg: &Config{
				BasicAuthUsername: "foo",
			},
		},
		{
			name: "wrong auth header",
			cfg: &Config{
				BasicAuthUsername: "foo",
				BasicAuthPassword: "bar",
			},
			headers: map[string]string{
				"Authorization": "wrong",
			},
		},
		{
			name: "correct auth header",
			cfg: &Config{
				BasicAuthUsername: "foo",
				BasicAuthPassword: "bar",
			},
			headers: map[string]string{
				"Authorization": "Basic " + base64.StdEncoding.EncodeToString([]byte("foo:bar")),
			},
			authorized: true,
		},
		{
			name: "wrong bearer token",
			cfg: &Config{
				BearerToken: "foo",
			},
			headers: map[string]string{
				"Authorization": "Bearer bar",
			},
		},
		{
			name: "correct bearer token",
			cfg: &Config{
				BearerToken: "foo",
			},
			headers: map[string]string{
				"Authorization": "Bearer foo",
			},
			authorized: true,
		},
		{
			name: "basic auth with ignore path",
			cfg: &Config{
				BasicAuthUsername: "foo",
				BasicAuthPassword: "foo",
				IgnorePaths: []string{
					"/healthz",
					"/api",
				},
			},
			authorized: true,
			path:       "/healthz",
		},
		{
			name: "bearer token with non ignored path",
			cfg: &Config{
				BearerToken: "foo",
				IgnorePaths: []string{
					"/healthz",
					"/api",
				},
			},
			authorized: false,
			path:       "/api/foo",
		},
		{
			name: "basic auth with non ignore path",
			cfg: &Config{
				BasicAuthUsername: "foo",
				BasicAuthPassword: "foo",
				IgnorePaths: []string{
					"/healthz",
					"/api",
				},
			},
			authorized: false,
			path:       "/api/foo",
		},
		{
			name: "bearer token with ignore path",
			cfg: &Config{
				BearerToken: "foo",
				IgnorePaths: []string{
					"/healthz",
					"/api",
				},
			},
			authorized: false,
			path:       "/api/foo",
		},
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "", http.StatusOK)
	})

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			w := httptest.NewRecorder()

			req, err := http.NewRequest("GET", c.path, nil)
			if err != nil {
				t.Errorf("%v", err)
			}

			for name, value := range c.headers {
				req.Header.Set(name, value)
			}

			h := c.cfg.AuthHandler(handler)
			h.ServeHTTP(w, req)

			if c.authorized && w.Code != http.StatusOK {
				t.Errorf("request should be authorized, was not: %d", w.Code)

			}
			if !c.authorized && w.Code == http.StatusOK {
				t.Errorf("request should not be authorized")
			}
		})
	}
}
