// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package auth

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/timescale/promscale/pkg/log"
)

var (
	usernameAndTokenFlagsSetError = fmt.Errorf("at most one of basic-auth-username, bearer-token & bearer-token-file must be set")
	noUsernameFlagSetError        = fmt.Errorf("invalid auth setup, cannot enable authorization with password only (username required)")
	noPasswordFlagsSetError       = fmt.Errorf("one of basic-auth-password & basic-auth-password-file must be configured")
	multiplePasswordFlagsSetError = fmt.Errorf("at most one of basic-auth-password & basic-auth-password-file must be configured")
	multipleTokenFlagsSetError    = fmt.Errorf("at most one of bearer-token & bearer-token-file must be set")
)

type arrayOfIgnorePaths []string

type Config struct {
	BasicAuthUsername     string
	BasicAuthPassword     string
	BasicAuthPasswordFile string

	BearerToken     string
	BearerTokenFile string

	IgnorePaths arrayOfIgnorePaths
}

func (p *arrayOfIgnorePaths) String() string {
	return fmt.Sprintf("auth ignored paths: %#v", p)
}

func (p *arrayOfIgnorePaths) Set(path string) error {
	*p = append(*p, path)
	return nil
}

func readFromFile(path string, defaultValue string) (string, error) {
	if path == "" {
		return defaultValue, nil
	}
	bs, err := os.ReadFile(path) // #nosec G304
	if err != nil {
		return "", fmt.Errorf("unable to read file %s: %w", path, err)
	}

	return strings.TrimSpace(string(bs)), nil
}

func (a *Config) Validate() error {
	switch {
	case a.BasicAuthUsername != "":
		if a.BearerToken != "" || a.BearerTokenFile != "" {
			return usernameAndTokenFlagsSetError
		}
		if a.BasicAuthPassword == "" && a.BasicAuthPasswordFile == "" {
			return noPasswordFlagsSetError
		}
		if a.BasicAuthPassword != "" && a.BasicAuthPasswordFile != "" {
			return multiplePasswordFlagsSetError
		}
		pwd, err := readFromFile(a.BasicAuthPasswordFile, a.BasicAuthPassword)
		if err != nil {
			return fmt.Errorf("error reading password file: %w", err)
		}
		a.BasicAuthPassword = pwd
	case a.BasicAuthPassword != "" || a.BasicAuthPasswordFile != "":
		// At this point, if we have password set with no username, throw
		// error to warn the user this is an invalid auth setup.
		return noUsernameFlagSetError
	case a.BearerToken != "" || a.BearerTokenFile != "":
		if a.BearerToken != "" && a.BearerTokenFile != "" {
			return multipleTokenFlagsSetError
		}
		token, err := readFromFile(a.BearerTokenFile, a.BearerToken)
		if err != nil {
			return fmt.Errorf("error reading bearer token file: %w", err)
		}
		a.BearerToken = token
	case a.IgnorePaths != nil:
		for _, p := range a.IgnorePaths {
			_, err := path.Match(p, "")
			if err != nil {
				return fmt.Errorf("invalid ignore path pattern: %w", err)
			}
		}
	}

	return nil
}

func ParseFlags(fs *flag.FlagSet, cfg *Config) *Config {
	fs.StringVar(&cfg.BasicAuthUsername, "web.auth.username", "", "Authentication username used for web endpoint authentication. Disabled by default.")
	fs.StringVar(&cfg.BasicAuthPassword, "web.auth.password", "", "Authentication password used for web endpoint authentication. This flag should be set together with auth-username. It is mutually exclusive with auth-password-file and bearer-token flags.")
	fs.StringVar(&cfg.BasicAuthPasswordFile, "web.auth.password-file", "", "Path for auth password file containing the actual password used for web endpoint authentication. This flag should be set together with auth-username. It is mutually exclusive with auth-password and bearer-token methods.")
	fs.StringVar(&cfg.BearerToken, "web.auth.bearer-token", "", "Bearer token (JWT) used for web endpoint authentication. Disabled by default. Mutually exclusive with bearer-token-file and basic auth methods.")
	fs.StringVar(&cfg.BearerTokenFile, "web.auth.bearer-token-file", "", "Path of the file containing the bearer token (JWT) used for web endpoint authentication. Disabled by default. Mutually exclusive with bearer-token and basic auth methods.")
	fs.Var(&cfg.IgnorePaths, "web.auth.ignore-path", "HTTP paths which has to be skipped from authentication. This flag shall be repeated and each one would be appended to the ignore list.")
	return cfg
}

func Validate(cfg *Config) error {
	return cfg.Validate()
}

func (cfg *Config) isIgnoredPath(r *http.Request) bool {
	for _, pathIgnored := range cfg.IgnorePaths {
		ignorePathFound, _ := path.Match(pathIgnored, r.URL.Path)
		if ignorePathFound {
			return true
		}
	}
	return false
}

func (cfg *Config) AuthHandler(handler http.Handler) http.Handler {
	if cfg.BasicAuthUsername != "" {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if cfg.isIgnoredPath(r) {
				handler.ServeHTTP(w, r)
				return
			}
			user, pass, ok := r.BasicAuth()
			if !ok || cfg.BasicAuthUsername != user || cfg.BasicAuthPassword != pass {
				log.Error("msg", "Unauthorized access to endpoint, invalid username or password")
				http.Error(w, "Unauthorized access to endpoint, invalid username or password.", http.StatusUnauthorized)
				return
			}
			handler.ServeHTTP(w, r)
		})
	}

	if cfg.BearerToken != "" {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if cfg.isIgnoredPath(r) {
				handler.ServeHTTP(w, r)
				return
			}
			splitToken := strings.Split(r.Header.Get("Authorization"), "Bearer ")
			if len(splitToken) < 2 || cfg.BearerToken != splitToken[1] {
				log.Error("msg", "Unauthorized access to endpoint, invalid bearer token")
				http.Error(w, "Unauthorized access to endpoint, invalid bearer token", http.StatusUnauthorized)
				return
			}
			handler.ServeHTTP(w, r)
		})
	}

	return handler
}
