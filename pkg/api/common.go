package api

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/httputil"
	"github.com/timescale/promscale/pkg/promql"
)

var (
	minTime = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()
	maxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()

	minTimeFormatted = minTime.Format(time.RFC3339Nano)
	maxTimeFormatted = maxTime.Format(time.RFC3339Nano)

	usernameAndTokenFlagsSetError = fmt.Errorf("at most one of basic-auth-username, bearer-token & bearer-token-file must be set")
	noUsernameFlagSetError        = fmt.Errorf("invalid auth setup, cannot enable authorization with password only (username required)")
	noPasswordFlagsSetError       = fmt.Errorf("one of basic-auth-password & basic-auth-password-file must be configured")
	multiplePasswordFlagsSetError = fmt.Errorf("at most one of basic-auth-password & basic-auth-password-file must be configured")
	multipleTokenFlagsSetError    = fmt.Errorf("at most one of bearer-token & bearer-token-file must be set")
)

type Auth struct {
	BasicAuthUsername     string
	BasicAuthPassword     string
	BasicAuthPasswordFile string

	BearerToken     string
	BearerTokenFile string
}

func (a *Auth) Validate() error {
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
	}

	return nil
}

type Config struct {
	AllowedOrigin   *regexp.Regexp
	ReadOnly        bool
	AdminAPIEnabled bool
	TelemetryPath   string

	Auth *Auth
}

func ParseFlags(cfg *Config) *Config {
	cfg.Auth = &Auth{}

	flag.BoolVar(&cfg.ReadOnly, "read-only", false, "Read-only mode for the connector. Operations related to writing or updating the database are disallowed. It is used when pointing the connector to a TimescaleDB read replica.")
	flag.BoolVar(&cfg.AdminAPIEnabled, "web-enable-admin-api", false, "Allow operations via API that are for advanced users. Currently, these operations are limited to deletion of series.")
	flag.StringVar(&cfg.TelemetryPath, "web-telemetry-path", "/metrics", "Web endpoint for exposing Promscale's Prometheus metrics.")

	flag.StringVar(&cfg.Auth.BasicAuthUsername, "auth-username", "", "Authentication username used for web endpoint authentication. Disabled by default.")
	flag.StringVar(&cfg.Auth.BasicAuthPassword, "auth-password", "", "Authentication password used for web endpoint authentication. This flag should be set together with auth-username. It is mutually exclusive with auth-password-file and bearer-token flags.")
	flag.StringVar(&cfg.Auth.BasicAuthPasswordFile, "auth-password-file", "", "Path for auth password file containing the actual password used for web endpoint authentication. This flag should be set together with auth-username. It is mutually exclusive with auth-password and bearer-token methods.")
	flag.StringVar(&cfg.Auth.BearerToken, "bearer-token", "", "Bearer token (JWT) used for web endpoint authentication. Disabled by default. Mutually exclusive with bearer-token-file and basic auth methods.")
	flag.StringVar(&cfg.Auth.BearerTokenFile, "bearer-token-file", "", "Path of the file containing the bearer token (JWT) used for web endpoint authentication. Disabled by default. Mutually exclusive with bearer-token and basic auth methods.")
	return cfg
}

func Validate(cfg *Config) error {
	return cfg.Auth.Validate()
}

func readFromFile(path string, defaultValue string) (string, error) {
	if path == "" {
		return defaultValue, nil
	}
	bs, err := ioutil.ReadFile(path) // #nosec G304
	if err != nil {
		return "", fmt.Errorf("unable to read file %s: %w", path, err)
	}

	return strings.TrimSpace(string(bs)), nil
}

func corsWrapper(conf *Config, f http.HandlerFunc) http.HandlerFunc {
	if conf.AllowedOrigin == nil {
		return f
	}
	return func(w http.ResponseWriter, r *http.Request) {
		httputil.SetCORS(w, conf.AllowedOrigin, r)
		f(w, r)
	}
}

func setResponseHeaders(w http.ResponseWriter, res *promql.Result, warnings storage.Warnings) {
	w.Header().Set("Content-Type", "application/json")
	if warnings != nil && len(warnings) > 0 {
		w.Header().Set("Cache-Control", "no-store")
	}
	if res != nil && res.Value != nil {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}

func respondQuery(w http.ResponseWriter, res *promql.Result, warnings storage.Warnings) {
	setResponseHeaders(w, res, warnings)
	switch resVal := res.Value.(type) {
	case promql.Vector:
		warnings := make([]string, 0, len(res.Warnings))
		for _, warn := range res.Warnings {
			warnings = append(warnings, warn.Error())
		}
		_ = marshalVectorResponse(w, resVal, warnings)
	case promql.Matrix:
		warnings := make([]string, 0, len(res.Warnings))
		for _, warn := range res.Warnings {
			warnings = append(warnings, warn.Error())
		}
		_ = marshalMatrixResponse(w, resVal, warnings)
	default:
		resp := &response{
			Status: "success",
			Data: &queryData{
				ResultType: res.Value.Type(),
				Result:     res.Value,
			},
		}
		for _, warn := range res.Warnings {
			resp.Warnings = append(resp.Warnings, warn.Error())
		}
		_ = json.NewEncoder(w).Encode(resp)
	}
}

func respond(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(&response{
		Status: http.StatusText(status),
		Data:   message,
	})
}

func respondError(w http.ResponseWriter, status int, err error, errType string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(&errResponse{
		Status:    "error",
		ErrorType: errType,
		Error:     err.Error(),
	})
}

func respondErrorWithMessage(w http.ResponseWriter, status int, err error, errType string, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(&errResponse{
		Status:    "error",
		ErrorType: errType,
		Error:     err.Error(),
		Message:   message,
	})
}

type errResponse struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
	Message   string `json:"message,omitempty"`
}

type response struct {
	Status   string      `json:"status"`
	Data     interface{} `json:"data,omitempty"`
	Warnings []string    `json:"warnings,omitempty"`
}

type queryData struct {
	ResultType parser.ValueType `json:"resultType"`
	Result     parser.Value     `json:"result"`
}

func marshalMatrixResponse(writer io.Writer, data promql.Matrix, warnings []string) error {
	out := &errorWrapper{writer: writer}
	marshalCommonHeader(out)
	marshalMatrixData(out, data)
	marshalCommonFooter(out, warnings)
	return out.err
}

func parseTimeParam(r *http.Request, paramName string, defaultValue time.Time) (time.Time, error) {
	val := r.FormValue(paramName)
	if val == "" {
		return defaultValue, nil
	}
	result, err := parseTime(val)
	if err != nil {
		return time.Time{}, fmt.Errorf("Invalid time value for '%s': %w", paramName, err)
	}
	return result, nil
}

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	// Stdlib's time parser can only handle 4 digit years. As a workaround until
	// that is fixed we want to at least support our own boundary times.
	// Context: https://github.com/prometheus/client_golang/issues/614
	// Upstream issue: https://github.com/golang/go/issues/20555
	switch s {
	case minTimeFormatted:
		return minTime, nil
	case maxTimeFormatted:
		return maxTime, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}
