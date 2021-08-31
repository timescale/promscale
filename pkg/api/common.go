// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

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
	"github.com/timescale/promscale/pkg/log"
	pgmodel "github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/promql"
	"github.com/timescale/promscale/pkg/tenancy"
)

var (
	minTimeFormatted = pgmodel.MinTime.Format(time.RFC3339Nano)
	maxTimeFormatted = pgmodel.MaxTime.Format(time.RFC3339Nano)

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
	AllowedOrigin    *regexp.Regexp
	ReadOnly         bool
	HighAvailability bool
	AdminAPIEnabled  bool
	TelemetryPath    string

	Auth         *Auth
	MultiTenancy tenancy.Authorizer

	// PromQL configuration.
	EnableFeatures       string
	EnabledFeaturesList  []string
	MaxQueryTimeout      time.Duration
	SubQueryStepInterval time.Duration // Default step interval value if the user has not provided.
	LookBackDelta        time.Duration
	MaxSamples           int64
	MaxPointsPerTs       int64
}

func ParseFlags(fs *flag.FlagSet, cfg *Config) *Config {
	cfg.Auth = &Auth{}

	fs.BoolVar(&cfg.ReadOnly, "read-only", false, "Read-only mode for the connector. Operations related to writing or updating the database are disallowed. It is used when pointing the connector to a TimescaleDB read replica.")
	fs.BoolVar(&cfg.HighAvailability, "high-availability", false, "Enable external_labels based HA.")
	fs.BoolVar(&cfg.AdminAPIEnabled, "web-enable-admin-api", false, "Allow operations via API that are for advanced users. Currently, these operations are limited to deletion of series.")
	fs.StringVar(&cfg.TelemetryPath, "web-telemetry-path", "/metrics", "Web endpoint for exposing Promscale's Prometheus metrics.")

	fs.StringVar(&cfg.Auth.BasicAuthUsername, "auth-username", "", "Authentication username used for web endpoint authentication. Disabled by default.")
	fs.StringVar(&cfg.Auth.BasicAuthPassword, "auth-password", "", "Authentication password used for web endpoint authentication. This flag should be set together with auth-username. It is mutually exclusive with auth-password-file and bearer-token flags.")
	fs.StringVar(&cfg.Auth.BasicAuthPasswordFile, "auth-password-file", "", "Path for auth password file containing the actual password used for web endpoint authentication. This flag should be set together with auth-username. It is mutually exclusive with auth-password and bearer-token methods.")
	fs.StringVar(&cfg.Auth.BearerToken, "bearer-token", "", "Bearer token (JWT) used for web endpoint authentication. Disabled by default. Mutually exclusive with bearer-token-file and basic auth methods.")
	fs.StringVar(&cfg.Auth.BearerTokenFile, "bearer-token-file", "", "Path of the file containing the bearer token (JWT) used for web endpoint authentication. Disabled by default. Mutually exclusive with bearer-token and basic auth methods.")

	// PromQL configuration flags.
	fs.StringVar(&cfg.EnableFeatures, "promql-enable-feature", "", "[EXPERIMENTAL] Enable optional PromQL features, separated by commas. These are disabled by default in Promscale's PromQL engine. "+
		"Currently, this includes 'promql-at-modifier' and 'promql-negative-offset'. For more information, see https://github.com/prometheus/prometheus/blob/master/docs/disabled_features.md")
	fs.DurationVar(&cfg.MaxQueryTimeout, "promql-query-timeout", 2*time.Minute, "Maximum time a query may take before being aborted. This option sets both the default and maximum value of the 'timeout' parameter in "+
		"'/api/v1/query.*' endpoints.")
	fs.DurationVar(&cfg.SubQueryStepInterval, "promql-default-subquery-step-interval", 1*time.Minute, "Default step interval to be used for PromQL subquery evaluation. "+
		"This value is used if the subquery does not specify the step value explicitly. Example: <metric_name>[30m:]. Note: in Prometheus this setting is set by the evaluation_interval option.")
	fs.DurationVar(&cfg.LookBackDelta, "promql-lookback-delta", time.Minute*5, "Maximum lookback duration for retrieving metrics during expression evaluations and federation.")
	fs.Int64Var(&cfg.MaxSamples, "promql-max-samples", 50000000, "Maximum number of samples a single "+
		"query can load into memory. Note that queries will fail if they try to load more samples than this into memory, "+
		"so this also limits the number of samples a query can return.")
	fs.Int64Var(&cfg.MaxPointsPerTs, "promql-max-points-per-ts", 11000, "Maximum number of points per time-series in a query-range request. "+
		"This calculation is an estimation, that happens as (start - end)/step where start and end are the 'start' and 'end' timestamps of the query_range.")
	return cfg
}

func Validate(cfg *Config) error {
	if cfg.EnableFeatures != "" {
		cfg.EnabledFeaturesList = strings.Split(cfg.EnableFeatures, ",")
	} else {
		cfg.EnabledFeaturesList = []string{}
	}
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

func setResponseHeaders(w http.ResponseWriter, samples *promql.Result, isExemplar bool, warnings storage.Warnings) {
	w.Header().Set("Content-Type", "application/json")
	if warnings != nil && len(warnings) > 0 {
		w.Header().Set("Cache-Control", "no-store")
	}
	if isExemplar {
		// Exemplar response headers do not return StatusNoContent, if data is nil.
		w.WriteHeader(http.StatusOK)
		return
	}
	if samples != nil && samples.Value != nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func respondQuery(w http.ResponseWriter, res *promql.Result, warnings storage.Warnings) {
	setResponseHeaders(w, res, false, warnings)
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

func respondExemplar(w http.ResponseWriter, data []pgmodel.ExemplarQueryResult) {
	setResponseHeaders(w, nil, true, nil)
	_ = marshalExemplarResponse(w, data)
}

func respond(w http.ResponseWriter, status int, message interface{}) {
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
	b, err := json.Marshal(&errResponse{
		Status:    "error",
		ErrorType: errType,
		Error:     err.Error(),
	})
	if err != nil {
		log.Error("msg", "error marshalling json error", "err", err)
	}
	if n, err := w.Write(b); err != nil {
		log.Error("msg", "error writing response", "bytesWritten", n, "err", err)
	}
}

func respondErrorWithMessage(w http.ResponseWriter, status int, err error, errType string, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	b, err := json.Marshal(&errResponse{
		Status:    "error",
		ErrorType: errType,
		Error:     err.Error(),
		Message:   message,
	})
	if err != nil {
		log.Error("msg", "error marshalling json error", "err", err)
	}
	if n, err := w.Write(b); err != nil {
		log.Error("msg", "error writing response", "bytesWritten", n, "err", err)
	}
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
	marshalCommonFooter(out, warnings, true)
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
		return pgmodel.MinTime, nil
	case maxTimeFormatted:
		return pgmodel.MaxTime, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func ParseDuration(s string) (time.Duration, error) {
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
