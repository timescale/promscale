package utils

import (
	"fmt"
	"sync"

	"github.com/prometheus/common/config"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
)

const (
	LabelJob = "job"
	Megabyte = 1024 * 1024
)

// authStore is used while creating a client. This is done to avoid complexity in the writer and reader code, thereby
// keeping them independent of the auth.
var authStore sync.Map

// Auth defines the authentication for prom-migrator.
type Auth struct {
	Username    string
	Password    string
	BearerToken string
}

// Convert converts the auth credentials to HTTP client compatible format.
func (a *Auth) ToHTTPClientConfig() config.HTTPClientConfig {
	return config.HTTPClientConfig{
		BasicAuth: &config.BasicAuth{
			Username: a.Username,
			Password: config.Secret(a.Password),
		},
		BearerToken: config.Secret(a.BearerToken),
	}
}

// SetAuthStore sets the authStore map.
func SetAuthStore(clientType uint, clientConfig config.HTTPClientConfig) error {
	if _, ok := authStore.Load(clientType); ok {
		return fmt.Errorf("auth store is read-only. Attempting to change pre-existing key: %d", clientType)
	}
	authStore.Store(clientType, clientConfig)
	return nil
}

// CreatePrombRequest creates a new promb query based on the matchers.
func CreatePrombQuery(mint, maxt int64, matchers []*labels.Matcher) (*prompb.Query, error) {
	ms, err := toLabelMatchers(matchers)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	return &prompb.Query{
		StartTimestampMs: mint,
		EndTimestampMs:   maxt,
		Matchers:         ms,
	}, nil
}

// LabelSet creates a new label_set for the provided metric name and job name.
func LabelSet(metricName, migrationJobName string) []prompb.Label {
	return []prompb.Label{
		{Name: labels.MetricName, Value: metricName},
		{Name: LabelJob, Value: migrationJobName},
	}
}

func toLabelMatchers(matchers []*labels.Matcher) ([]*prompb.LabelMatcher, error) {
	pbMatchers := make([]*prompb.LabelMatcher, 0, len(matchers))
	for _, m := range matchers {
		var mType prompb.LabelMatcher_Type
		switch m.Type {
		case labels.MatchEqual:
			mType = prompb.LabelMatcher_EQ
		case labels.MatchNotEqual:
			mType = prompb.LabelMatcher_NEQ
		case labels.MatchRegexp:
			mType = prompb.LabelMatcher_RE
		case labels.MatchNotRegexp:
			mType = prompb.LabelMatcher_NRE
		default:
			return nil, fmt.Errorf("invalid matcher type")
		}
		pbMatchers = append(pbMatchers, &prompb.LabelMatcher{
			Type:  mType,
			Name:  m.Name,
			Value: m.Value,
		})
	}
	return pbMatchers, nil
}
