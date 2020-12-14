// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package utils

import (
	"fmt"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/common/config"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/timescale/promscale/pkg/clockcache"
)

const (
	LabelJob           = "job"
	Megabyte           = 1024 * 1024
	maxLabelsCacheSize = 50 * 1000
)

var (
	// authStore is used while creating a client. This is done to avoid complexity in the writer and reader code, thereby
	// keeping them independent of the auth.
	authStore   sync.Map
	labelsCache *clockcache.Cache
	seps        = []byte{'\xff'}
)

func init() {
	labelsCache = clockcache.WithMax(maxLabelsCacheSize)
}

// Auth defines the authentication for prom-migrator.
type Auth struct {
	Username    string
	Password    string
	BearerToken string
}

// Convert converts the auth credentials to HTTP client compatible format.
func (a *Auth) ToHTTPClientConfig() config.HTTPClientConfig {
	conf := config.HTTPClientConfig{}
	if a.Password != "" {
		conf.BasicAuth = &config.BasicAuth{
			Username: a.Username,
			Password: config.Secret(a.Password),
		}
	}
	if a.BearerToken != "" {
		// Since Password and BearerToken are mutually exclusive, we assign both on input flag condition
		// and leave upto the HTTPClientConfig.Validate() for validation.
		conf.BearerToken = config.Secret(a.BearerToken)
	}
	return conf
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
		EndTimestampMs:   maxt - 1, // maxt is exclusive while EndTimestampMs is inclusive.
		Matchers:         ms,
	}, nil
}

// Hash returns the hash of the provided promb labels-set. It fetches the value of the hash from the labelsCache if
// exists, else creates the hash and returns it after storing the new hash value.
func Hash(lset prompb.Labels) uint64 {
	if hash, found := labelsCache.Get(lset.String()); found {
		return hash.(uint64)
	}
	b := make([]byte, 0, 1024)
	for i, v := range lset.Labels {
		if len(b)+len(v.Name)+len(v.Value)+2 >= cap(b) {
			// If labels entry is 1KB+ do not allocate whole entry.
			h := xxhash.New()
			_, _ = h.Write(b)
			for _, v := range lset.Labels[i:] {
				_, _ = h.WriteString(v.Name)
				_, _ = h.Write(seps)
				_, _ = h.WriteString(v.Value)
				_, _ = h.Write(seps)
			}
			return h.Sum64()
		}

		b = append(b, v.Name...)
		b = append(b, seps[0])
		b = append(b, v.Value...)
		b = append(b, seps[0])
	}
	sum := xxhash.Sum64(b)
	labelsCache.Insert(lset.String(), sum)
	return sum
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
