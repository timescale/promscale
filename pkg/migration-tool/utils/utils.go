// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package utils

import (
	"fmt"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
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

// BuildWriteRequest builds the write request by converting the promb.timeseries into byte slice
// which can be fed to the shards.
func BuildWriteRequest(timeseries []prompb.TimeSeries, buf []byte) (compressed []byte, numBytesCompressed, numBytesUncompressedBytes int, err error) {
	req := &prompb.WriteRequest{
		Timeseries: timeseries,
	}
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, -1, -1, err
	}
	numBytesUncompressedBytes = len(data)

	// snappy uses len() to see if it needs to allocate a new slice. Make the
	// buffer as long as possible.
	if buf != nil {
		buf = buf[0:cap(buf)]
	}
	compressed = snappy.Encode(buf, data)
	numBytesCompressed = len(compressed)
	return
}
