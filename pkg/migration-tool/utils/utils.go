package utils

import (
	"fmt"
	"net/url"

	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
)

const (
	Megabyte = 1024 * 1024
	LabelJob = "job"
)

// CreateReadClient creates a new read client that can be used to fetch promb samples.
func CreateReadClient(name, urlString string, readTimeout model.Duration) (remote.ReadClient, error) {
	parsedUrl, err := url.Parse(urlString)
	if err != nil {
		return nil, fmt.Errorf("parsing-url: %w", err)
	}
	rc, err := remote.NewReadClient(name, &remote.ClientConfig{
		URL:     &config.URL{URL: parsedUrl},
		Timeout: readTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("new read-client: %w", err)
	}
	return rc, nil
}

// CreateWriteClient creates a new write client that can be used to push promb samples.
func CreateWriteClient(name, urlString string, readTimeout model.Duration) (remote.WriteClient, error) {
	parsedUrl, err := url.Parse(urlString)
	if err != nil {
		return nil, fmt.Errorf("parsing-url: %w", err)
	}
	wc, err := remote.NewWriteClient(name, &remote.ClientConfig{
		URL:     &config.URL{URL: parsedUrl},
		Timeout: readTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("new write-client: %w", err)
	}
	return wc, nil
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
