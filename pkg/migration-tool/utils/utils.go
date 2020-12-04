package utils

import (
	"fmt"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
)

const (
	LabelJob = "job"
	Megabyte = 1024 * 1024
)

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
