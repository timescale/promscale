package prompb

import (
	"errors"
	"github.com/prometheus/prometheus/pkg/labels"
)

// FromLabelMatchers parses protobuf label matchers to Prometheus label matchers.
func FromLabelMatchers(matchers []*LabelMatcher) ([]*labels.Matcher, error) {
	result := make([]*labels.Matcher, 0, len(matchers))
	for _, matcher := range matchers {
		var mtype labels.MatchType
		switch matcher.Type {
		case LabelMatcher_EQ:
			mtype = labels.MatchEqual
		case LabelMatcher_NEQ:
			mtype = labels.MatchNotEqual
		case LabelMatcher_RE:
			mtype = labels.MatchRegexp
		case LabelMatcher_NRE:
			mtype = labels.MatchNotRegexp
		default:
			return nil, errors.New("invalid matcher type")
		}
		matcher, err := labels.NewMatcher(mtype, matcher.Name, matcher.Value)
		if err != nil {
			return nil, err
		}
		result = append(result, matcher)
	}
	return result, nil
}
