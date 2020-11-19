package utils

import (
	"fmt"
	"net/url"
	"sync"

	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
)

var interner = sync.Map{}

const Megabyte = 1024 * 1024

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

// ProgressSeries is the series that represents the progress metric series.
type ProgressSeries struct {
	*prompb.TimeSeries
}

// GetorGenerateProgressTimeseries returns the progress timeseries. It creates a new time-series based on input
// if no time-series is present in the interner. To add a sample, call the append sample function.
func GetorGenerateProgressTimeseries(metricName, migrationJobName string) (*ProgressSeries, error) {
	if ps, ok := interner.Load(labels.MetricName); ok {
		ps.(*ProgressSeries).Samples = make([]prompb.Sample, 0)
		return ps.(*ProgressSeries), nil
	}
	lset := labels.Labels{
		labels.Label{Name: labels.MetricName, Value: metricName},
		labels.Label{Name: "migration-job", Value: migrationJobName},
	}
	ps := &ProgressSeries{&prompb.TimeSeries{
		Labels:  labelsToLabelsProto(lset),
		Samples: make([]prompb.Sample, 0),
	}}
	interner.Store(labels.MetricName, ps)
	return ps, nil
}

// MergeWithTimeseries merges the new series with the time-series.
func MergeWithTimeseries(ts *[]*prompb.TimeSeries, newTS *prompb.TimeSeries) {
	*ts = append(*ts, newTS)
}

// Append appends the new sample inside the progress series.
func (ps *ProgressSeries) Append(v float64, t int64) {
	ps.Samples = append(ps.Samples, prompb.Sample{Timestamp: t, Value: v})
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

func labelsToLabelsProto(labels labels.Labels) []prompb.Label {
	result := make([]prompb.Label, 0, len(labels))
	for _, l := range labels {
		result = append(result, prompb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return result
}
