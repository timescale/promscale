// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgmodel

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/timescale/promscale/pkg/prompb"
)

// Labels stores a labels.Labels in its canonical string representation
type Labels struct {
	names      []string
	values     []string
	metricName string
	str        string
}

var LabelsInterner = sync.Map{}

func GetLabels(str string) (l *Labels) {
	val, ok := LabelsInterner.Load(str)
	if !ok {
		return
	}
	l = val.(*Labels)
	return
}

func SetLabels(str string, lset *Labels) *Labels {
	val, _ := LabelsInterner.LoadOrStore(str, lset)
	return val.(*Labels)
}

// LabelsFromSlice converts a labels.Labels to a Labels object
func LabelsFromSlice(ls labels.Labels) (*Labels, error) {
	ll := make([]prompb.Label, len(ls))
	for i := range ls {
		ll[i].Name = ls[i].Name
		ll[i].Value = ls[i].Value
	}
	l, _, err := labelProtosToLabels(ll)
	return l, err
}

// initLabels intializes labels
func getStr(labels []prompb.Label) (string, error) {
	if len(labels) == 0 {
		return "", nil
	}

	comparator := func(i, j int) bool {
		return labels[i].Name < labels[j].Name
	}

	if !sort.SliceIsSorted(labels, comparator) {
		sort.Slice(labels, comparator)
	}

	expectedStrLen := len(labels) * 4 // 2 for the length of each key, and 2 for the length of each value
	for i := range labels {
		l := labels[i]
		expectedStrLen += len(l.Name) + len(l.Value)
	}

	// BigCache cannot handle cases where the key string has a size greater than
	// 16bits, so we error on such keys here. Since we are restricted to a 16bit
	// total length anyway, we only use 16bits to store the legth of each substring
	// in our string encoding
	if expectedStrLen > math.MaxUint16 {
		return "", fmt.Errorf("series too long, combined series has length %d, max length %d", expectedStrLen, ^uint16(0))
	}

	// the string representation is
	//   (<key-len>key <val-len> val)* (<key-len>key <val-len> val)?
	// that is a series of the a sequence of key values pairs with each string
	// prefixed with it's length as a little-endian uint16
	builder := strings.Builder{}
	builder.Grow(expectedStrLen)

	lengthBuf := make([]byte, 2)
	for i := range labels {
		l := labels[i]
		key := l.Name

		// this cast is safe since we check that the combined length of all the
		// strings fit within a uint16, each string's length must also fit
		binary.LittleEndian.PutUint16(lengthBuf, uint16(len(key)))
		builder.WriteByte(lengthBuf[0])
		builder.WriteByte(lengthBuf[1])
		builder.WriteString(key)

		val := l.Value

		// this cast is safe since we check that the combined length of all the
		// strings fit within a uint16, each string's length must also fit
		binary.LittleEndian.PutUint16(lengthBuf, uint16(len(val)))
		builder.WriteByte(lengthBuf[0])
		builder.WriteByte(lengthBuf[1])
		builder.WriteString(val)
	}

	return builder.String(), nil
}

func labelProtosToLabels(labelPairs []prompb.Label) (*Labels, string, error) {
	str, err := getStr(labelPairs)
	if err != nil {
		return nil, "", err
	}
	labels := GetLabels(str)
	if labels == nil {
		labels = new(Labels)
		labels.str = str
		labels.names = make([]string, len(labelPairs))
		labels.values = make([]string, len(labelPairs))
		for i, l := range labelPairs {
			labels.names[i] = l.Name
			labels.values[i] = l.Value
			if l.Name == MetricNameLabelName {
				labels.metricName = l.Value
			}
		}
		labels = SetLabels(str, labels)
	}

	return labels, labels.metricName, err
}

func (l *Labels) String() string {
	return l.str
}

// Compare returns a comparison int between two Labels
func (l *Labels) Compare(b *Labels) int {
	return strings.Compare(l.str, b.str)
}

// Equal returns true if two Labels are equal
func (l *Labels) Equal(b *Labels) bool {
	return l.str == b.str
}

// Labels implements sort.Interface

func (l *Labels) Len() int {
	return len(l.names)
}

func (l *Labels) Less(i, j int) bool {
	return l.names[i] < l.names[j]
}

func (l *Labels) Swap(i, j int) {
	l.names[j], l.names[i] = l.names[i], l.names[j]
	l.values[j], l.values[i] = l.values[i], l.values[j]
}

// FromLabelMatchers parses protobuf label matchers to Prometheus label matchers.
func FromLabelMatchers(matchers []*prompb.LabelMatcher) ([]*labels.Matcher, error) {
	result := make([]*labels.Matcher, 0, len(matchers))
	for _, matcher := range matchers {
		var mtype labels.MatchType
		switch matcher.Type {
		case prompb.LabelMatcher_EQ:
			mtype = labels.MatchEqual
		case prompb.LabelMatcher_NEQ:
			mtype = labels.MatchNotEqual
		case prompb.LabelMatcher_RE:
			mtype = labels.MatchRegexp
		case prompb.LabelMatcher_NRE:
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
