// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgmodel

import (
	"encoding/binary"
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

// Get the canonical version of a Labels if one exists.
// input: the string representation of a Labels as defined by getStr()
// This function should not be called directly, use labelProtosToLabels() or
// LabelsFromSlice() instead.
func GetLabels(str string) (l *Labels) {
	val, ok := LabelsInterner.Load(str)
	if !ok {
		return
	}
	l = val.(*Labels)
	return
}

// Try to set a Labels as the canonical Labels for a given string
// representation, returning the canonical version (which can be different in
// the even of multiple goroutines setting labels concurrently).
// This function should not be called directly, use labelProtosToLabels() or
// LabelsFromSlice() instead.
func SetLabels(str string, lset *Labels) *Labels {
	val, _ := LabelsInterner.LoadOrStore(str, lset)
	return val.(*Labels)
}

// LabelsFromSlice converts a labels.Labels to a canonical Labels object
func LabelsFromSlice(ls labels.Labels) (*Labels, error) {
	ll := make([]prompb.Label, len(ls))
	for i := range ls {
		ll[i].Name = ls[i].Name
		ll[i].Value = ls[i].Value
	}
	l, _, err := labelProtosToLabels(ll)
	return l, err
}

// Get a string representation for hashing and comparison
// This representation is guaranteed to uniquely represent the underlying label
// set, though need not human-readable, or indeed, valid utf-8
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

// labelProtosToLabels converts a prompb.Label to a canonical Labels object
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

// Get a string representation for hashing and comparison
// This representation is guaranteed to uniquely represent the underlying label
// set, though need not human-readable, or indeed, valid utf-8
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
var _ sort.Interface = (*Labels)(nil)

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
