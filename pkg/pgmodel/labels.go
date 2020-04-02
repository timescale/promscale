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

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
)

// labels stores a labels.Labels in its canonical string representation
type Labels struct {
	names       []string
	values      []string
	metric_name string
	str         string
}

func EmptyLables() Labels {
	return Labels{}
}

func LabelsFromSlice(ls labels.Labels) (Labels, error) {
	length := len(ls)
	names := make([]string, 0, length)
	values := make([]string, 0, length)

	metricName := ""
	for _, l := range ls {
		names = append(names, l.Name)
		values = append(values, l.Value)
		if l.Name == metricNameLabelName {
			metricName = l.Value
		}
	}

	return LabelsFromSlices(names, values, metricName)
}

func LabelsFromSlices(names []string, values []string, metric_name string) (Labels, error) {
	l := Labels{names: names, values: values, metric_name: metric_name}

	if !sort.IsSorted(&l) {
		sort.Sort(&l)
	}

	length := len(l.names)
	vals := l.values[:length]

	expected_str_len := length * 4 // 2 for the length of each key, and 2 for the lengthof each value
	for i := 0; i < length; i++ {
		expected_str_len += len(l.names[i]) + len(vals[i])
	}

	// BigCache cannot handle cases where the key string has a size greater than
	// 16bits, so we error on such keys here. Since we are restricted to a 16bit
	// total length anyway, we only use 16bits to store the legth of each substring
	// in our string encoding
	if expected_str_len > math.MaxUint16 {
		return l, fmt.Errorf("series too long, combined series has length %d, max length %d", expected_str_len, ^uint16(0))
	}

	// the string representation is
	//   (<key-len>key <val-len> val)* (<key-len>key <val-len> val)?
	// that is a series of the a sequence of key values pairs with each string
	// prefixed with it's length as a little-endian uint16
	builder := strings.Builder{}
	builder.Grow(expected_str_len)

	lengthBuf := make([]byte, 2)
	for i := 0; i < length; i++ {
		key := l.names[i]

		// this cast is safe since we check that the combined length of all the
		// strings fit within a uint16, each string's length must also fit
		binary.LittleEndian.PutUint16(lengthBuf, uint16(len(key)))
		builder.WriteByte(lengthBuf[0])
		builder.WriteByte(lengthBuf[1])
		builder.WriteString(key)

		val := vals[i]

		// this cast is safe since we check that the combined length of all the
		// strings fit within a uint16, each string's length must also fit
		binary.LittleEndian.PutUint16(lengthBuf, uint16(len(val)))
		builder.WriteByte(lengthBuf[0])
		builder.WriteByte(lengthBuf[1])
		builder.WriteString(val)
	}

	l.str = builder.String()

	return l, nil
}

func labelProtosToLabels(labelPairs []prompb.Label) (Labels, string, error) {
	length := len(labelPairs)
	names := make([]string, 0, length)
	values := make([]string, 0, length)

	metricName := ""
	for _, l := range labelPairs {
		names = append(names, l.Name)
		values = append(values, l.Value)
		if l.Name == metricNameLabelName {
			metricName = l.Value
		}
	}

	ls, err := LabelsFromSlices(names, values, metricName)

	return ls, metricName, err
}

func (l Labels) isEmpty() bool {
	return l.names == nil
}

func (l *Labels) String() string {
	return l.str
}

func (a Labels) Compare(b Labels) int {
	return strings.Compare(a.str, b.str)
}

func (a Labels) Equal(b Labels) bool {
	return a.str == b.str
}

// Labels implements sort.Interface

func (l *Labels) Len() int {
	return len(l.names)
}

func (l *Labels) Less(i, j int) bool {
	return l.names[i] < l.names[j]
}

func (l *Labels) Swap(i, j int) {
	tmp := l.names[j]
	l.names[j] = l.names[i]
	l.names[i] = tmp

	tmp = l.values[j]
	l.values[j] = l.values[i]
	l.values[i] = tmp
}
