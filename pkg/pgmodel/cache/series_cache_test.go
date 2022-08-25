// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package cache

import (
	"bytes"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/prompb"
	"math"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
)

func ConvertLabels(ls labels.Labels) []prompb.Label {
	ll := make([]prompb.Label, len(ls))
	for i := range ls {
		ll[i].Name = ls[i].Name
		ll[i].Value = ls[i].Value
	}
	return ll
}

func TestBigLabels(t *testing.T) {
	builder := strings.Builder{}
	builder.Grow(int(^uint16(0)) + 1) // one greater than uint16 max

	builder.WriteByte('a')
	for len(builder.String()) < math.MaxUint16 {
		builder.WriteString(builder.String())
	}

	l := labels.Labels{
		labels.Label{
			Name:  builder.String(),
			Value: "",
		},
	}

	pbLabels := ConvertLabels(l)
	_, _, err := GenerateKey(pbLabels)
	require.Error(t, err)
}

func TestGenerateKey(t *testing.T) {
	var labels = []prompb.Label{
		{Name: "__name__", Value: "test"},
		{Name: "hell", Value: "oworld"},
		{Name: "hello", Value: "world"},
	}
	var keyBuffer = new(bytes.Buffer)
	var metricName, err = generateKey(labels, keyBuffer)

	require.Nil(t, err)

	require.Equal(t, "test", metricName)
	require.Equal(t, []byte("\x08\x00__name__\x04\x00test\x04\x00hell\x06\x00oworld\x05\x00hello\x05\x00world"), keyBuffer.Bytes())
}
