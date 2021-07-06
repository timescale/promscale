// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/prompb"
)

func TestOrderExemplarLabelsPositionExists(t *testing.T) {
	rawExemplars := []prompb.Exemplar{
		{
			Labels:    []prompb.Label{{Name: "TraceID", Value: "some_trace_id"}, {Name: "component", Value: "tester"}},
			Value:     1.5,
			Timestamp: 1,
		},
		{
			Labels:    []prompb.Label{{Name: "app", Value: "test"}, {Name: "component", Value: "tester"}},
			Value:     2.5,
			Timestamp: 3,
		},
		{
			Labels:    []prompb.Label{}, // No labels. A valid label according to Open Metrics.
			Value:     3.5,
			Timestamp: 5,
		},
	}
	insertable := newExemplarSamples(nil, rawExemplars)
	index := prepareIndex(rawExemplars)
	require.True(t, insertable.OrderExemplarLabels(index))
	rawExemplars = append(rawExemplars, prompb.Exemplar{
		Labels:    []prompb.Label{{Name: "namespace", Value: "default"}},
		Value:     10,
		Timestamp: 10,
	})
	// Index invalid now. Should return positionExists as false, indicating that index needs an update.
	require.False(t, insertable.OrderExemplarLabels(index))
}

func prepareIndex(exemplars []prompb.Exemplar) map[string]int {
	index := make(map[string]int)
	position := 1
	for _, exemplar := range exemplars {
		for i := range exemplar.Labels {
			lbl := exemplar.Labels[i]
			if _, exists := index[lbl.Name]; !exists {
				index[lbl.Name] = position
				position++
			}
		}
	}
	return index
}
