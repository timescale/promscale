package end_to_end_tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/prompb"
)

func TestInsertExemplars(t *testing.T) {
	ts := []prompb.TimeSeries{
		{
			Samples: []prompb.Sample{{Timestamp: 0, Value: 1}, {Timestamp: 1, Value: 2}},
			Labels:  []prompb.Label{{Name: "__name__", Value: "test_metric"}},
		},
		{
			Samples: []prompb.Sample{{Timestamp: 2, Value: 3}, {Timestamp: 3, Value: 4}},
			Labels:  []prompb.Label{{Name: "__name__", Value: "test_metric"}},
		},
		{
			Samples: []prompb.Sample{{Timestamp: 4, Value: 5}, {Timestamp: 5, Value: 6}},
			Labels:  []prompb.Label{{Name: "__name__", Value: "test_metric"}},
		},
		{
			Samples: []prompb.Sample{{Timestamp: 6, Value: 7}, {Timestamp: 7, Value: 8}},
			Labels:  []prompb.Label{{Name: "__name__", Value: "test_metric"}},
		},
	}
	output := insertExemplars(ts, 2)
	require.Equal(t, nonNullTsExemplar(output), 2)
}

func nonNullTsExemplar(ts []prompb.TimeSeries) (num int) {
	for _, t := range ts {
		if t.Exemplars != nil {
			num++
		}
	}
	return
}
