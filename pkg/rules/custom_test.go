package rules

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	prometheus_promql "github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	promscale_promql "github.com/timescale/promscale/pkg/promql"
)

func TestYoloVector(t *testing.T) {
	lbls := []labels.Label{{Name: "__name__", Value: "foo"}, {Name: "instance", Value: "localhost:9201"}}
	promscaleVector := promscale_promql.Vector([]promscale_promql.Sample{
		{
			Point:  promscale_promql.Point{T: 1, V: 1},
			Metric: lbls,
		}, {
			Point:  promscale_promql.Point{T: 2, V: 2},
			Metric: lbls,
		},
	})

	prometheusVector := prometheus_promql.Vector([]prometheus_promql.Sample{
		{
			Point:  prometheus_promql.Point{T: 1, V: 1},
			Metric: lbls,
		}, {
			Point:  prometheus_promql.Point{T: 2, V: 2},
			Metric: lbls,
		},
	})

	gotVector := yoloVector(&promscaleVector)
	require.Equal(t, prometheusVector, gotVector)
}
