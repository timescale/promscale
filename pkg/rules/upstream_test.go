package rules

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	prometheus_promql "github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	promscale_promql "github.com/timescale/promscale/pkg/promql"
)

func TestVectorCompatibility(t *testing.T) {
	// Note: We check for Sample for incompatibilities and not Vector
	// since Vector is not a struct, rather a type on []Sample. Hence, we
	// use Sample for checks as that's what the Vector relies on.
	typA := reflect.ValueOf(prometheus_promql.Sample{}).Type()
	typB := reflect.ValueOf(promscale_promql.Sample{}).Type()

	numA := typA.NumField()
	numB := typB.NumField()
	require.Equal(t, numB, numA, "number of struct fields mismatch")

	for i := 0; i < numA; i++ {
		require.Equal(t, typB.Field(i).Type.Kind(), typA.Field(i).Type.Kind(), "mismatch in field type at ", fmt.Sprint(i))
	}
}

func TestYoloVector(t *testing.T) {
	cases := []struct {
		name     string
		in       promscale_promql.Vector
		expected prometheus_promql.Vector
	}{
		{
			name: "response",
			in: promscale_promql.Vector([]promscale_promql.Sample{
				{
					Point:  promscale_promql.Point{T: 1, V: 1},
					Metric: []labels.Label{{Name: "__name__", Value: "foo"}, {Name: "instance", Value: "localhost:9201"}},
				}, {
					Point:  promscale_promql.Point{T: 2, V: 2},
					Metric: []labels.Label{{Name: "__name__", Value: "foo"}, {Name: "instance", Value: "localhost:9201"}},
				},
			}),
			expected: prometheus_promql.Vector([]prometheus_promql.Sample{
				{
					Point:  prometheus_promql.Point{T: 1, V: 1},
					Metric: []labels.Label{{Name: "__name__", Value: "foo"}, {Name: "instance", Value: "localhost:9201"}},
				}, {
					Point:  prometheus_promql.Point{T: 2, V: 2},
					Metric: []labels.Label{{Name: "__name__", Value: "foo"}, {Name: "instance", Value: "localhost:9201"}},
				},
			}),
		},
		{
			name: "response with decimals",
			in: promscale_promql.Vector([]promscale_promql.Sample{
				{
					Point:  promscale_promql.Point{T: 10, V: 1.1},
					Metric: []labels.Label{{Name: "__name__", Value: "foo"}},
				}, {
					Point:  promscale_promql.Point{T: 20, V: 2.22},
					Metric: []labels.Label{{Name: "__name__", Value: "foo"}},
				},
			}),
			expected: prometheus_promql.Vector([]prometheus_promql.Sample{
				{
					Point:  prometheus_promql.Point{T: 10, V: 1.1},
					Metric: []labels.Label{{Name: "__name__", Value: "foo"}},
				}, {
					Point:  prometheus_promql.Point{T: 20, V: 2.22},
					Metric: []labels.Label{{Name: "__name__", Value: "foo"}},
				},
			}),
		},
		{
			name:     "nil",
			in:       promscale_promql.Vector(nil),
			expected: prometheus_promql.Vector(nil),
		},
		{
			name:     "empty arrays",
			in:       promscale_promql.Vector([]promscale_promql.Sample{}),
			expected: prometheus_promql.Vector([]prometheus_promql.Sample{}),
		},
		{
			name:     "slice",
			in:       promscale_promql.Vector(make([]promscale_promql.Sample, 0)),
			expected: prometheus_promql.Vector(make([]prometheus_promql.Sample, 0)),
		},
		{
			name: "defaults",
			in: promscale_promql.Vector([]promscale_promql.Sample{
				{
					Point:  promscale_promql.Point{},
					Metric: []labels.Label{{Name: "__name__", Value: "foo"}, {Name: "instance", Value: "localhost:9201"}},
				}, {
					Point:  promscale_promql.Point{},
					Metric: []labels.Label{{Name: "__name__", Value: "foo"}, {Name: "instance", Value: "localhost:9201"}},
				},
			}),
			expected: prometheus_promql.Vector([]prometheus_promql.Sample{
				{
					Point:  prometheus_promql.Point{},
					Metric: []labels.Label{{Name: "__name__", Value: "foo"}, {Name: "instance", Value: "localhost:9201"}},
				}, {
					Point:  prometheus_promql.Point{},
					Metric: []labels.Label{{Name: "__name__", Value: "foo"}, {Name: "instance", Value: "localhost:9201"}},
				},
			}),
		},
	}

	for _, c := range cases {
		gotVector := yoloVector(&c.in)
		require.Equal(t, c.expected, gotVector)
	}
}

// harkishen@harkishen:~/go/src/github.com/timescale/promscale/pkg/rules$ go test -bench . -cpu=1,2,4 -benchtime=100000x
//goos: linux
//goarch: amd64
//pkg: github.com/timescale/promscale/pkg/rules
//cpu: Intel(R) Core(TM) i5-8265U CPU @ 1.60GHz
//BenchmarkVectorConversion/yolo                    100000                 0.001350 ns/op
//BenchmarkVectorConversion/yolo-2                  100000                 0.001100 ns/op
//BenchmarkVectorConversion/yolo-4                  100000                 0.002040 ns/op
//BenchmarkVectorConversion/looping                 100000                 1.021 ns/op
//BenchmarkVectorConversion/looping-2               100000                 0.7710 ns/op
//BenchmarkVectorConversion/looping-4               100000                 0.6590 ns/op
//PASS
//ok      github.com/timescale/promscale/pkg/rules        0.031s
func BenchmarkVectorConversion(b *testing.B) {
	var in promscale_promql.Vector

	for i := 0; i <= 10000; i++ {
		in = append(in, promscale_promql.Sample{Point: promscale_promql.Point{T: int64(100 + i), V: float64(i)}, Metric: labels.Labels{}})
	}

	// Yolo method
	b.Run("yolo", func(b *testing.B) {
		yoloVector(&in)
	})

	vectorConvertor := func(psv promscale_promql.Vector) (promv prometheus_promql.Vector) {
		promv = make(prometheus_promql.Vector, len(psv))
		for i := range psv {
			promv[i].Metric = psv[i].Metric
			promv[i].T = psv[i].T
			promv[i].V = psv[i].V
		}
		return promv
	}
	b.Run("looping", func(b *testing.B) {
		vectorConvertor(in)
	})
}
