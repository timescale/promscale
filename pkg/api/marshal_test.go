package api

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/timescale/promscale/pkg/promql"
)

func TestMarshalVector(t *testing.T) {
	testCases := []struct {
		name     string
		value    promql.Vector
		warnings []string
	}{
		{
			name:  "empty",
			value: promql.Vector{},
		},
		{
			name: "single_metric",
			value: promql.Vector{
				promql.Sample{
					Point: promql.Point{
						T: 1,
						V: 3.14,
					},
					Metric: labels.Labels{
						{
							Name:  "__name__",
							Value: "nameVal",
						},
					},
				},
			},
		},
		{
			name: "multiple_labels",
			value: promql.Vector{
				promql.Sample{
					Point: promql.Point{
						T: 1,
						V: 3.14,
					},
					Metric: labels.Labels{
						{
							Name:  "__name__",
							Value: "nameVal",
						},
						{
							Name:  "more",
							Value: "333",
						},
					},
				},
			},
		},
		{
			name: "multi_metric",
			value: promql.Vector{
				promql.Sample{
					Point: promql.Point{
						T: 1,
						V: 3.14,
					},
					Metric: labels.Labels{
						{
							Name:  "__name__",
							Value: "nameVal",
						},
					},
				},
				promql.Sample{
					Point: promql.Point{
						T: 1,
						V: 2.7,
					},
					Metric: labels.Labels{
						{
							Name:  "other key",
							Value: "other value",
						},
					},
				},
			},
		},
		{
			name: "duplicate_metric",
			value: promql.Vector{
				promql.Sample{
					Point: promql.Point{
						T: 1,
						V: 3.14,
					},
					Metric: labels.Labels{
						{
							Name:  "__name__",
							Value: "nameVal",
						},
					},
				},
				promql.Sample{
					Point: promql.Point{
						T: 1,
						V: 2.7,
					},
					Metric: labels.Labels{
						{
							Name:  "__name__",
							Value: "nameVal",
						},
					},
				},
			},
		},
		{
			name: "escaped_names_and_warnings",
			value: promql.Vector{
				promql.Sample{
					Point: promql.Point{
						T: 1,
						V: 3.14,
					},
					Metric: labels.Labels{
						{
							Name:  "__\name__",
							Value: "name<val>\n",
						},
					},
				},
			},
			warnings: []string{"test\nwarning", "2nd <warn>"},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(_ *testing.T) {
			builder := strings.Builder{}
			_ = marshalVectorResponse(&builder, testCase.value, testCase.warnings)
			result := builder.String()
			expected := builtinMarshal(testCase.value, testCase.warnings)
			if result != expected {
				t.Errorf("unexpected output\ngot:\n\t%s\nexpected:\n\t%s\n", result, expected)
			}
		})
	}
}

func TestMarshalVectorDuplicates(t *testing.T) {
	// the vector contract says that all samples must have the same time,
	// so we only bother serializing the first one and just reuse it
	// This does cause us to have different output than the default
	// marshaler in this case
	// name: "different_times",
	value := promql.Vector{
		promql.Sample{
			Point: promql.Point{
				T: 1,
				V: 3.14,
			},
			Metric: labels.Labels{
				{
					Name:  "__name__",
					Value: "nameVal",
				},
			},
		},
		promql.Sample{
			Point: promql.Point{
				T: 333,
				V: 2.7,
			},
			Metric: labels.Labels{
				{
					Name:  "other key",
					Value: "other value",
				},
			},
		},
	}
	builder := strings.Builder{}
	_ = marshalVectorResponse(&builder, value, nil)
	result := builder.String()
	expected := `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"nameVal"},"value":[0.001,"3.14"]},{"metric":{"other key":"other value"},"value":[0.001,"2.7"]}]}}` + "\n"
	if result != expected {
		t.Errorf("unexpected output\ngot:\n\t%s\nexpected:\n\t%s\n", result, expected)
	}
}

func TestMarshalMatrix(t *testing.T) {
	testCases := []struct {
		name     string
		value    promql.Matrix
		warnings []string
	}{
		{
			name:  "nil",
			value: promql.Matrix{},
		},
		{
			name:  "empty",
			value: promql.Matrix{},
		},
		{
			name: "empty_data_metric",
			value: promql.Matrix{
				promql.Series{
					Metric: labels.Labels{
						{
							Name:  "__name__",
							Value: "nameVal",
						},
					},
					Points: []promql.Point{},
				},
			},
		},
		{
			name: "single_metric",
			value: promql.Matrix{
				promql.Series{
					Metric: labels.Labels{
						{
							Name:  "__name__",
							Value: "nameVal",
						},
					},
					Points: []promql.Point{
						{
							T: 1,
							V: 3.14,
						},
					},
				},
			},
		},
		{
			name: "multiple_labels",
			value: promql.Matrix{
				promql.Series{
					Metric: labels.Labels{
						{
							Name:  "__name__",
							Value: "nameVal",
						},
						{
							Name:  "more",
							Value: "333",
						},
					},
					Points: []promql.Point{
						{
							T: 1,
							V: 3.14,
						},
					},
				},
			},
		},
		{
			name: "multi_metric",
			value: promql.Matrix{
				promql.Series{
					Metric: labels.Labels{
						{
							Name:  "__name__",
							Value: "nameVal",
						},
					},
					Points: []promql.Point{
						{
							T: 1,
							V: 3.14,
						},
					},
				},
				promql.Series{
					Metric: labels.Labels{
						{
							Name:  "other key",
							Value: "other value",
						},
					},
					Points: []promql.Point{
						{
							T: 3,
							V: 2.7,
						},
					},
				},
			},
		},
		{
			name: "escaped_names_and_warnings",
			value: promql.Matrix{
				promql.Series{
					Metric: labels.Labels{
						{
							Name:  "__\name__",
							Value: "name<val>\n",
						},
					},
					Points: []promql.Point{
						{
							T: 1,
							V: 3.14,
						},
					},
				},
			},
			warnings: []string{"test\nwarning", "2nd <warn>"},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			builder := strings.Builder{}
			_ = marshalMatrixResponse(&builder, testCase.value, testCase.warnings)
			result := builder.String()
			expected := builtinMarshal(testCase.value, testCase.warnings)
			if result != expected {
				t.Errorf("unexpected output\ngot:\n\t%s\nexpected:\n\t%s\n", result, expected)
			}
		})
	}
}

func builtinMarshal(val parser.Value, warnings []string) string {
	resp := &response{
		Status: "success",
		Data: &queryData{
			ResultType: val.Type(),
			Result:     val,
		},
	}
	resp.Warnings = append(resp.Warnings, warnings...)
	builder := new(strings.Builder)
	_ = json.NewEncoder(builder).Encode(resp)
	return builder.String()
}
