// Copyright (c) 2019 The Jaeger Authors.
// Copyright (c) 2017 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is copied from upstream jaeger[1] to run storage integration
// tests as part of promscale e2e.
// [1] https://github.com/jaegertracing/jaeger/blob/4fc291568d8ac59a1c67cc47ee1d91ab20dd06c4/plugin/storage/integration/trace_compare_test.go

// TODO: Remove this file after adding promscale integration storage changes
// into upstream jaeger.

package jaeger_integration_tests

import (
	"encoding/json"
	"testing"

	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jaegertracing/jaeger/model"
)

// CompareSliceOfTraces compares two trace slices
func CompareSliceOfTraces(t *testing.T, expected []*model.Trace, actual []*model.Trace) {
	require.Equal(t, len(expected), len(actual), "Unequal number of expected vs. actual traces")
	model.SortTraces(expected)
	model.SortTraces(actual)
	for i := range expected {
		checkSize(t, expected[i], actual[i])
	}
	if diff := pretty.Diff(expected, actual); len(diff) > 0 {
		for _, d := range diff {
			t.Logf("Expected and actual differ: %s\n", d)
		}
		out, err := json.Marshal(actual)
		out2, err2 := json.Marshal(expected)
		assert.NoError(t, err)
		assert.NoError(t, err2)
		t.Logf("Actual traces: %s", string(out))
		t.Logf("Expected traces: %s", string(out2))
		t.Fail()
	}
}

// CompareTraces compares two traces
func CompareTraces(t *testing.T, expected *model.Trace, actual *model.Trace) {
	if expected.Spans == nil {
		require.Nil(t, actual.Spans)
		return
	}
	require.NotNil(t, actual)
	require.NotNil(t, actual.Spans)
	model.SortTrace(expected)
	model.SortTrace(actual)
	checkSize(t, expected, actual)

	if diff := pretty.Diff(expected, actual); len(diff) > 0 {
		for _, d := range diff {
			t.Logf("Expected and actual differ: %v\n", d)
		}
		out, err := json.Marshal(actual)
		assert.NoError(t, err)
		t.Logf("Actual trace: %s", string(out))
		t.Fail()
	}
}

func checkSize(t *testing.T, expected *model.Trace, actual *model.Trace) {
	require.Equal(t, len(expected.Spans), len(actual.Spans))
	for i := range expected.Spans {
		expectedSpan := expected.Spans[i]
		actualSpan := actual.Spans[i]
		require.True(t, len(expectedSpan.Tags) == len(actualSpan.Tags))
		require.True(t, len(expectedSpan.Logs) == len(actualSpan.Logs))
		if expectedSpan.Process != nil && actualSpan.Process != nil {
			require.True(t, len(expectedSpan.Process.Tags) == len(actualSpan.Process.Tags))
		}
	}
}
