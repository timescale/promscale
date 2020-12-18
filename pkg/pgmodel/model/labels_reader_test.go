// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"

	promLabels "github.com/prometheus/prometheus/pkg/labels"
)

func TestBigLables(t *testing.T) {
	builder := strings.Builder{}
	builder.Grow(int(^uint16(0)) + 1) // one greater than uint16 max

	builder.WriteByte('a')
	for len(builder.String()) < math.MaxUint16 {
		builder.WriteString(builder.String())
	}

	l := promLabels.Labels{
		promLabels.Label{
			Name:  builder.String(),
			Value: "",
		},
	}

	_, err := LabelsFromSlice(l)
	if err == nil {
		t.Errorf("expected error")
	}
}

func TestLabelsReaderLabelsNames(t *testing.T) {
	testCases := []struct {
		name        string
		expectedRes []string
		sqlQueries  []SqlQuery
	}{
		{
			name: "Error on query",
			sqlQueries: []SqlQuery{
				{
					Sql:     "SELECT distinct key from _prom_catalog.label",
					Args:    []interface{}(nil),
					Results: RowResults{},
					Err:     fmt.Errorf("some error"),
				},
			},
		}, {
			name: "Error on scanning values",
			sqlQueries: []SqlQuery{
				{
					Sql:     "SELECT distinct key from _prom_catalog.label",
					Args:    []interface{}(nil),
					Results: RowResults{{1}},
				},
			},
		}, {
			name: "Empty result, is ok",
			sqlQueries: []SqlQuery{
				{
					Sql:     "SELECT distinct key from _prom_catalog.label",
					Args:    []interface{}(nil),
					Results: RowResults{},
				},
			},
			expectedRes: []string{},
		}, {
			name: "Result should be sorted",
			sqlQueries: []SqlQuery{
				{
					Sql:     "SELECT distinct key from _prom_catalog.label",
					Args:    []interface{}(nil),
					Results: RowResults{{"b"}, {"a"}},
				},
			},
			expectedRes: []string{"a", "b"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewSqlRecorder(tc.sqlQueries, t)
			reader := labelsReader{conn: mock}
			res, err := reader.LabelNames()

			var expectedErr error
			for _, q := range tc.sqlQueries {
				if q.Err != nil {
					expectedErr = err
					break
				}
			}

			if tc.name == "Error on scanning values" {
				if err.Error() != "wrong value type int" {
					expectedErr = fmt.Errorf("wrong value type int")
					t.Errorf("unexpected error\n got: %v\n expected: %v", err, expectedErr)
					return
				}
			} else if expectedErr != err {
				t.Errorf("unexpected error\n got: %v\n expected: %v", err, expectedErr)
				return
			}

			outputIsSorted := sort.SliceIsSorted(res, func(i, j int) bool {
				return res[i] < res[j]
			})
			if !outputIsSorted {
				t.Errorf("returned label names %v are not sorted", res)
			}

			if !reflect.DeepEqual(tc.expectedRes, res) {
				t.Errorf("expected: %v, got: %v", tc.expectedRes, res)
			}
		})
	}
}

func TestLabelsReaderLabelsValues(t *testing.T) {
	testCases := []struct {
		name        string
		expectedRes []string
		sqlQueries  []SqlQuery
	}{
		{
			name: "Error on query",
			sqlQueries: []SqlQuery{
				{
					Sql:     "SELECT value from _prom_catalog.label WHERE key = $1",
					Args:    []interface{}{"m"},
					Results: RowResults{},
					Err:     fmt.Errorf("some error"),
				},
			},
		}, {
			name: "Error on scanning values",
			sqlQueries: []SqlQuery{
				{
					Sql:     "SELECT value from _prom_catalog.label WHERE key = $1",
					Args:    []interface{}{"m"},
					Results: RowResults{{1}},
				},
			},
		}, {
			name: "Empty result, is ok",
			sqlQueries: []SqlQuery{
				{
					Sql:     "SELECT value from _prom_catalog.label WHERE key = $1",
					Args:    []interface{}{"m"},
					Results: RowResults{},
				},
			},
			expectedRes: []string{},
		}, {
			name: "Result should be sorted",
			sqlQueries: []SqlQuery{
				{
					Sql:     "SELECT value from _prom_catalog.label WHERE key = $1",
					Args:    []interface{}{"m"},
					Results: RowResults{{"b"}, {"a"}},
				},
			},
			expectedRes: []string{"a", "b"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewSqlRecorder(tc.sqlQueries, t)
			querier := labelsReader{conn: mock}
			res, err := querier.LabelValues("m")

			var expectedErr error
			for _, q := range tc.sqlQueries {
				if q.Err != nil {
					expectedErr = err
					break
				}
			}

			if tc.name == "Error on scanning values" {
				if err.Error() != "wrong value type int" {
					expectedErr = fmt.Errorf("wrong value type int")
					t.Errorf("unexpected error\n got: %v\n expected: %v", err, expectedErr)
					return
				}
			} else if expectedErr != err {
				t.Errorf("unexpected error\n got: %v\n expected: %v", err, expectedErr)
				return
			}

			outputIsSorted := sort.SliceIsSorted(res, func(i, j int) bool {
				return res[i] < res[j]
			})
			if !outputIsSorted {
				t.Errorf("returned label names %v are not sorted", res)
			}

			if !reflect.DeepEqual(tc.expectedRes, res) {
				t.Errorf("expected: %v, got: %v", tc.expectedRes, res)
			}
		})
	}
}
