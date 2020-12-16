package utils

import (
	"fmt"
	"github.com/timescale/promscale/pkg/pgmodel"
	"reflect"
	"sort"
	"testing"
)

func TestLabelsReaderLabelsNames(t *testing.T) {
	testCases := []struct {
		name        string
		expectedRes []string
		sqlQueries  []pgmodel.sqlQuery
	}{
		{
			name: "Error on query",
			sqlQueries: []pgmodel.sqlQuery{
				{
					sql:     "SELECT distinct key from _prom_catalog.label",
					args:    []interface{}(nil),
					results: pgmodel.rowResults{},
					err:     fmt.Errorf("some error"),
				},
			},
		}, {
			name: "Error on scanning values",
			sqlQueries: []pgmodel.sqlQuery{
				{
					sql:     "SELECT distinct key from _prom_catalog.label",
					args:    []interface{}(nil),
					results: pgmodel.rowResults{{1}},
				},
			},
		}, {
			name: "Empty result, is ok",
			sqlQueries: []pgmodel.sqlQuery{
				{
					sql:     "SELECT distinct key from _prom_catalog.label",
					args:    []interface{}(nil),
					results: pgmodel.rowResults{},
				},
			},
			expectedRes: []string{},
		}, {
			name: "Result should be sorted",
			sqlQueries: []pgmodel.sqlQuery{
				{
					sql:     "SELECT distinct key from _prom_catalog.label",
					args:    []interface{}(nil),
					results: pgmodel.rowResults{{"b"}, {"a"}},
				},
			},
			expectedRes: []string{"a", "b"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock := pgmodel.newSqlRecorder(tc.sqlQueries, t)
			reader := labelsReader{conn: mock}
			res, err := reader.LabelNames()

			var expectedErr error
			for _, q := range tc.sqlQueries {
				if q.err != nil {
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
		sqlQueries  []pgmodel.sqlQuery
	}{
		{
			name: "Error on query",
			sqlQueries: []pgmodel.sqlQuery{
				{
					sql:     "SELECT value from _prom_catalog.label WHERE key = $1",
					args:    []interface{}{"m"},
					results: pgmodel.rowResults{},
					err:     fmt.Errorf("some error"),
				},
			},
		}, {
			name: "Error on scanning values",
			sqlQueries: []pgmodel.sqlQuery{
				{
					sql:     "SELECT value from _prom_catalog.label WHERE key = $1",
					args:    []interface{}{"m"},
					results: pgmodel.rowResults{{1}},
				},
			},
		}, {
			name: "Empty result, is ok",
			sqlQueries: []pgmodel.sqlQuery{
				{
					sql:     "SELECT value from _prom_catalog.label WHERE key = $1",
					args:    []interface{}{"m"},
					results: pgmodel.rowResults{},
				},
			},
			expectedRes: []string{},
		}, {
			name: "Result should be sorted",
			sqlQueries: []pgmodel.sqlQuery{
				{
					sql:     "SELECT value from _prom_catalog.label WHERE key = $1",
					args:    []interface{}{"m"},
					results: pgmodel.rowResults{{"b"}, {"a"}},
				},
			},
			expectedRes: []string{"a", "b"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock := pgmodel.newSqlRecorder(tc.sqlQueries, t)
			querier := labelsReader{conn: mock}
			res, err := querier.LabelValues("m")

			var expectedErr error
			for _, q := range tc.sqlQueries {
				if q.err != nil {
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
