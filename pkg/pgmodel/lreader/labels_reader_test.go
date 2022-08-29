// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package lreader

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/tenancy"
)

func TestLabelsReaderLabelsNames(t *testing.T) {
	testCases := []struct {
		name        string
		expectedRes []string
		sqlQueries  []model.SqlQuery
	}{
		{
			name: "Error on query",
			sqlQueries: []model.SqlQuery{
				{
					Sql:     "SELECT distinct key from _prom_catalog.label",
					Args:    []interface{}(nil),
					Results: model.RowResults{},
					Err:     fmt.Errorf("some error"),
				},
			},
		}, {
			name: "Error on scanning values",
			sqlQueries: []model.SqlQuery{
				{
					Sql:     "SELECT distinct key from _prom_catalog.label",
					Args:    []interface{}(nil),
					Results: model.RowResults{{1}},
				},
			},
		}, {
			name: "Empty result, is ok",
			sqlQueries: []model.SqlQuery{
				{
					Sql:     "SELECT distinct key from _prom_catalog.label",
					Args:    []interface{}(nil),
					Results: model.RowResults{},
				},
			},
			expectedRes: []string{},
		}, {
			name: "Result should be sorted",
			sqlQueries: []model.SqlQuery{
				{
					Sql:     "SELECT distinct key from _prom_catalog.label",
					Args:    []interface{}(nil),
					Results: model.RowResults{{"b"}, {"a"}},
				},
			},
			expectedRes: []string{"a", "b"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock := model.NewSqlRecorder(tc.sqlQueries, t)
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
				if err.Error() != "wrong value type int for scan of *string" {
					expectedErr = fmt.Errorf("wrong value type int for scan of *string")
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
		sqlQueries  []model.SqlQuery
		labelName   string
		tenant      tenancy.AuthConfig
	}{
		{
			name: "Error on query",
			sqlQueries: []model.SqlQuery{
				{
					Sql:     "SELECT value from _prom_catalog.label WHERE key = $1",
					Args:    []interface{}{"m"},
					Results: model.RowResults{},
					Err:     fmt.Errorf("some error"),
				},
			},
			labelName: "m",
		}, {
			name: "Error on scanning values",
			sqlQueries: []model.SqlQuery{
				{
					Sql:     "SELECT value from _prom_catalog.label WHERE key = $1",
					Args:    []interface{}{"m"},
					Results: model.RowResults{{1}},
				},
			},
			labelName: "m",
		}, {
			name: "Empty result, is ok",
			sqlQueries: []model.SqlQuery{
				{
					Sql:     "SELECT value from _prom_catalog.label WHERE key = $1",
					Args:    []interface{}{"m"},
					Results: model.RowResults{},
				},
			},
			labelName:   "m",
			expectedRes: []string{},
		}, {
			name: "Result should be sorted",
			sqlQueries: []model.SqlQuery{
				{
					Sql:     "SELECT value from _prom_catalog.label WHERE key = $1",
					Args:    []interface{}{"m"},
					Results: model.RowResults{{"b"}, {"a"}},
				},
			},
			expectedRes: []string{"a", "b"},
			labelName:   "m",
		},
		{
			name: "Tenant values are not filtered when tenant is not configured",
			sqlQueries: []model.SqlQuery{
				{
					Sql:     "SELECT value from _prom_catalog.label WHERE key = $1",
					Args:    []interface{}{tenancy.TenantLabelKey},
					Results: model.RowResults{{"a"}, {"b"}},
				},
			},
			expectedRes: []string{"a", "b"},
			labelName:   tenancy.TenantLabelKey,
		},
		{
			name: "Tenant values are filtered when tenant is configured",
			sqlQueries: []model.SqlQuery{
				{
					Sql:     "SELECT array_agg(distinct a.value) FROM (SELECT * FROM _prom_catalog.label l WHERE EXISTS( SELECT 1 FROM _prom_catalog.series WHERE ( labels @> array[l.id] AND labels @> ( SELECT array_agg(id :: INTEGER) FROM _prom_catalog.label WHERE key = '__tenant__' AND value = $1 )::int[] ) ) ) a WHERE a.key = $2",
					Args:    []interface{}{"a", "__tenant__"},
					Results: model.RowResults{{[]string{"a"}}},
				},
			},
			expectedRes: []string{"a"},
			labelName:   tenancy.TenantLabelKey,
			tenant:      tenancy.NewSelectiveTenancyConfig([]string{"a"}, false, true),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock := model.NewSqlRecorder(tc.sqlQueries, t)
			querier := labelsReader{conn: mock}
			if tc.tenant != nil {
				querier = labelsReader{conn: mock, authConfig: tc.tenant}
			}
			res, err := querier.LabelValues(tc.labelName)

			var expectedErr error
			for _, q := range tc.sqlQueries {
				if q.Err != nil {
					expectedErr = err
					break
				}
			}

			if tc.name == "Error on scanning values" {
				if err.Error() != "wrong value type int for scan of *string" {
					expectedErr = fmt.Errorf("wrong value type int for scan of *string")
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
