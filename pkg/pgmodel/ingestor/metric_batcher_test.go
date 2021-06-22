package ingestor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgmodel/model"
)

func TestMetricTableName(t *testing.T) {
	testCases := []struct {
		name        string
		tableName   string
		errExpected bool
		sqlQueries  []model.SqlQuery
	}{
		{
			name:      "no error",
			tableName: "res1",
			sqlQueries: []model.SqlQuery{
				{
					Sql:     "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"t1"},
					Results: model.RowResults{{"res1", true}},
				},
			},
		},
		{
			name:      "no error2",
			tableName: "res2",
			sqlQueries: []model.SqlQuery{
				{
					Sql:     "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"t1"},
					Results: model.RowResults{{"res2", true}},
				},
			},
		},
		{
			name:        "error",
			tableName:   "res1",
			errExpected: true,
			sqlQueries: []model.SqlQuery{
				{
					Sql:  "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args: []interface{}{"t1"},
					Err:  fmt.Errorf("test"),
				},
			},
		},
		{
			name:        "empty table name",
			tableName:   "res2",
			errExpected: true,
			sqlQueries: []model.SqlQuery{
				{
					Sql:     "SELECT table_name, possibly_new FROM _prom_catalog.get_or_create_metric_table_name($1)",
					Args:    []interface{}{"t1"},
					Results: model.RowResults{{"", true}},
				},
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			mock := model.NewSqlRecorder(c.sqlQueries, t)

			name, _, err := metricTableName(mock, "t1")
			require.Equal(t, c.errExpected, err != nil)

			if err == nil {
				require.Equal(t, c.tableName, name)
			}
		})
	}
}
