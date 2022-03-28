package querier

import (
	"fmt"
	"strings"

	pgmodelcommon "github.com/timescale/promscale/pkg/pgmodel/common/errors"
	pgmodel "github.com/timescale/promscale/pkg/pgmodel/model"
)

// Given a clause with %d placeholder for parameter numbers, and the existing
// and new parameters, return a clause with the parameters set to the
// appropriate $index and the full set of parameter values
func setParameterNumbers(clause string, existingArgs []interface{}, newArgs ...interface{}) (string, []interface{}, error) {
	argIndex := len(existingArgs) + 1
	argCountInClause := strings.Count(clause, "%d")

	if argCountInClause != len(newArgs) {
		return "", nil, fmt.Errorf("invalid number of args: in sql %d vs args %d", argCountInClause, len(newArgs))
	}

	argIndexes := make([]interface{}, 0, argCountInClause)

	for argCountInClause > 0 {
		argIndexes = append(argIndexes, argIndex)
		argIndex++
		argCountInClause--
	}

	newSQL := fmt.Sprintf(clause, argIndexes...)
	resArgs := append(existingArgs, newArgs...)
	return newSQL, resArgs, nil
}

type clauseBuilder struct {
	schemaName    string
	metricName    string
	columnName    string
	contradiction bool
	clauses       []string
	args          []interface{}
}

func (c *clauseBuilder) SetMetricName(name string) {
	if c.metricName == "" {
		c.metricName = name
		return
	}

	/* Impossible to have 2 different metric names at same time */
	if c.metricName != name {
		c.contradiction = true
	}
}

func (c *clauseBuilder) GetMetricName() string {
	return c.metricName
}

func (c *clauseBuilder) SetSchemaName(name string) {
	if c.schemaName == "" {
		c.schemaName = name
		return
	}

	/* Impossible to have 2 different schema names at same time */
	if c.schemaName != name {
		c.contradiction = true
	}
}

func (c *clauseBuilder) GetSchemaName() string {
	return c.schemaName
}

func (c *clauseBuilder) SetColumnName(name string) {
	if c.columnName == "" {
		c.columnName = name
		return
	}

	/* Impossible to have 2 different column names at same time */
	if c.columnName != name {
		c.contradiction = true
	}
}

func (c *clauseBuilder) GetColumnName() string {
	if c.columnName == "" {
		return defaultColumnName
	}
	return c.columnName
}

func (c *clauseBuilder) addClause(clause string, args ...interface{}) error {
	if len(args) > 0 {
		switch args[0] {
		case pgmodel.SchemaNameLabelName:
			return fmt.Errorf("__schema__ label matcher only supports equals matcher")
		case pgmodel.ColumnNameLabelName:
			return fmt.Errorf("__column__ label matcher only supports equals matcher")
		}
	}
	clauseWithParameters, newArgs, err := setParameterNumbers(clause, c.args, args...)
	if err != nil {
		return err
	}

	c.clauses = append(c.clauses, clauseWithParameters)
	c.args = newArgs
	return nil
}

func (c *clauseBuilder) Build(includeMetricName bool) ([]string, []interface{}, error) {
	if c.contradiction {
		return []string{"FALSE"}, nil, nil
	}

	/* no support for queries across all data */
	if len(c.clauses) == 0 && c.metricName == "" {
		return nil, nil, pgmodelcommon.ErrNoClausesGen
	}

	if includeMetricName && c.metricName != "" {
		nameClause, newArgs, err := setParameterNumbers(subQueryEQ, c.args, pgmodel.MetricNameLabelName, c.metricName)
		if err != nil {
			return nil, nil, err
		}
		return append(c.clauses, nameClause), newArgs, err
	}

	if len(c.clauses) == 0 {
		return []string{"TRUE"}, nil, nil
	}
	return c.clauses, c.args, nil
}
