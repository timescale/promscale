package rollup

import "github.com/prometheus/prometheus/promql/parser"

type SqlOptimizer struct {
	columnName, instantQueryAgg string
}

func (s SqlOptimizer) ColumnName() string {
	return s.columnName
}

func (s SqlOptimizer) InstantQueryAgg() string {
	return s.instantQueryAgg
}

type (
	metricType     string
	promqlFuncName string
)

var supportedPromQLFunc = map[metricType]map[promqlFuncName]SqlOptimizer{
	"GAUGE": {
		"min_over_time":   {columnName: "min", instantQueryAgg: "min(value)"},
		"max_over_time":   {columnName: "max", instantQueryAgg: "max(value)"},
		"avg_over_time":   {columnName: "sum / count"},
		"sum_over_time":   {},
		"count_over_time": {},
	},
}

func (c *Config) SupportsFunctionalOptimization(functionalCall parser.Node, metricName string) *SqlOptimizer {
	typ, ok := c.managerRef.metricTypeCache[metricName]
	if !ok {
		// No metadata found, hence no optimization.
		return nil
	}
	node, isFuncCall := functionalCall.(*parser.Call)
	if !isFuncCall {
		return nil
	}
	fnName := node.Func.Name

	supportedOptimizations, found := supportedPromQLFunc[metricType(typ)]
	if !found {
		return nil
	}
	optimizer, ok := supportedOptimizations[promqlFuncName(fnName)]
	if !ok {
		return nil
	}
	return &optimizer
}
