package rollup

func (c *Config) GetOptimizer(metricName string) *SqlOptimizer {
	typ, ok := c.managerRef.metricTypeCache[metricName]
	if !ok {
		// No metadata found, hence no optimization.
		return nil
	}
	return &SqlOptimizer{
		typ: metricType(typ),
	}
}

type (
	metricType        string
	promqlFuncName    string
	columnInformation struct {
		columnName      string
		instantQueryAgg string
	}
)

var metricFuncRelation = map[metricType]map[promqlFuncName]columnInformation{
	"GAUGE": {
		"":                {columnName: "sum / count"}, // When no function is used.
		"avg_over_time":   {columnName: "sum, count", instantQueryAgg: "sum(sum) / sum(value)"},
		"min_over_time":   {columnName: "min", instantQueryAgg: "min(value)"},
		"max_over_time":   {columnName: "max", instantQueryAgg: "max(value)"},
		"sum_over_time":   {columnName: "sum", instantQueryAgg: "sum(value)"},
		"count_over_time": {columnName: "count", instantQueryAgg: "sum(value)"}, // Since we want to sum all the counts of each bucket.
	},
}

type SqlOptimizer struct {
	typ metricType
}

func (s *SqlOptimizer) RegularColumnName() string {
	r := metricFuncRelation[s.typ]
	return r[""].columnName
}

func (s *SqlOptimizer) GetColumnClause(funcName string) string {
	r := metricFuncRelation[s.typ]
	c, supported := r[promqlFuncName(funcName)]
	if !supported {
		return ""
	}
	return c.columnName
}

func (s *SqlOptimizer) GetAggForInstantQuery(funcName string) string {
	r := metricFuncRelation[s.typ]
	c, supported := r[promqlFuncName(funcName)]
	if !supported {
		return ""
	}
	return c.instantQueryAgg
}
