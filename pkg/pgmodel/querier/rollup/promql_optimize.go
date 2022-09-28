package rollup

type (
	metricType        string
	columnInformation struct {
		columnName          string
		instantQueryAgg     string
		onlyForInstantQuery bool
	}
)

var instantQueryFuncColumn = map[metricType]map[string]columnInformation{ // Relationship=> metric_type: PromQL_function_name: info_on_which_rollup_column_to_use
	"GAUGE": {
		"":                {columnName: "sum / count"}, // When no function is used.
		"avg_over_time":   {columnName: "sum, count", instantQueryAgg: "sum(sum) / sum(value)"},
		"min_over_time":   {columnName: "min", instantQueryAgg: "min(value)"},
		"max_over_time":   {columnName: "max", instantQueryAgg: "max(value)"},
		"sum_over_time":   {columnName: "sum", instantQueryAgg: "sum(value)"},
		"count_over_time": {columnName: "count", instantQueryAgg: "sum(value)"}, // Since we want to sum all the counts of each bucket.
	},
	"COUNTER": {
		"": {columnName: "last_with_counter_reset"},
	},
	"HISTOGRAM": {
		"": {columnName: "last_with_counter_reset"},
	},
	"SUMMARY": {
		"":                {columnName: "sum / count"},
		"avg_over_time":   {columnName: "sum, count", instantQueryAgg: "sum(sum) / sum(value)"},
		"min_over_time":   {columnName: "sum / count", instantQueryAgg: "min(value)"},
		"max_over_time":   {columnName: "sum / count", instantQueryAgg: "max(value)"},
		"sum_over_time":   {columnName: "sum", instantQueryAgg: "sum(value)"},
		"count_over_time": {columnName: "count", instantQueryAgg: "sum(value)"},
	},
}

var rangeQueryFuncColumn = map[metricType]map[string]string{
	"GAUGE": {
		"":                "sum / count", // When no function is used.
		"min_over_time":   "min",
		"max_over_time":   "max",
		"sum_over_time":   "sum",
		"count_over_time": "count",
	},
	"COUNTER": {
		"": "last_with_counter_reset",
	},
	"HISTOGRAM": {
		"": "last_with_counter_reset",
	},
	"SUMMARY": {
		"":                "sum / count",
		"avg_over_time":   "sum, count",
		"sum_over_time":   "sum",
		"count_over_time": "count",
	},
}

type Optimizer interface {
	RegularColumnName() string
	GetColumnClause(funcName string) string
}

type SqlOptimizer struct {
	typ metricType
}

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

func (o *SqlOptimizer) InstantQuery() InstantQuery {
	return InstantQuery{o.typ}
}

func (o *SqlOptimizer) RangeQuery() RangeQuery {
	return RangeQuery{o.typ}
}

type InstantQuery struct {
	typ metricType
}

func (i InstantQuery) RegularColumnName() string {
	return instantQueryFuncColumn[i.typ][""].columnName
}

func (i InstantQuery) GetColumnClause(funcName string) string {
	clause, exists := instantQueryFuncColumn[i.typ][funcName]
	if !exists {
		return ""
	}
	return clause.columnName
}

func (i InstantQuery) GetAggForInstantQuery(funcName string) string {
	r := instantQueryFuncColumn[i.typ]
	c, supported := r[funcName]
	if !supported {
		return ""
	}
	return c.instantQueryAgg
}

type RangeQuery struct {
	typ metricType
}

func (r RangeQuery) RegularColumnName() string {
	return rangeQueryFuncColumn[r.typ][""]
}

func (r RangeQuery) GetColumnClause(funcName string) string {
	clause, exists := rangeQueryFuncColumn[r.typ][funcName]
	if !exists {
		return ""
	}
	return clause
}
