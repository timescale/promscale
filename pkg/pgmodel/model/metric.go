package model

// MetricInfo contains all the database specific metric data.
type MetricInfo struct {
	MetricID                            int64
	TableSchema, TableName, SeriesTable string
}

// Len returns the memory size of MetricInfo in bytes.
func (v MetricInfo) Len() int {
	return 8 + len(v.TableSchema) + len(v.TableName) + len(v.SeriesTable)
}
