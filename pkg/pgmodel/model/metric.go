package model

// MetricInfo contains all the database specific metric data.
type MetricInfo struct {
	TableSchema, TableName, SeriesTable string
}

// Len returns the memory size of MetricInfo in bytes.
func (v MetricInfo) Len() int {
	return len(v.TableSchema) + len(v.TableName) + len(v.SeriesTable)
}
