// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package schema

const (
	Tag       = "ps_tag"

	Prom      = "prom_api"
	Data      = "prom_data"
	Info      = "prom_info"
	Exemplar  = "prom_data_exemplar"
	Ext       = "_prom_ext"
	Catalog   = "_prom_catalog"
	Timescale = "public"

	LockID = 0x4D829C732AAFCEDE // Chosen randomly.

	SeriesView = "prom_series"
	MetricView = "prom_metric"
	DataSeries = "prom_data_series"

	Trace       = "_ps_trace"
	TracePublic = "ps_trace"
)
