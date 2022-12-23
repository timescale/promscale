// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package schema

import (
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	PromData         = "prom_data"
	PromDataExemplar = "prom_data_exemplar"
	PromExt          = "_prom_ext"
	// Public is where all timescaledb-functions are loaded
	Public = "public"

	LockID = 5585198506344173278 // Chosen randomly.

	PromDataSeries = "prom_data_series"
	PsTrace        = "_ps_trace"
)

var (
	PromDataColumns     = []string{"time", "value", "series_id"}
	PromDataColumnsOIDs = []uint32{
		pgtype.TimestamptzOID,
		pgtype.Float8OID,
		pgtype.Int8OID,
	}
	PromExemplarColumns = []string{"time", "series_id", "exemplar_label_values", "value"}
)

func PromExemplarColumnsOIDs(typeMap *pgtype.Map) ([]uint32, bool) {
	labelValueArrayType, ok := typeMap.TypeForName("_prom_api.label_value_array")
	if !ok {
		return []uint32{}, ok
	}
	return []uint32{
		pgtype.TimestamptzOID,
		pgtype.Int8OID,
		labelValueArrayType.OID,
		pgtype.Float8OID,
	}, ok
}
