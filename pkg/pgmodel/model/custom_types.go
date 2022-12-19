// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

type LabelArray = pgtype.FlatArray[pgtype.Int4]
type ArrayOfLabelArray = pgtype.FlatArray[LabelArray]

var (
	registerTypesMux sync.RWMutex
	// We use a map of slice instead of a single slice because we run some tests
	// in parallel each targeting a different DB. Since each DB might assigns
	// different OIDs to the custom types, if we don't distinguish between
	// them, pgx won't be able to find the corresponding encoding/decoding
	// plan and will return an error.
	registeredTypes = make(map[string][]*pgtype.Type)
)

// RegisterCustomPgTypes registers the custom types specified in the `oidSql`
// query, into the connection's `pgtype.Map`. The types are cached to avoid
// querying the database every time a connection is created.
func RegisterCustomPgTypes(ctx context.Context, conn *pgx.Conn) error {
	cfg := conn.Config().Config
	types, ok := getRegisteredTypes(cfg)
	if !ok {
		var err error
		types, err = fetchCustomPgTypes(ctx, conn)
		if err != nil {
			return fmt.Errorf("couldn't register custom PostgreSQL types: %w", err)
		}
	}
	registerCustomPgTypes(conn.TypeMap(), types)
	return nil
}

// UnRegisterCustomPgTypes deletes the cached types for the given connection.
// This is useful for post test cleanup.
func UnRegisterCustomPgTypes(cfg pgconn.Config) {
	registerTypesMux.Lock()
	defer registerTypesMux.Unlock()
	delete(registeredTypes, key(cfg))
}

func key(cfg pgconn.Config) string {
	return cfg.Host + strconv.FormatUint(uint64(cfg.Port), 10) + cfg.Database
}

var oidsSql = `SELECT
'prom_api.label_array'::regtype::oid,
'prom_api._label_array'::regtype::oid,
'prom_api.label_value_array'::regtype::oid,
'prom_api._label_value_array'::regtype::oid,
'ps_trace.tag_type'::regtype::oid,
'ps_trace.trace_id'::regtype::oid,
'ps_trace.tag_map'::regtype::oid,
'ps_trace._tag_map'::regtype::oid,
'ps_trace.status_code'::regtype::oid
`

func fetchCustomPgTypes(ctx context.Context, conn *pgx.Conn) ([]*pgtype.Type, error) {
	registerTypesMux.Lock()
	defer registerTypesMux.Unlock()

	cfg := conn.Config().Config
	types, ok := registeredTypes[key(cfg)]
	if ok {
		return types, nil
	}

	var (
		// Read-only fields after the ingestor inits.
		labelArrayOID             uint32
		arrayOfLabelArrayOID      uint32
		labelValueArrayOID        uint32
		arrayOfLabelValueArrayOID uint32
		traceTagTypeOID           uint32
		traceIdOID                uint32
		traceTagMapOID            uint32
		arrayOfTraceTagMapOID     uint32
		traceStatusCodeOID        uint32
	)
	err := conn.
		QueryRow(ctx, oidsSql).
		Scan(
			&labelArrayOID,
			&arrayOfLabelArrayOID,
			&labelValueArrayOID,
			&arrayOfLabelValueArrayOID,
			&traceTagTypeOID,
			&traceIdOID,
			&traceTagMapOID,
			&arrayOfTraceTagMapOID,
			&traceStatusCodeOID,
		)
	if err != nil {
		return nil, fmt.Errorf("query to retrieve custom types oids failed: %w", err)
	}

	m := conn.TypeMap()
	// This type is registered with static values, so it's always present
	int4Type, _ := m.TypeForOID(pgtype.Int4OID)
	labelArrayType := &pgtype.Type{
		Name: "_prom_api.label_array",
		OID:  labelArrayOID,
		Codec: &pgtype.ArrayCodec{
			ElementType: int4Type,
		},
	}

	// This type is registered with static values, so it's always present
	textType, _ := m.TypeForOID(pgtype.TextOID)
	labelValueArrayType := &pgtype.Type{
		Name:  "_prom_api.label_value_array",
		OID:   labelValueArrayOID,
		Codec: &pgtype.ArrayCodec{ElementType: textType},
	}
	traceTagMapType := &pgtype.Type{
		Name:  "ps_trace.tag_map",
		OID:   traceTagMapOID,
		Codec: &pgtype.JSONBCodec{},
	}

	types = []*pgtype.Type{
		labelArrayType,
		labelValueArrayType,
		traceTagMapType,
		{
			Name:  "_prom_api._label_array",
			OID:   arrayOfLabelValueArrayOID,
			Codec: &pgtype.ArrayCodec{ElementType: labelValueArrayType},
		},
		{
			Name:  "_prom_api._label_array",
			OID:   arrayOfLabelArrayOID,
			Codec: &pgtype.ArrayCodec{ElementType: labelArrayType},
		},
		{
			Name:  "ps_trace.tag_type",
			OID:   traceTagTypeOID,
			Codec: &pgtype.Int2Codec{},
		},
		{
			Name:  "ps_trace.trace_id",
			OID:   traceIdOID,
			Codec: &pgtype.UUIDCodec{},
		},
		{
			Name:  "ps_trace._tag_map",
			OID:   arrayOfTraceTagMapOID,
			Codec: &pgtype.ArrayCodec{ElementType: traceTagMapType},
		},
		{
			Name:  "ps_trace.status_code",
			OID:   traceStatusCodeOID,
			Codec: &pgtype.EnumCodec{},
		},
	}

	registeredTypes[key(conn.Config().Config)] = types

	return types, nil
}

func getRegisteredTypes(cfg pgconn.Config) ([]*pgtype.Type, bool) {
	registerTypesMux.RLock()
	defer registerTypesMux.RUnlock()
	types, ok := registeredTypes[key(cfg)]
	return types, ok
}

func registerCustomPgTypes(m *pgtype.Map, types []*pgtype.Type) {
	for _, t := range types {
		m.RegisterType(t)
	}
}

func SliceToArrayOfLabelArray(src [][]int32) ArrayOfLabelArray {
	a := make(ArrayOfLabelArray, 0, len(src))
	for _, i := range src {
		la := make(LabelArray, 0, len(i))
		for _, j := range i {
			la = append(la, pgtype.Int4{Int32: j, Valid: true})
		}
		a = append(a, la)
	}
	return a
}

// Wrapper to allow DecodeBinary to reuse the existing array so that a pool is
// effective
type ReusableArray[T any] struct {
	pgtype.FlatArray[T]
}

func (a *ReusableArray[T]) SetDimensions(dimensions []pgtype.ArrayDimension) error {
	if dimensions == nil {
		a.FlatArray = nil
		return nil
	}

	elementCount := cardinality(dimensions)

	// Reuse the current array if it's capable to support the new dimensions
	// constraint. Otherwise, create a new array.
	if cap(a.FlatArray) > int(elementCount) {
		a.FlatArray = a.FlatArray[:elementCount]
	} else {
		a.FlatArray = make(pgtype.FlatArray[T], elementCount)
	}

	return nil
}

// Cardinality returns the number of elements in an array of dimensions size.
func cardinality(dimensions []pgtype.ArrayDimension) int {
	if len(dimensions) == 0 {
		return 0
	}

	elementCount := int(dimensions[0].Length)
	for _, d := range dimensions[1:] {
		elementCount *= int(d.Length)
	}

	return elementCount
}
