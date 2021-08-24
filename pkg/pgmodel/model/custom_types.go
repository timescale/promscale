// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgtype"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgxconn"
)

var (
	// Read-only fields after the ingestor inits.
	labelArrayOID      uint32
	isLabelArrayOIDSet bool
	labelArrayOIDMux   sync.Mutex
)

func labelArrayTranscoder() pgtype.ValueTranscoder { return new(pgtype.Int4Array) }

func registerLabelArrayOID(conn pgxconn.PgxConn) error {
	labelArrayOIDMux.Lock()
	defer labelArrayOIDMux.Unlock()
	err := conn.QueryRow(context.Background(), `SELECT '`+schema.Prom+`.label_array'::regtype::oid`).Scan(&labelArrayOID)
	if err != nil {
		return fmt.Errorf("registering prom_api.label_array oid: %w", err)
	}
	isLabelArrayOIDSet = true
	return nil
}

var (
	// Read-only fields after the ingestor inits.
	labelValueArrayOID      uint32
	isLabelValueArrayOIDSet bool
	labelValueArrayOIDMux   sync.Mutex
)

func labelValueArrayTranscoder() pgtype.ValueTranscoder { return new(pgtype.TextArray) }

func registerLabelValueArrayOID(conn pgxconn.PgxConn) error {
	labelValueArrayOIDMux.Lock()
	defer labelValueArrayOIDMux.Unlock()
	err := conn.QueryRow(context.Background(), `SELECT '`+schema.Prom+`.label_value_array'::regtype::oid`).Scan(&labelValueArrayOID)
	if err != nil {
		return fmt.Errorf("registering prom_api.label_value_array oid: %w", err)
	}
	isLabelValueArrayOIDSet = true
	return nil
}

func RegisterCustomPgTypes(conn pgxconn.PgxConn) error {
	var err error
	if err = registerLabelArrayOID(conn); err != nil {
		return fmt.Errorf("register label array oid: %w", err)
	}
	if err = registerLabelValueArrayOID(conn); err != nil {
		return fmt.Errorf("register label value array oid: %w", err)
	}
	return nil
}

type PgCustomType uint8

const (
	LabelArray PgCustomType = iota
	LabelValueArray
)

func GetCustomTypeOID(t PgCustomType) uint32 {
	switch t {
	case LabelArray:
		labelArrayOIDMux.Lock()
		defer labelArrayOIDMux.Unlock()
		if !isLabelArrayOIDSet {
			panic("label_array oid is not set. This needs to be set first before calling the type.")
		}
		return labelArrayOID
	case LabelValueArray:
		labelValueArrayOIDMux.Lock()
		defer labelValueArrayOIDMux.Unlock()
		if !isLabelValueArrayOIDSet {
			panic("label_value_array oid is not set.  This needs to be set first before calling the type.")
		}
		return labelValueArrayOID
	default:
		panic("invalid type")
	}
}

// GetCustomType returns a custom pgtype.
func GetCustomType(t PgCustomType) *pgtype.ArrayType {
	switch t {
	case LabelArray:
		if !isLabelArrayOIDSet {
			panic("label_array oid is not set. This needs to be set first before calling the type.")
		}
		return pgtype.NewArrayType("prom_api.label_array", GetCustomTypeOID(t), labelArrayTranscoder)
	case LabelValueArray:
		if !isLabelValueArrayOIDSet {
			panic("label_value_array oid is not set.  This needs to be set first before calling the type.")
		}
		return pgtype.NewArrayType("prom_api.label_value_array", GetCustomTypeOID(t), labelValueArrayTranscoder)
	default:
		panic("invalid type")
	}
}

func SetLabelArrayOIDForTest(oid uint32) {
	labelArrayOID = oid
	isLabelArrayOIDSet = true

	labelValueArrayOID = oid
	isLabelValueArrayOIDSet = true
}
