// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"context"
	"fmt"

	"github.com/jackc/pgtype"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgxconn"
)

var (
	// Read-only fields after the ingestor inits.
	labelArrayOID      uint32
	isLabelArrayOIDSet bool
)

func labelArrayTranscoder() pgtype.ValueTranscoder { return new(pgtype.Int4Array) }

func RegisterLabelArrayOID(conn pgxconn.PgxConn) error {
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
)

func labelValueArrayTranscoder() pgtype.ValueTranscoder { return new(pgtype.TextArray) }

func RegisterLabelValueArrayOID(conn pgxconn.PgxConn) error {
	err := conn.QueryRow(context.Background(), `SELECT '`+schema.Prom+`.label_value_array'::regtype::oid`).Scan(&labelValueArrayOID)
	if err != nil {
		return fmt.Errorf("registering prom_api.label_value_array oid: %w", err)
	}
	isLabelValueArrayOIDSet = true
	return nil
}

const (
	LabelArray = iota
	LabelValueArray
)

// GetCustomType returns a custom pgtype.
func GetCustomType(t uint8) *pgtype.ArrayType {
	switch t {
	case LabelArray:
		if !isLabelArrayOIDSet {
			panic("label_array oid is not set. This needs to be set first before calling the type.")
		}
		return pgtype.NewArrayType("prom_api.label_array", labelArrayOID, labelArrayTranscoder)
	case LabelValueArray:
		if !isLabelValueArrayOIDSet {
			panic("label_value_array oid is not set.  This needs to be set first before calling the type.")
		}
		return pgtype.NewArrayType("prom_api.label_value_array", labelValueArrayOID, labelValueArrayTranscoder)
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
