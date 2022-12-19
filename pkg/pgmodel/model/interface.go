// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"context"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

const (
	MetricNameLabelName = "__name__"
	SchemaNameLabelName = "__schema__"
	ColumnNameLabelName = "__column__"
)

var (
	MinTime = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()
	MaxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()
)

type Metadata struct {
	MetricFamily string `json:"metric,omitempty"`
	Unit         string `json:"unit"`
	Type         string `json:"type"`
	Help         string `json:"help"`
}

// Dispatcher is responsible for inserting label, series and data into the storage.
type Dispatcher interface {
	InsertTs(ctx context.Context, rows Data) (uint64, error)
	InsertMetadata(context.Context, []Metadata) (uint64, error)
	CompleteMetricCreation(context.Context) error
	Close()
}

func TimestamptzToMs(t pgtype.Timestamptz) int64 {
	switch t.InfinityModifier {
	case pgtype.NegativeInfinity:
		return math.MinInt64
	case pgtype.Infinity:
		return math.MaxInt64
	default:
		return t.Time.UnixNano() / 1e6
	}
}
