// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package trace

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const insertInstrumentationLibSQL = `SELECT ps_trace.put_instrumentation_lib($1, $2, $3)`

type instrumentationLibrary struct {
	name        string
	version     string
	schemaURLID pgtype.Int8
}

func (il instrumentationLibrary) SizeInCache() uint64 {
	return uint64(len(il.name) + len(il.version) + 18) // 9 bytes per pgtype.Int8
}

func (il instrumentationLibrary) Before(item sortable) bool {
	otherIl, ok := item.(instrumentationLibrary)
	if !ok {
		panic(fmt.Sprintf("cannot use Before method on instrumentationLibrary with a different type: %T", item))
	}
	if il.name != otherIl.name {
		return il.name < otherIl.name
	}
	if il.version != otherIl.version {
		return il.version < otherIl.version
	}
	if il.schemaURLID.Valid != otherIl.schemaURLID.Valid {
		return !il.schemaURLID.Valid
	}
	return il.schemaURLID.Int64 < otherIl.schemaURLID.Int64
}

func (il instrumentationLibrary) AddToDBBatch(batch pgxconn.PgxBatch) {
	batch.Queue(insertInstrumentationLibSQL, il.name, il.version, il.schemaURLID)
}

func (il instrumentationLibrary) ScanIDs(r pgx.BatchResults) (interface{}, error) {
	var id pgtype.Int8
	err := r.QueryRow().Scan(&id)
	return id, err
}

// instrumentationLibraryBatch queues up items to send to the db but it sorts before sending
// this avoids deadlocks in the db
type instrumentationLibraryBatch struct {
	b batcher
}

func newInstrumentationLibraryBatch(cache cache) instrumentationLibraryBatch {
	return instrumentationLibraryBatch{
		b: newBatcher(cache),
	}
}

func (lib instrumentationLibraryBatch) Queue(name, version string, schemaURLID pgtype.Int8) {
	if name == "" {
		return
	}
	lib.b.Queue(instrumentationLibrary{name, version, schemaURLID})
}

func (lib instrumentationLibraryBatch) SendBatch(ctx context.Context, conn pgxconn.PgxConn) (err error) {
	return lib.b.SendBatch(ctx, conn)
}
func (lib instrumentationLibraryBatch) GetID(name, version string, schemaURLID pgtype.Int8) (pgtype.Int8, error) {
	if name == "" {
		return pgtype.Int8{Valid: false}, nil
	}
	il := instrumentationLibrary{name, version, schemaURLID}
	id, err := lib.b.GetID(il)
	if err != nil {
		return id, fmt.Errorf("error getting ID for instrumentation library %v: %w", il, err)
	}

	return id, nil
}
