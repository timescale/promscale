// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package trace

import (
	"context"
	"fmt"
	"sort"

	"github.com/jackc/pgtype"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const insertInstrumentationLibSQL = `SELECT %s.put_instrumentation_lib($1, $2, $3)`

type instrumentationLibrary struct {
	name        string
	version     string
	SchemaUrlID pgtype.Int8
}

//TagBatch queues up items to send to the db but it sorts before sending
//this avoids deadlocks in the db. It also avoids sending the same tags repeatedly.
type instrumentationLibraryBatch map[instrumentationLibrary]int64

func newInstrumentationLibraryBatch() instrumentationLibraryBatch {
	return make(map[instrumentationLibrary]int64)
}

func (batch instrumentationLibraryBatch) Queue(name, version string, schemaUrlID pgtype.Int8) {
	if name != "" {
		batch[instrumentationLibrary{name, version, schemaUrlID}] = 0
	}
}

func (batch instrumentationLibraryBatch) SendBatch(ctx context.Context, conn pgxconn.PgxConn) error {
	libs := make([]instrumentationLibrary, len(batch))
	i := 0
	for lib := range batch {
		libs[i] = lib
		i++
	}
	sort.Slice(libs, func(i, j int) bool {
		if libs[i].name == libs[j].name {
			if libs[i].version == libs[j].version {
				if libs[i].SchemaUrlID.Status == libs[j].SchemaUrlID.Status {
					return libs[i].SchemaUrlID.Int < libs[j].SchemaUrlID.Int
				}
				return libs[i].SchemaUrlID.Status < libs[j].SchemaUrlID.Status
			}
			return libs[i].version < libs[j].version
		}
		return libs[i].version < libs[j].version
	})

	dbBatch := conn.NewBatch()
	for _, lib := range libs {
		dbBatch.Queue(fmt.Sprintf(insertInstrumentationLibSQL, schema.TracePublic), lib.name, lib.version, lib.SchemaUrlID)
	}

	br, err := conn.SendBatch(ctx, dbBatch)
	if err != nil {
		return err
	}
	for _, lib := range libs {
		var id int64
		if err := br.QueryRow().Scan(&id); err != nil {
			return err
		}
		batch[lib] = id
	}
	if err = br.Close(); err != nil {
		return err
	}
	return nil
}

func (batch instrumentationLibraryBatch) GetID(name, version string, schemaUrlID pgtype.Int8) (pgtype.Int8, error) {
	if name == "" {
		return pgtype.Int8{Status: pgtype.Null}, nil
	}
	id, ok := batch[instrumentationLibrary{name, version, schemaUrlID}]
	if id == 0 || !ok {
		return pgtype.Int8{Status: pgtype.Null}, fmt.Errorf("instrumention library id not found: %s %s", name, version)
	}
	return pgtype.Int8{Int: id, Status: pgtype.Present}, nil
}
