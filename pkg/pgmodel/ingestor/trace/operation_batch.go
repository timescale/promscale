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

const insertOperationSQL = `SELECT %s.put_operation($1, $2, $3)`

type operation struct {
	serviceName string
	spanName    string
	spanKind    string
}

//Operation batch queues up items to send to the db but it sorts before sending
//this avoids deadlocks in the db
type operationBatch map[operation]pgtype.Int8

func newOperationBatch() operationBatch {
	return make(map[operation]pgtype.Int8)
}

func (o operationBatch) Queue(serviceName, spanName, spanKind string) {
	o[operation{serviceName, spanName, spanKind}] = pgtype.Int8{}
}

func (batch operationBatch) SendBatch(ctx context.Context, conn pgxconn.PgxConn) error {
	ops := make([]operation, len(batch))
	i := 0
	for op := range batch {
		ops[i] = op
		i++
	}
	sort.Slice(ops, func(i, j int) bool {
		if ops[i].serviceName != ops[j].serviceName {
			return ops[i].serviceName < ops[j].serviceName
		}
		if ops[i].spanName != ops[j].spanName {
			return ops[i].spanName < ops[j].spanName
		}
		return ops[i].spanKind < ops[j].spanKind
	})

	dbBatch := conn.NewBatch()
	for _, op := range ops {
		dbBatch.Queue(fmt.Sprintf(insertOperationSQL, schema.TracePublic), op.serviceName, op.spanName, op.spanKind)
	}
	br, err := conn.SendBatch(ctx, dbBatch)
	if err != nil {
		return err
	}
	for _, op := range ops {
		var id pgtype.Int8
		if err := br.QueryRow().Scan(&id); err != nil {
			return err
		}
		batch[op] = id
	}
	if err = br.Close(); err != nil {
		return err
	}
	return nil
}
func (batch operationBatch) GetID(serviceName, spanName, spanKind string) (pgtype.Int8, error) {
	id, ok := batch[operation{serviceName, spanName, spanKind}]
	if !ok {
		return pgtype.Int8{Status: pgtype.Null}, fmt.Errorf("operation id not found: %s %s %s", serviceName, spanName, spanKind)
	}
	if id.Status != pgtype.Present {
		return pgtype.Int8{Status: pgtype.Null}, fmt.Errorf("operation id is null")
	}
	if id.Int == 0 {
		return pgtype.Int8{Status: pgtype.Null}, fmt.Errorf("operation id is 0")
	}
	return id, nil
}
