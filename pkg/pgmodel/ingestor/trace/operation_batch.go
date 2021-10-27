// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package trace

import (
	"context"
	"fmt"

	"github.com/jackc/pgtype"
	pgx "github.com/jackc/pgx/v4"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const insertOperationSQL = `SELECT %s.put_operation($1, $2, $3)`

type operation struct {
	serviceName string
	spanName    string
	spanKind    string
}

func (o operation) SizeInCache() uint64 {
	return uint64(len(o.serviceName) + len(o.spanName) + len(o.spanKind) + 9) // 9 bytes for pgtype.Int8
}

func (o operation) Before(item sortable) bool {
	otherOp, ok := item.(operation)
	if !ok {
		panic(fmt.Sprintf("cannot use Before function on operation with a different type: %T", item))
	}
	if o.serviceName != otherOp.serviceName {
		return o.serviceName < otherOp.serviceName
	}
	if o.spanName != otherOp.spanName {
		return o.spanName < otherOp.spanName
	}
	return o.spanKind < otherOp.spanKind
}

func (o operation) AddToDBBatch(batch pgxconn.PgxBatch) {
	batch.Queue(fmt.Sprintf(insertOperationSQL, schema.TracePublic), o.serviceName, o.spanName, o.spanKind)
}

func (o operation) ScanIDs(r pgx.BatchResults) (interface{}, error) {
	var id pgtype.Int8
	err := r.QueryRow().Scan(&id)
	return id, err
}

//Operation batch queues up items to send to the db but it sorts before sending
//this avoids deadlocks in the db
type operationBatch struct {
	b batcher
}

func newOperationBatch(cache cache) operationBatch {
	return operationBatch{
		b: newBatcher(cache),
	}
}

func (o operationBatch) Queue(serviceName, spanName, spanKind string) {
	o.b.Queue(operation{serviceName, spanName, spanKind})
}

func (o operationBatch) SendBatch(ctx context.Context, conn pgxconn.PgxConn) (err error) {
	return o.b.SendBatch(ctx, conn)
}
func (o operationBatch) GetID(serviceName, spanName, spanKind string) (pgtype.Int8, error) {
	op := operation{serviceName, spanName, spanKind}
	id, err := o.b.GetID(op)
	if err != nil {
		return id, fmt.Errorf("error getting ID for operation %v: %w", op, err)
	}

	return id, nil
}
