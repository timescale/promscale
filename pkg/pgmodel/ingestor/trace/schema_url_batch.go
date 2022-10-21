// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package trace

import (
	"context"
	"fmt"
	"github.com/timescale/promscale/pkg/clockcache"

	"github.com/jackc/pgtype"
	pgx "github.com/jackc/pgx/v4"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const insertSchemaURLSQL = `SELECT ps_trace.put_schema_url($1)`

type schemaURL string

func (s schemaURL) SizeInCache() uint64 {
	return uint64(len(s) + 9) // 9 bytes for pgtype.Int8
}

func (s schemaURL) Before(url sortable) bool {
	if u, ok := url.(schemaURL); ok {
		return s < u
	}
	panic(fmt.Sprintf("cannot use Before function on schemaURL with a different type: %T", url))
}

func (s schemaURL) AddToDBBatch(batch pgxconn.PgxBatch) {
	batch.Queue(insertSchemaURLSQL, s)
}

func (s schemaURL) ScanIDs(r pgx.BatchResults) (pgtype.Int8, error) {
	var id pgtype.Int8
	err := r.QueryRow().Scan(&id)
	return id, err
}

type schemaURLBatch struct {
	b batcher[schemaURL, pgtype.Int8]
}

func newSchemaUrlBatch(cache *clockcache.Cache[schemaURL, pgtype.Int8]) schemaURLBatch {
	return schemaURLBatch{
		b: newBatcher(cache),
	}
}

func (s schemaURLBatch) Queue(url string) {
	if url == "" {
		return
	}
	s.b.Queue(schemaURL(url))
}

func (s schemaURLBatch) SendBatch(ctx context.Context, conn pgxconn.PgxConn) (err error) {
	return s.b.SendBatch(ctx, conn)
}

func (s schemaURLBatch) GetID(url string) (pgtype.Int8, error) {
	if url == "" {
		return pgtype.Int8{Status: pgtype.Null}, nil
	}
	id, err := s.b.Get(schemaURL(url))
	if err != nil {
		return pgtype.Int8{Status: pgtype.Null}, fmt.Errorf("error getting ID for schema url %s: %w", url, err)
	}

	if id.Status != pgtype.Present {
		return pgtype.Int8{Status: pgtype.Null}, fmt.Errorf("error getting ID for schema url %v: %s", url, "ID is null")
	}
	if id.Int == 0 {
		return pgtype.Int8{Status: pgtype.Null}, fmt.Errorf("error getting ID for schema url %v: %s", url, "ID is 0")
	}

	return id, nil
}
