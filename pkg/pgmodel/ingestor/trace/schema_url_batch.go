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

const insertSchemaURLSQL = `SELECT %s.put_schema_url($1)`

type schemaUrl string

//TagBatch queues up items to send to the db but it sorts before sending
//this avoids deadlocks in the db. It also avoids sending the same tags repeatedly.
type schemaUrlBatch map[schemaUrl]pgtype.Int8

func newSchemaUrlBatch() schemaUrlBatch {
	return make(map[schemaUrl]pgtype.Int8)
}

func (batch schemaUrlBatch) Queue(url string) {
	if url != "" {
		batch[schemaUrl(url)] = pgtype.Int8{}
	}
}

func (batch schemaUrlBatch) SendBatch(ctx context.Context, conn pgxconn.PgxConn) error {
	urls := make([]schemaUrl, len(batch))
	i := 0
	for url := range batch {
		urls[i] = url
		i++
	}
	sort.Slice(urls, func(i, j int) bool {
		return urls[i] < urls[j]
	})

	dbBatch := conn.NewBatch()
	for _, sURL := range urls {
		dbBatch.Queue(fmt.Sprintf(insertSchemaURLSQL, schema.TracePublic), sURL)
	}

	br, err := conn.SendBatch(ctx, dbBatch)
	if err != nil {
		return err
	}
	for _, sURL := range urls {
		var id pgtype.Int8
		if err := br.QueryRow().Scan(&id); err != nil {
			return err
		}
		batch[sURL] = id
	}
	if err = br.Close(); err != nil {
		return err
	}
	return nil
}

func (batch schemaUrlBatch) GetID(url string) (pgtype.Int8, error) {
	if url == "" {
		return pgtype.Int8{Status: pgtype.Null}, nil
	}
	id, ok := batch[schemaUrl(url)]
	if !ok {
		return pgtype.Int8{Status: pgtype.Null}, fmt.Errorf("schema url id not found")
	}
	return id, nil
}
