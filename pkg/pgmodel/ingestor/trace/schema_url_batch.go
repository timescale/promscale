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

type schemaURL string

//schemaURLBatch queues up items to send to the DB but it sorts before sending
//this avoids deadlocks in the DB. It also avoids sending the same URLs repeatedly.
type schemaURLBatch map[schemaURL]pgtype.Int8

func newSchemaUrlBatch() schemaURLBatch {
	return make(map[schemaURL]pgtype.Int8)
}

func (batch schemaURLBatch) Queue(url string) {
	if url != "" {
		batch[schemaURL(url)] = pgtype.Int8{}
	}
}

func (batch schemaURLBatch) SendBatch(ctx context.Context, conn pgxconn.PgxConn) error {
	urls := make([]schemaURL, len(batch))
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

func (batch schemaURLBatch) GetID(url string) (pgtype.Int8, error) {
	if url == "" {
		return pgtype.Int8{Status: pgtype.Null}, nil
	}
	id, ok := batch[schemaURL(url)]
	if !ok {
		return pgtype.Int8{Status: pgtype.Null}, fmt.Errorf("schema url id not found")
	}
	if id.Status != pgtype.Present {
		return pgtype.Int8{Status: pgtype.Null}, fmt.Errorf("schema url id is null")
	}
	if id.Int == 0 {
		return pgtype.Int8{Status: pgtype.Null}, fmt.Errorf("schema url id is 0")
	}
	return id, nil
}
