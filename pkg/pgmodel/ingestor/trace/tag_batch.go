// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package trace

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const (
	insertTagKeySQL = "SELECT ps_trace.put_tag_key($1, $2::ps_trace.tag_type)"
	insertTagSQL    = "SELECT ps_trace.put_tag($1, $2, $3::ps_trace.tag_type)"
)

type tag struct {
	key   string
	value string
	typ   TagType
}

func (t tag) SizeInCache() uint64 {
	return uint64(len(t.key) + len(t.value) + 1 + 18) // 1 byte for TagType and 9 bytes per pgtype.Int8
}

func (t tag) Before(item sortable) bool {
	otherTag, ok := item.(tag)
	if !ok {
		panic(fmt.Sprintf("cannot use Before function on tag with a different type: %T", item))
	}
	if t.key != otherTag.key {
		return t.key < otherTag.key
	}
	if t.value != otherTag.value {
		return t.value < otherTag.value
	}
	return t.typ < otherTag.typ
}

func (t tag) AddToDBBatch(batch pgxconn.PgxBatch) {
	batch.Queue(insertTagKeySQL, t.key, t.typ)
	batch.Queue(insertTagSQL, t.key, t.value, t.typ)
}

func (t tag) ScanIDs(r pgx.BatchResults) (interface{}, error) {
	var id tagIDs
	err := r.QueryRow().Scan(&id.keyID)
	if err != nil {
		return nil, fmt.Errorf("error scanning key ID: %w", err)
	}
	err = r.QueryRow().Scan(&id.valueID)
	if err != nil {
		return nil, fmt.Errorf("error scanning value ID: %w", err)
	}
	return id, nil
}

type tagIDs struct {
	keyID   pgtype.Int8
	valueID pgtype.Int8
}

// tagBatch queues up items to send to the db but it sorts before sending
// this avoids deadlocks in the db. It also avoids sending the same tags repeatedly.
type tagBatch struct {
	b batcher
}

func newTagBatch(cache cache) tagBatch {
	return tagBatch{
		b: newBatcher(cache),
	}
}

func (t tagBatch) Queue(tags map[string]interface{}, typ TagType) error {
	for k, v := range tags {
		byteVal, err := json.Marshal(v)
		if err != nil {
			return err
		}
		t.b.Queue(tag{k, string(byteVal), typ})
	}
	return nil
}

func (t tagBatch) SendBatch(ctx context.Context, conn pgxconn.PgxConn) (err error) {
	return t.b.SendBatch(ctx, conn)
}

func (tb tagBatch) GetTagMapJSON(tags map[string]interface{}, typ TagType) ([]byte, error) {
	tagMap := make(map[int64]int64)
	for k, v := range tags {
		byteVal, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		t := tag{k, string(byteVal), typ}
		ids, err := tb.b.Get(t)
		if err != nil {
			return nil, fmt.Errorf("error getting tag from batch %v: %w", t, err)
		}
		tagIDs, ok := ids.(tagIDs)
		if !ok {
			return nil, fmt.Errorf("error getting tag %v from batch: %w", t, errors.ErrInvalidCacheEntryType)
		}
		if !(tagIDs.keyID.Valid && tagIDs.valueID.Valid) {
			return nil, fmt.Errorf("tag IDs have NULL values: %#v", tagIDs)
		}
		if tagIDs.keyID.Int64 == 0 || tagIDs.valueID.Int64 == 0 {
			return nil, fmt.Errorf("tag IDs have 0 values: %#v", tagIDs)
		}
		tagMap[tagIDs.keyID.Int64] = tagIDs.valueID.Int64
	}

	jsonBytes, err := json.Marshal(tagMap)
	if err != nil {
		return nil, err
	}
	return jsonBytes, nil
}
