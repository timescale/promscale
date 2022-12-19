package trace

import (
	"context"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgxconn"
)

type cache interface {
	// Get returns the cached value using a key. It also returns false
	// if the value does not exist in cache.
	Get(key interface{}) (interface{}, bool)
	// Insert inserts a value in the cache using the supplied key. It also
	// expect a size of the cached entry (including the key and the value).
	// It returns the canonical value and a flag that indicates if the
	// value already existed in cache.
	Insert(key interface{}, value interface{}, size uint64) (interface{}, bool)
}

type sortable interface {
	Before(sortable) bool
}

type cacheable interface {
	SizeInCache() uint64
}

type batchItem interface {
	sortable
	cacheable
	AddToDBBatch(pgxconn.PgxBatch)
	ScanIDs(pgx.BatchResults) (interface{}, error)
}

type sortableItems []batchItem

func (q sortableItems) Len() int {
	return len(q)
}

func (q sortableItems) Less(i, j int) bool {
	return q[i].Before(q[j])
}

func (q sortableItems) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

// batcher queues up items to send to the DB but it sorts before sending
// this avoids deadlocks in the DB. It also avoids sending the same items repeatedly.
// batcher is not thread safe
type batcher struct {
	batch map[batchItem]interface{}
	cache cache
}

func newBatcher(cache cache) batcher {
	return batcher{
		batch: make(map[batchItem]interface{}),
		cache: cache,
	}
}

// Queue adds item to the batch to be sent or replaces an existing item
func (b batcher) Queue(i batchItem) {
	b.batch[i] = nil
}

// Number of items in the batch.
func (b batcher) Len() int {
	return len(b.batch)
}

// SendBatch sends the batch over the DB connections and gets the ID results.
// Caching is also checked for existance of IDs before sending to the DB to
// avoid unnecessary IO.
func (b batcher) SendBatch(ctx context.Context, conn pgxconn.PgxConn) (err error) {
	batch := make([]batchItem, len(b.batch))
	i := 0
	for q := range b.batch {
		id, ok := b.cache.Get(q)
		if !ok {
			batch[i] = q
			i++
			continue
		}
		b.batch[q] = id
	}

	if i == 0 { // All items cached.
		return nil
	}

	batch = batch[:i]

	sort.Sort(sortableItems(batch))

	dbBatch := conn.NewBatch()
	for _, item := range batch {
		item.AddToDBBatch(dbBatch)
	}

	br, err := conn.SendBatch(ctx, dbBatch)
	if err != nil {
		return err
	}

	defer func() {
		// Only return Close error if there was no previous error.
		if tempErr := br.Close(); err == nil {
			err = tempErr
		}
	}()

	for _, item := range batch {
		id, err := item.ScanIDs(br)
		if err != nil {
			return err
		}
		b.batch[item] = id
		b.cache.Insert(item, id, item.SizeInCache())
	}

	return nil
}

// GetID returns the ID for a batched item in the pgtype.Int8 form.
func (b batcher) GetID(i batchItem) (pgtype.Int8, error) {
	entry, ok := b.batch[i]
	if !ok {
		return pgtype.Int8{Valid: false}, fmt.Errorf("error getting ID from batch")
	}

	id, ok := entry.(pgtype.Int8)
	if !ok {
		return pgtype.Int8{Valid: false}, errors.ErrInvalidCacheEntryType
	}
	if !id.Valid {
		return id, fmt.Errorf("ID is null")
	}
	if id.Int64 == 0 {
		return pgtype.Int8{Valid: false}, fmt.Errorf("ID is 0")
	}
	return id, nil
}

// Get returns the ID for the batch item in whatever the type was used.
func (b batcher) Get(i batchItem) (interface{}, error) {
	entry, ok := b.batch[i]
	if !ok {
		return nil, fmt.Errorf("error getting item from batch")
	}

	return entry, nil
}
