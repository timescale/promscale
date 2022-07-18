package trace

import (
	"context"
	"fmt"
	"github.com/timescale/promscale/pkg/clockcache"
	"sort"

	"github.com/jackc/pgx/v4"
	"github.com/timescale/promscale/pkg/pgxconn"
)

type sortable interface {
	Before(sortable) bool
}

type cacheable interface {
	SizeInCache() uint64
}

type batchItem[IDType comparable] interface {
	sortable
	cacheable
	AddToDBBatch(pgxconn.PgxBatch)
	ScanIDs(pgx.BatchResults) (IDType, error)
	comparable
}

type batchItems[K batchItem[V], V comparable] []K

func (q batchItems[K, V]) Len() int {
	return len(q)
}

func (q batchItems[K, V]) Less(i, j int) bool {
	return q[i].Before(q[j])
}

func (q batchItems[K, V]) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

// batcher queues up items to send to the DB, sorting them before sending to
// avoid deadlocks in the DB. It also avoids sending the same items repeatedly.
// batcher is not thread safe
type batcher[K batchItem[V], V comparable] struct {
	batch map[K]V
	cache *clockcache.Cache[K, V]
}

func newBatcher[K batchItem[V], V comparable](cache *clockcache.Cache[K, V]) batcher[K, V] {
	return batcher[K, V]{
		batch: make(map[K]V),
		cache: cache,
	}
}

// Queue adds item to the batch to be sent or replaces an existing item
func (b batcher[K, V]) Queue(i K) {
	var zero V
	b.batch[i] = zero
}

// Number of items in the batch.
func (b batcher[K, V]) Len() int {
	return len(b.batch)
}

// SendBatch sends the batch over the DB connections and gets the ID results.
// Caching is also checked for existance of IDs before sending to the DB to
// avoid unnecessary IO.
func (b batcher[K, V]) SendBatch(ctx context.Context, conn pgxconn.PgxConn) (err error) {
	batch := make([]K, len(b.batch))

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

	sort.Sort(batchItems[K, V](batch))

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

// Get returns the ID for the batch item in whatever the type was used.
func (b batcher[K, V]) Get(i K) (V, error) {
	entry, ok := b.batch[i]
	if !ok {
		return entry, fmt.Errorf("error getting item from batch")
	}

	return entry, nil
}
