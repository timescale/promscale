package main

import (
	"container/heap"
	"context"
	"fmt"
	"math"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func main() {
	err := dumpSamples("/Users/arye/promdata", math.MinInt64, math.MaxInt64)
	if err != nil {
		fmt.Println(err)
	}
}

type SeriesItem struct {
	ts     int64 // The value of the item; arbitrary.
	val    float64
	series storage.Series // The priority of the item in the queue.
	it     chunkenc.Iterator
	index  int //TODO: can remove?
}

type PriorityQueue []*SeriesItem

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].ts < pq[j].ts
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*SeriesItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
//func (pq *PriorityQueue) update(item *SeriesItem, ts int64) {
//item.ts = ts
//heap.Fix(pq, item.index)
//}

func dumpSamples(path string, mint, maxt int64) (err error) {
	db, err := tsdb.OpenDBReadOnly(path, nil)
	if err != nil {
		return err
	}
	defer func() {
		err2 := db.Close()
		if err == nil {
			err = err2
		}
	}()
	q, err := db.Querier(context.TODO(), mint, maxt)
	if err != nil {
		return err
	}
	defer q.Close()

	ss := q.Select(false, nil, labels.MustNewMatcher(labels.MatchRegexp, "", ".*"))
	pq := make(PriorityQueue, 0, 100)

	for ss.Next() {
		series := ss.At()
		it := series.Iterator()
		it.Next()
		ts, val := it.At()
		si := &SeriesItem{ts, val, series, it, len(pq)}
		pq = append(pq, si)
	}

	if ws := ss.Warnings(); len(ws) > 0 {
		for _, w := range ws {
			if err == nil {
				err = w
			}
		}
		return err
	}

	if ss.Err() != nil {
		return ss.Err()
	}

	heap.Init(&pq)
	for pq.Len() > 0 {
		item := pq[0]
		fmt.Printf("%s %g %d\n", item.series.Labels(), item.val, item.ts)
		if item.it.Next() {
			item.ts, item.val = item.it.At()
			heap.Fix(&pq, 0)
		} else {
			item2 := heap.Pop(&pq)
			if item != item2 {
				panic("items not equal")
			}
		}

	}

	/*for ss.Next() {
		series := ss.At()
		lbs := series.Labels()
		it := series.Iterator()
		for it.Next() {
			ts, val := it.At()
			fmt.Printf("%s %g %d\n", lbs, val, ts)
		}
		if it.Err() != nil {
			return ss.Err()
		}
	}*/

	if ws := ss.Warnings(); len(ws) > 0 {
		for _, w := range ws {
			if err == nil {
				err = w
			}
		}
		return err
	}

	if ss.Err() != nil {
		return ss.Err()
	}
	return nil
}
