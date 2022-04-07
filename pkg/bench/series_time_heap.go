package bench

import (
	"container/heap"
	"fmt"
	"strconv"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/record"
)

type SeriesItem struct {
	ts        int64 // The value of the item; arbitrary.
	val       float64
	series_id uint64 // a unique series identifier
	it        chunkenc.Iterator
}

type SeriesTimeHeap []*SeriesItem

func NewSeriesTimeHeap(conf *BenchConfig, ss storage.SeriesSet, qmi *qmInfo, seriesIndex int) (SeriesTimeHeap, error) {
	sth := make(SeriesTimeHeap, 0, 100)
	fmt.Println("Starting to load series")
	seriesId := uint64(0)
	for ss.Next() {
		series := ss.At()
		it := series.Iterator()
		it.Next()
		ts, val := it.At()
		si := &SeriesItem{ts, val, seriesId, it}
		sth = append(sth, si)

		if conf.SeriesMultiplier == 1 && conf.MetricMultiplier == 1 {
			rs := record.RefSeries{
				Ref:    seriesId,
				Labels: series.Labels(),
			}
			seriesId++
			qmi.qm.StoreSeries([]record.RefSeries{rs}, seriesIndex)
		} else {
			lbls := series.Labels()
			build := labels.NewBuilder(lbls)
			for seriesMultiplierIndex := 0; seriesMultiplierIndex < conf.SeriesMultiplier; seriesMultiplierIndex++ {
				for metricMultiplierIndex := 0; metricMultiplierIndex < conf.MetricMultiplier; metricMultiplierIndex++ {
					if seriesMultiplierIndex != 0 {
						build.Set("multiplier", strconv.Itoa(seriesMultiplierIndex))
					}
					if metricMultiplierIndex != 0 {
						build.Set("__name__", lbls.Get("__name__")+"_"+strconv.Itoa(metricMultiplierIndex))
					}
					rs := record.RefSeries{
						Ref:    seriesId,
						Labels: build.Labels(),
					}
					seriesId++
					qmi.qm.StoreSeries([]record.RefSeries{rs}, seriesIndex)
				}
			}
		}
	}
	fmt.Println("Done loading series, # series", seriesId)

	if err := checkSeriesSet(ss); err != nil {
		return nil, err
	}

	heap.Init(&sth)
	return sth, nil
}

func (pq SeriesTimeHeap) Len() int { return len(pq) }

func (pq SeriesTimeHeap) Less(i, j int) bool {
	return pq[i].ts < pq[j].ts
}

func (pq SeriesTimeHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *SeriesTimeHeap) Push(x interface{}) {
	item := x.(*SeriesItem)
	*pq = append(*pq, item)
}

func (pq *SeriesTimeHeap) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[0 : n-1]
	return item
}

func (pq *SeriesTimeHeap) Visit(visitor func(ts int64, val float64, seriesID uint64) error) error {
	for pq.Len() > 0 {
		item := (*pq)[0]
		//fmt.Printf("%s %g %d\n", item.series.Labels(), item.val, item.ts)
		err := visitor(item.ts, item.val, item.series_id)
		if err != nil {
			return err
		}
		if item.it.Next() {
			item.ts, item.val = item.it.At()
			heap.Fix(pq, 0)
		} else {
			item2 := heap.Pop(pq)
			if item != item2 {
				panic("items not equal")
			}
		}
	}
	return nil
}
