package bench

import (
	"container/heap"
	"fmt"
	"io"
	"strconv"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/record"
)

type SeriesItem struct {
	series_id uint64 // a unique series identifier
	it        chunkenc.Iterator
}

type SeriesTimeHeap []*SeriesItem

func NewSeriesTimeHeap(conf *BenchConfig, block *tsdb.Block, qmi *qmInfo, seriesIndex int) (SeriesTimeHeap, []io.Closer, error) {
	sth := make(SeriesTimeHeap, 0, 100)
	fmt.Println("Starting to load series")
	seriesId := uint64(0)
	build := labels.NewBuilder(nil)
	closers := []io.Closer{}

	ir, err := block.Index()
	if err != nil {
		return nil, closers, err
	}

	p, err := ir.Postings("", "") // The special all key.
	if err != nil {
		return nil, closers, err
	}
	closers = append(closers, ir)

	chunkr, err := block.Chunks()
	if err != nil {
		return nil, closers, err
	}
	closers = append(closers, chunkr)

	lbls := labels.Labels{}
	chks := []chunks.Meta{}
	for p.Next() {
		if err = ir.Series(p.At(), &lbls, &chks); err != nil {
			return nil, closers, err
		}

		it := NewBufferingIterator(chunkr, chks)
		if !it.Next() { //initialize to first position
			panic("can't get first item")
		}
		si := &SeriesItem{seriesId, it}
		sth = append(sth, si)

		if conf.SeriesMultiplier == 1 && conf.MetricMultiplier == 1 {
			rs := record.RefSeries{
				Ref:    seriesId,
				Labels: lbls,
			}
			seriesId++
			qmi.qm.StoreSeries([]record.RefSeries{rs}, seriesIndex)
		} else {
			for seriesMultiplierIndex := 0; seriesMultiplierIndex < conf.SeriesMultiplier; seriesMultiplierIndex++ {
				for metricMultiplierIndex := 0; metricMultiplierIndex < conf.MetricMultiplier; metricMultiplierIndex++ {
					build.Reset(lbls)
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

	/*if err := checkSeriesSet(ss); err != nil {
		return nil, err
	}*/

	heap.Init(&sth)
	return sth, closers, nil
}

func (pq SeriesTimeHeap) Len() int { return len(pq) }

func (pq SeriesTimeHeap) Less(i, j int) bool {
	tsi, _ := pq[i].it.At()
	tsj, _ := pq[j].it.At()
	return tsi < tsj
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

func (pq *SeriesTimeHeap) Visit(conf *BenchConfig, visitor func(ts int64, val float64, seriesID uint64) error) error {
	for pq.Len() > 0 {
		item := (*pq)[0]
		//fmt.Printf("%s %g %d\n", item.series.Labels(), item.val, item.ts)

		seriesID := uint64(item.series_id)
		ts, val := item.it.At()
		for seriesMultiplierIndex := 0; seriesMultiplierIndex < conf.SeriesMultiplier; seriesMultiplierIndex++ {
			for metricMultiplierIndex := 0; metricMultiplierIndex < conf.MetricMultiplier; metricMultiplierIndex++ {
				err := visitor(ts, val, seriesID)
				seriesID++
				if err != nil {
					return err
				}
			}
		}

		if item.it.Next() {
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
