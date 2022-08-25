package tmpcache

import (
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
	"sync"
)

type UnresolvedSeriesCacheImpl struct {
	cache map[*model.SeriesCacheKey]*model.UnresolvedSeries
	lock  sync.RWMutex
}

func NewUnresolvedSeriesCache() *UnresolvedSeriesCacheImpl {
	cache := &UnresolvedSeriesCacheImpl{
		cache: make(map[*model.SeriesCacheKey]*model.UnresolvedSeries),
	}
	return cache
}

func (t *UnresolvedSeriesCacheImpl) GetSeries(key *model.SeriesCacheKey, labels []prompb.Label) (series *model.UnresolvedSeries, err error) {
	elem, present := t.get(key)
	if !present {
		unresolvedSeries := model.NewUnresolvedSeries(labels)
		t.set(key, unresolvedSeries)
		return unresolvedSeries, nil
	}
	return elem, nil
}

func (t *UnresolvedSeriesCacheImpl) get(key *model.SeriesCacheKey) (elem *model.UnresolvedSeries, present bool) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	elem, present = t.cache[key]
	return
}

func (t *UnresolvedSeriesCacheImpl) set(key *model.SeriesCacheKey, series *model.UnresolvedSeries) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.cache[key] = series
}
