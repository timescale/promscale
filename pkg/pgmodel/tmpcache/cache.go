package tmpcache

import (
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

func NewUnresolvedSeriesCache() *UnresolvedSeriesCacheImpl {
	cache := &UnresolvedSeriesCacheImpl{
		make(map[*model.SeriesCacheKey]*model.UnresolvedSeries),
	}
	return cache
}

type UnresolvedSeriesCacheImpl struct {
	cache map[*model.SeriesCacheKey]*model.UnresolvedSeries
}

func (t *UnresolvedSeriesCacheImpl) GetSeries(key *model.SeriesCacheKey, labels []prompb.Label) (series *model.UnresolvedSeries, err error) {
	elem, present := t.cache[key]
	if !present {
		unresolvedSeries := model.NewUnresolvedSeries(labels)
		t.cache[key] = unresolvedSeries
		return unresolvedSeries, nil
	}
	return elem, nil
}
