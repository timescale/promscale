// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package cache

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

// this seems like a good default size for /active/ series. This results in Promscale using around 360MB on start.
const DefaultSeriesCacheSize = 1000000

// SeriesCache is a cache of model.Series entries.
type SeriesCache interface {
	Reset()
	GetSeriesFromProtos(labelPairs []prompb.Label) (series *model.Series, err error)
	Len() int
	Cap() int
	Evictions() uint64
}

type SeriesCacheImpl struct {
	*ResizableCache
	labelsDict  sync.Map
	dictCounter atomic.Uint32
	dictLock    sync.Mutex
}

func NewSeriesCache(config Config, sigClose <-chan struct{}) *SeriesCacheImpl {
	return &SeriesCacheImpl{NewResizableCache(
		clockcache.WithMetrics("series", "metric", config.SeriesCacheInitialSize),
		config.SeriesCacheMemoryMaxBytes,
		sigClose), sync.Map{}, atomic.Uint32{}, sync.Mutex{}}
}

// Get the canonical version of a series if one exists.
// input: the string representation of a Labels as defined by generateKey()
func (t *SeriesCacheImpl) loadSeries(str string) (l *model.Series) {
	val, ok := t.Get(str)
	if !ok {
		return nil
	}
	return val.(*model.Series)
}

// Try to set a series as the canonical Series for a given string
// representation, returning the canonical version (which can be different in
// the even of multiple goroutines setting labels concurrently).
func (t *SeriesCacheImpl) setSeries(str string, lset *model.Series) *model.Series {
	//str not counted twice in size since the key and lset.str will point to same thing.
	val, inCache := t.Insert(str, lset, lset.FinalSizeBytes())
	if !inCache {
		// It seems that cache was full and eviction failed to remove
		// element due to starvation caused by a lot of concurrent gets
		// This is a signal to grow our cache
		log.Info("growing cache because of eviction starvation")
		t.grow()
	}
	return val.(*model.Series)
}

var keyPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// GetSeriesFromLabels converts a labels.Labels to a canonical model.Series object
func (t *SeriesCacheImpl) GetSeriesFromLabels(ls labels.Labels) (*model.Series, error) {
	ll := make([]prompb.Label, len(ls))
	for i := range ls {
		ll[i].Name = ls[i].Name
		ll[i].Value = ls[i].Value
	}
	l, err := t.GetSeriesFromProtos(ll)
	return l, err
}

// GetSeriesFromProtos returns a model.Series entry given a list of Prometheus
// prompb.Label.
// If the desired entry is not in the cache, a "placeholder" model.Series entry
// is constructed and put into the cache. It is not populated with database IDs
// until a later phase, see model.Series.SetSeriesID.
func (t *SeriesCacheImpl) GetSeriesFromProtos(labelPairs []prompb.Label) (*model.Series, error) {
	seriesKey := t.getSeriesKey(labelPairs)
	series := t.loadSeries(seriesKey)
	if series == nil {
		series = model.NewSeries(labelPairs)
		series = t.setSeries(seriesKey, series)
	}
	return series, nil
}

// We use dictionary encoding to generate series key.
// Each labelPair is encoded to uint32
// We perform variable encoding on uint32 to byte and append all to byte buffer
// Bytes are then serialized to string using Base64 encoding
func (s *SeriesCacheImpl) getSeriesKey(labelPairs []prompb.Label) string {
	seriesKeyBytes := keyPool.Get().(*bytes.Buffer)
	idEncodeBuf := make([]byte, binary.MaxVarintLen32)
	ensureLabelsSorted(labelPairs)
	for _, labelPair := range labelPairs {
		l := labels.Label{Name: labelPair.Name, Value: labelPair.Value}
		pairEnc, found := s.labelsDict.Load(l)
		if !found {
			s.dictLock.Lock()
			pairEnc, _ = s.labelsDict.LoadOrStore(l, s.dictCounter.Add(1))
			s.dictLock.Unlock()
		}
		encodedSize := binary.PutUvarint(idEncodeBuf, uint64(pairEnc.(uint32)))
		tmp := idEncodeBuf[:encodedSize]
		//since length can never exceed 4 bytes this is safe
		seriesKeyBytes.WriteByte(byte(len(tmp)))
		seriesKeyBytes.Write(tmp)
	}
	return base64.StdEncoding.EncodeToString(seriesKeyBytes.Bytes())
}

// We really shouldn't find unsorted labels. This code is to be extra safe.
func ensureLabelsSorted(in []prompb.Label) {
	comparator := func(i, j int) bool {
		return in[i].Name < in[j].Name
	}
	for i := len(in) - 1; i > 0; i-- {
		if in[i].Name < in[i-1].Name {
			sort.Slice(in, comparator)
			break
		}
	}
}
