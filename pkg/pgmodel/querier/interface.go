package querier

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

// Reader reads the data based on the provided read request.
type Reader interface {
	Read(*prompb.ReadRequest) (*prompb.ReadResponse, error)
}

// SeriesSet adds a Close method to storage.SeriesSet to provide a way to free memory/
type SeriesSet interface {
	storage.SeriesSet
	Close()
}

// Querier provides access to the three query methods: remote read, samples,
// and exemplars.
type Querier interface {
	// RemoteReadQuerier returns a remote storage querier
	RemoteReadQuerier() RemoteReadQuerier
	// SamplesQuerier returns a sample querier.
	SamplesQuerier() SamplesQuerier
	// ExemplarsQuerier returns an exemplar querier.
	ExemplarsQuerier(ctx context.Context) ExemplarQuerier
}

// RemoteReadQuerier queries the data using the provided query data and returns
// the matching timeseries.
type RemoteReadQuerier interface {
	// Query returns resulting timeseries for a query.
	Query(*prompb.Query) ([]*prompb.TimeSeries, error)
}

// SamplesQuerier queries data using the provided query data and returns the
// matching samples.
type SamplesQuerier interface {
	// Select returns a series set containing the exemplar that matches the supplied query parameters.
	Select(mint, maxt int64, sortSeries bool, hints *storage.SelectHints, queryHints *QueryHints, path []parser.Node, ms ...*labels.Matcher) (SeriesSet, parser.Node, *RollupConfig)
}

// ExemplarQuerier queries data using the provided query data and returns the
// matching exemplars.
type ExemplarQuerier interface {
	// Select returns a series set containing the exemplar that matches the supplied query parameters.
	Select(start, end time.Time, ms ...[]*labels.Matcher) ([]model.ExemplarQueryResult, error)
}
