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

// A Queryable handles queries against a storage.
// Use it when you need to have access to all samples without chunk encoding abstraction e.g promQL.
type Queryable interface {
	// SamplesQuerier returns a new promql.Querier on the storage. It helps querying over samples
	// in the database.
	SamplesQuerier(ctx context.Context, mint, maxt int64) (SamplesQuerier, error)
	// ExemplarsQuerier returns a new Querier that helps querying exemplars in the database.
	ExemplarsQuerier(ctx context.Context) ExemplarQuerier
}

// SamplesQuerier provides querying access over time series data of a fixed time range.
type SamplesQuerier interface {
	// LabelValues returns all potential values for a label name.
	// It is not safe to use the strings beyond the lifefime of the querier.
	LabelValues(name string) ([]string, storage.Warnings, error)

	// LabelNames returns all the unique label names present in the block in sorted order.
	LabelNames(...*labels.Matcher) ([]string, storage.Warnings, error)

	// Close releases the resources of the Querier.
	Close()

	// Select returns a set of series that matches the given label matchers.
	// Caller can specify if it requires returned series to be sorted. Prefer not requiring sorting for better performance.
	// It allows passing hints that can help in optimising select, but it's up to implementation how this is used if used at all.
	Select(sortSeries bool, hints *storage.SelectHints, qh *QueryHints, nodes []parser.Node, matchers ...*labels.Matcher) (storage.SeriesSet, parser.Node)
}

// Querier queries the data using the provided query data and returns the
// matching timeseries.
type Querier interface {
	// Query returns resulting timeseries for a query.
	Query(*prompb.Query) ([]*prompb.TimeSeries, error)
	// SamplesQuerier returns a sample querier.
	SamplesQuerier() SamplesQuerier2
	// ExemplarsQuerier returns an exemplar querier.
	ExemplarsQuerier(ctx context.Context) ExemplarQuerier
}

// SamplesQuerier2 queries data using the provided query data and returns the
// matching samples.
type SamplesQuerier2 interface {
	// Select returns a series set containing the exemplar that matches the supplied query parameters.
	Select(mint, maxt int64, sortSeries bool, hints *storage.SelectHints, queryHints *QueryHints, path []parser.Node, ms ...*labels.Matcher) (SeriesSet, parser.Node)
}

// ExemplarQuerier queries data using the provided query data and returns the
// matching exemplars.
type ExemplarQuerier interface {
	// Select returns a series set containing the exemplar that matches the supplied query parameters.
	Select(start, end time.Time, ms ...[]*labels.Matcher) ([]model.ExemplarQueryResult, error)
}
