package querier

import (
	"fmt"

	"github.com/timescale/promscale/pkg/prompb"
)

type queryRemoteRead struct {
	*pgxQuerier
}

func newQueryRemoteRead(qr *pgxQuerier) *queryRemoteRead {
	return &queryRemoteRead{qr}
}

// Query implements the RemoteReadQuerier interface. It is the entrypoint for
// remote read queries.
func (q *queryRemoteRead) Query(query *prompb.Query) ([]*prompb.TimeSeries, error) {
	if query == nil {
		return []*prompb.TimeSeries{}, nil
	}

	matchers, err := fromLabelMatchers(query.Matchers)
	if err != nil {
		return nil, err
	}

	qrySamples := newQuerySamples(q.pgxQuerier)
	sampleRows, _, err := qrySamples.fetchSamplesRows(query.StartTimestampMs, query.EndTimestampMs, nil, nil, nil, matchers)
	if err != nil {
		return nil, err
	}
	results, err := buildTimeSeries(sampleRows, q.tools.labelsReader)
	if err != nil {
		return nil, fmt.Errorf("building time-series: %w", err)
	}
	return results, nil
}
