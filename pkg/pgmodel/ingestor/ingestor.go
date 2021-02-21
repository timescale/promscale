// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"fmt"

	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
)

type Cfg struct {
	AsyncAcks        bool
	ReportInterval   int
	SeriesCacheSize  uint64
	NumCopiers       int
	DisableEpochSync bool
}

// DBIngestor ingest the TimeSeries data into Timescale database.
type DBIngestor struct {
	db     model.Inserter
	scache cache.SeriesCache
}

// NewPgxIngestorWithMetricCache returns a new Ingestor that uses connection pool and a metrics cache
// for caching metric table names.
func NewPgxIngestorWithMetricCache(conn pgxconn.PgxConn, cache cache.MetricCache, scache cache.SeriesCache, cfg *Cfg) (*DBIngestor, error) {
	pi, err := newPgxInserter(conn, cache, scache, cfg)
	if err != nil {
		return nil, err
	}

	return &DBIngestor{db: pi, scache: scache}, nil
}

// NewPgxIngestor returns a new Ingestor that write to PostgreSQL using PGX
func NewPgxIngestor(conn pgxconn.PgxConn) (*DBIngestor, error) {
	c := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
	s := cache.NewSeriesCache(cache.DefaultSeriesCacheSize)
	return NewPgxIngestorWithMetricCache(conn, c, s, &Cfg{})
}

// Ingest transforms and ingests the timeseries data into Timescale database.
// input:
//     tts the []Timeseries to insert
//     req the WriteRequest backing tts. It will be added to our WriteRequest
//         pool when it is no longer needed.
func (i *DBIngestor) Ingest(tts []prompb.TimeSeries, req *prompb.WriteRequest) (uint64, error) {
	data, totalRows, err := i.parseData(tts, req)

	if err != nil {
		return 0, err
	}

	rowsInserted, err := i.db.InsertNewData(data)
	if err == nil && int(rowsInserted) != totalRows {
		return rowsInserted, fmt.Errorf("failed to insert all the data! Expected: %d, Got: %d", totalRows, rowsInserted)
	}
	return rowsInserted, err
}

// Parts of metric creation not needed to insert data
func (i *DBIngestor) CompleteMetricCreation() error {
	return i.db.CompleteMetricCreation()
}

// Parse data into a set of samplesInfo infos per-metric.
// returns: map[metric name][]SamplesInfo, total rows to insert
// NOTE: req will be added to our WriteRequest pool in this function, it must
//       not be used afterwards.
func (ingestor *DBIngestor) parseData(tts []prompb.TimeSeries, req *prompb.WriteRequest) (map[string][]model.Samples, int, error) {
	dataSamples := make(map[string][]model.Samples)
	rows := 0

	for i := range tts {
		t := &tts[i]
		if len(t.Samples) == 0 {
			continue
		}

		// Normalize and canonicalize t.Labels.
		// After this point t.Labels should never be used again.
		seriesLabels, metricName, err := ingestor.scache.GetSeriesFromProtos(t.Labels)
		if err != nil {
			return nil, rows, err
		}
		if metricName == "" {
			return nil, rows, errors.ErrNoMetricName
		}
		sample := model.NewPromSample(seriesLabels, t.Samples)
		rows += len(t.Samples)

		dataSamples[metricName] = append(dataSamples[metricName], sample)
		// we're going to free req after this, but we still need the samples,
		// so nil the field
		t.Samples = nil
	}

	// WriteRequests can contain pointers into the original buffer we deserialized
	// them out of, and can be quite large in and of themselves. In order to prevent
	// memory blowup, and to allow faster deserializing, we recycle the WriteRequest
	// here, allowing it to be either garbage collected or reused for a new request.
	// In order for this to work correctly, any data we wish to keep using (e.g.
	// samples) must no longer be reachable from req.
	FinishWriteRequest(req)

	return dataSamples, rows, nil
}

// Close closes the ingestor
func (i *DBIngestor) Close() {
	i.db.Close()
}
