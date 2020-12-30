// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"fmt"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/ha"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
)

type Cfg struct {
	AsyncAcks       bool
	ReportInterval  int
	SeriesCacheSize uint64
	NumCopiers      int
	HA        bool
}

// DBIngestor ingest the TimeSeries data into Timescale database.
type DBIngestor struct {
	db model.Inserter
	Parser
}

// NewPgxIngestorWithMetricCache returns a new Ingestor that uses connection pool and a metrics cache
// for caching metric table names.
func NewPgxIngestorWithMetricCache(conn pgxconn.PgxConn, cache cache.MetricCache, cfg *Cfg) (*DBIngestor, error) {
	pi, err := newPgxInserter(conn, cache, cfg)
	if err != nil {
		return nil, err
	}

	if cfg.HA {
		return &DBIngestor{db: pi, Parser: ha.NewHAState(&conn)}, nil
	}

	return &DBIngestor{db: pi, Parser: &DataParser{}}, nil
}

// NewPgxIngestor returns a new Ingestor that write to PostgreSQL using PGX
func NewPgxIngestor(conn pgxconn.PgxConn) (*DBIngestor, error) {
	c := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
	return NewPgxIngestorWithMetricCache(conn, c, &Cfg{})
}

type Parser interface{
	ParseData([]prompb.TimeSeries, *prompb.WriteRequest) (map[string][]model.SamplesInfo, int, error)
}

// Ingest transforms and ingests the timeseries data into Timescale database.
// input:
//     tts the []Timeseries to insert
//     req the WriteRequest backing tts. It will be added to our WriteRequest
//         pool when it is no longer needed.
func (i *DBIngestor) Ingest(tts []prompb.TimeSeries, req *prompb.WriteRequest) (uint64, error) {
	var data map[string][]model.SamplesInfo
	var totalRows int
	var err error


	data, totalRows, err = i.ParseData(tts, req)
	// Note data == nil case is to handle samples from non-leader
	// prometheus instance
	if err != nil || data == nil {
		return 0, err
	}

	// WriteRequests can contain pointers into the original buffer we deserialized
	// them out of, and can be quite large in and of themselves. In order to prevent
	// memory blowup, and to allow faster deserializing, we recycle the WriteRequest
	// here, allowing it to be either garbage collected or reused for a new request.
	// In order for this to work correctly, any data we wish to keep using (e.g.
	// samples) must no longer be reachable from req.
	FinishWriteRequest(req)

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

type DataParser struct{}

// Parse data into a set of samplesInfo infos per-metric.
// returns: map[metric name][]SamplesInfo, total rows to insert
// NOTE: req will be added to our WriteRequest pool in this function, it must
//       not be used afterwards.
func (i *DataParser) ParseData(tts []prompb.TimeSeries, req *prompb.WriteRequest) (map[string][]model.SamplesInfo, int, error) {
	dataSamples := make(map[string][]model.SamplesInfo)
	rows := 0

	for i := range tts {
		t := &tts[i]
		if len(t.Samples) == 0 {
			continue
		}

		// Normalize and canonicalize t.Labels.
		// After this point t.Labels should never be used again.
		seriesLabels, metricName, err := model.LabelProtosToLabels(t.Labels)
		if err != nil {
			return nil, rows, err
		}
		if metricName == "" {
			return nil, rows, errors.ErrNoMetricName
		}
		sample := model.SamplesInfo{
			Labels:   seriesLabels,
			SeriesID: -1, // sentinel marking the seriesId as unset
			Samples:  t.Samples,
		}
		rows += len(t.Samples)

		dataSamples[metricName] = append(dataSamples[metricName], sample)
		// we're going to free req after this, but we still need the samples,
		// so nil the field
		t.Samples = nil
	}

	return dataSamples, rows, nil
}



// Close closes the ingestor
func (i *DBIngestor) Close() {
	i.db.Close()
}
