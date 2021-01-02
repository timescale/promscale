// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"fmt"

	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/ha"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
)

type Cfg struct {
	AsyncAcks       bool
	ReportInterval  int
	SeriesCacheSize uint64
	NumCopiers      int
	HA              bool
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

	var parser Parser
	if cfg.HA {
		haService, err := ha.NewHAService(conn)
		if err != nil {
			return nil, err
		}
		parser = ha.NewHAParser(haService)
	} else {
		parser = &DataParser{}
	}

	return &DBIngestor{db: pi, Parser: parser}, nil
}

// NewPgxIngestor returns a new Ingestor that write to PostgreSQL using PGX
func NewPgxIngestor(conn pgxconn.PgxConn) (*DBIngestor, error) {
	c := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
	return NewPgxIngestorWithMetricCache(conn, c, &Cfg{})
}

// Ingest transforms and ingests the timeseries data into Timescale database.
// input:
//     tts the []Timeseries to insert
//     req the WriteRequest backing tts. It will be added to our WriteRequest
//         pool when it is no longer needed.
// NOTE: req will be added to our WriteRequest pool in this function, it must
//       not be used afterwards.
func (i *DBIngestor) Ingest(tts []prompb.TimeSeries, req *prompb.WriteRequest) (uint64, error) {
	var data map[string][]model.SamplesInfo
	var totalRows int
	var err error

	data, totalRows, err = i.ParseData(tts)
	// WriteRequests can contain pointers into the original buffer we deserialized
	// them out of, and can be quite large in and of themselves. In order to prevent
	// memory blowup, and to allow faster deserializing, we recycle the WriteRequest
	// here, allowing it to be either garbage collected or reused for a new request.
	// In order for this to work correctly, any data we wish to keep using (e.g.
	// samples) must no longer be reachable from req.
	FinishWriteRequest(req)

	// Note data == nil case is to handle samples from non-leader
	// prometheus instance
	if err != nil || data == nil {
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

// Close closes the ingestor
func (i *DBIngestor) Close() {
	i.db.Close()
}
