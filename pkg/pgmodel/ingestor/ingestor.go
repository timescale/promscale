// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"fmt"

	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
)

type Cfg struct {
	AsyncAcks        bool
	ReportInterval   int
	NumCopiers       int
	DisableEpochSync bool
}

// DBIngestor ingest the TimeSeries data into Timescale database.
type DBIngestor struct {
	db     model.Inserter
	scache cache.SeriesCache
	parser Parser
}

// NewPgxIngestor returns a new Ingestor that uses connection pool and a metrics cache
// for caching metric table names.
func NewPgxIngestor(conn pgxconn.PgxConn, cache cache.MetricCache, scache cache.SeriesCache, parser Parser, cfg *Cfg) (*DBIngestor, error) {
	pi, err := newPgxInserter(conn, cache, scache, cfg)
	if err != nil {
		return nil, err
	}

	return &DBIngestor{db: pi, scache: scache, parser: parser}, nil
}

// NewPgxIngestorForTests returns a new Ingestor that write to PostgreSQL using PGX
// with an empty config, a new default size metrics cache and a non-ha-aware data parser
func NewPgxIngestorForTests(conn pgxconn.PgxConn) (*DBIngestor, error) {
	c := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
	s := cache.NewSeriesCache(cache.DefaultConfig, nil)
	return NewPgxIngestor(conn, c, s, &dataParser{scache: s}, &Cfg{})
}

// Ingest transforms and ingests the timeseries data into Timescale database.
// input:
//     tts the []Timeseries to insert
//     req the WriteRequest backing tts. It will be added to our WriteRequest
//         pool when it is no longer needed.
func (ingestor *DBIngestor) Ingest(tts []prompb.TimeSeries, req *prompb.WriteRequest) (uint64, error) {
	data, totalRows, err := ingestor.parser.ParseData(tts)
	// WriteRequests can contain pointers into the original buffer we deserialized
	// them out of, and can be quite large in and of themselves. In order to prevent
	// memory blowup, and to allow faster deserializing, we recycle the WriteRequest
	// here, allowing it to be either garbage collected or reused for a new request.
	// In order for this to work correctly, any data we wish to keep using (e.g.
	// samples) must no longer be reachable from req.
	FinishWriteRequest(req)

	// Note data == nil case is to handle samples from non-leader
	// prometheus instance or when len(tts) == 0
	if err != nil || data == nil {
		return 0, err
	}

	rowsInserted, err := ingestor.db.InsertNewData(data)
	if err == nil && int(rowsInserted) != totalRows {
		return rowsInserted, fmt.Errorf("failed to insert all the data! Expected: %d, Got: %d", totalRows, rowsInserted)
	}
	return rowsInserted, err
}

// Parts of metric creation not needed to insert data
func (ingestor *DBIngestor) CompleteMetricCreation() error {
	return ingestor.db.CompleteMetricCreation()
}

// Close closes the ingestor
func (ingestor *DBIngestor) Close() {
	ingestor.db.Close()
}
