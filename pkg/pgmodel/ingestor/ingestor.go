// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"fmt"
	"time"

	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
)

type Cfg struct {
	AsyncAcks              bool
	ReportInterval         int
	NumCopiers             int
	DisableEpochSync       bool
	IgnoreCompressedChunks bool
}

// DBIngestor ingest the TimeSeries data into Timescale database.
type DBIngestor struct {
	sCache             cache.SeriesCache
	samplesDispatcher  model.Dispatcher
	metadataDispatcher model.Dispatcher
}

// NewPgxIngestor returns a new Ingestor that uses connection pool and a metrics cache
// for caching metric table names.
func NewPgxIngestor(conn pgxconn.PgxConn, cache cache.MetricCache, sCache cache.SeriesCache, cfg *Cfg) (*DBIngestor, error) {
	toSamplesCopiers, toMetadataCopiers := launchCopiers(conn, cfg.NumCopiers)
	samplesDispatcher, err := newSamplesDispatcher(conn, cfg, cache, sCache, toSamplesCopiers)
	if err != nil {
		return nil, err
	}
	metadataDispatcher := newMetadataDispatcher(toMetadataCopiers)
	return &DBIngestor{
		sCache:             sCache,
		samplesDispatcher:  samplesDispatcher,
		metadataDispatcher: metadataDispatcher,
	}, nil
}

// NewPgxIngestorForTests returns a new Ingestor that write to PostgreSQL using PGX
// with an empty config, a new default size metrics cache and a non-ha-aware data parser
func NewPgxIngestorForTests(conn pgxconn.PgxConn, cfg *Cfg) (*DBIngestor, error) {
	if cfg == nil {
		cfg = &Cfg{}
	}
	c := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
	s := cache.NewSeriesCache(cache.DefaultConfig, nil)
	return NewPgxIngestor(conn, c, s, cfg)
}

// Ingest transforms and ingests the timeseries data into Timescale database.
// input:
//     tts the []Timeseries to insert
//     req the WriteRequest backing tts. It will be added to our WriteRequest
//         pool when it is no longer needed.
func (ingestor *DBIngestor) Ingest(r *prompb.WriteRequest) (uint64, int, error) {
	numTs := len(r.Timeseries)
	numMeta := len(r.Metadata)
	// WriteRequests can contain pointers into the original buffer we deserialized
	// them out of, and can be quite large in and of themselves. In order to prevent
	// memory blowup, and to allow faster deserializing, we recycle the WriteRequest
	// here, allowing it to be either garbage collected or reused for a new request.
	// In order for this to work correctly, any data we wish to keep using (e.g.
	// samples) must no longer be reachable from req.
	defer FinishWriteRequest(r)
	if numTs > 0 && numMeta == 0 {
		// Write request contains only time-series.
		n, err := ingestor.ingestSamples(r)
		return n, 0, err
	}
	if numMeta > 0 && numTs == 0 {
		// Write request contains only metadata.
		n, err := ingestor.ingestMetadata(r)
		return 0, n, err
	}
	fmt.Println("multi case")
	// Write request contains both samples and metadata, hence we ingest concurrently.
	type result struct {
		id      int8
		numRows uint64
		err     error
	}
	res := make(chan result, 2)
	defer close(res)
	go func() {
		n, err := ingestor.ingestSamples(r)
		res <- result{1, n, err}
	}()
	go func() {
		n, err := ingestor.ingestMetadata(r)
		res <- result{2, uint64(n), err}
	}()
	var (
		err                  error
		samplesRowsInserted  uint64
		metadataRowsInserted int
	)
	mergeErr := func(prevErr, err error, message string) error {
		if prevErr != nil {
			err = fmt.Errorf("%s: %s: %w", prevErr.Error(), message, err)
		}
		return err
	}
	for i := 0; i < 2; i++ {
		response := <-res
		switch response.id {
		case 1:
			samplesRowsInserted = response.numRows
			err = mergeErr(err, response.err, "ingesting samples")
		case 2:
			metadataRowsInserted = int(response.numRows)
			err = mergeErr(err, response.err, "ingesting metadata")
		}
	}
	return samplesRowsInserted, metadataRowsInserted, err
}

func (ingestor *DBIngestor) ingestSamples(r *prompb.WriteRequest) (uint64, error) {
	var (
		totalSamplesRows uint64
		dataSamples      = make(map[string][]model.Samples)
	)
	for i := range r.Timeseries {
		ts := &r.Timeseries[i]
		if len(ts.Samples) == 0 {
			continue
		}
		// Normalize and canonicalize t.Labels.
		// After this point t.Labels should never be used again.
		seriesLabels, metricName, err := ingestor.sCache.GetSeriesFromProtos(ts.Labels)
		if err != nil {
			return 0, err
		}
		if metricName == "" {
			return 0, errors.ErrNoMetricName
		}
		sample := model.NewPromSample(seriesLabels, ts.Samples)
		totalSamplesRows += uint64(len(ts.Samples))

		dataSamples[metricName] = append(dataSamples[metricName], sample)
		// we're going to free req after this, but we still need the samples,
		// so nil the field
		ts.Samples = nil
	}

	rowsInserted, err := ingestor.samplesDispatcher.InsertData(model.Data{Rows: dataSamples, ReceivedTime: time.Now()})
	if err == nil && rowsInserted != totalSamplesRows {
		return rowsInserted, fmt.Errorf("failed to insert all the data! Expected: %d, Got: %d", totalRows, rowsInserted)
	}
	return metadataRows, nil
}

// Parts of metric creation not needed to insert data
func (ingestor *DBIngestor) CompleteMetricCreation() error {
	return ingestor.samplesDispatcher.CompleteMetricCreation()
}

// Close closes the ingestor
func (ingestor *DBIngestor) Close() {
	ingestor.samplesDispatcher.Close()
}
