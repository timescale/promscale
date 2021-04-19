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
	sCache     cache.SeriesCache
	dispatcher model.Dispatcher
}

// NewPgxIngestor returns a new Ingestor that uses connection pool and a metrics cache
// for caching metric table names.
func NewPgxIngestor(conn pgxconn.PgxConn, cache cache.MetricCache, sCache cache.SeriesCache, cfg *Cfg) (*DBIngestor, error) {
	dispatcher, err := newPgxDispatcher(conn, cache, sCache, cfg)
	if err != nil {
		return nil, err
	}
	return &DBIngestor{
		sCache:     sCache,
		dispatcher: dispatcher,
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

const (
	meta = iota
	series
)

// result contains insert stats and is used when we ingest samples and metadata concurrently.
type result struct {
	id      int8
	numRows uint64
	err     error
}

// Ingest transforms and ingests the timeseries data into Timescale database.
// input:
//     tts the []Timeseries to insert
//     req the WriteRequest backing tts. It will be added to our WriteRequest
//         pool when it is no longer needed.
func (ingestor *DBIngestor) Ingest(r *prompb.WriteRequest) (numSamples uint64, numMetadata uint64, err error) {
	activeWriteRequests.Inc()
	defer activeWriteRequests.Dec() // Dec() is defered otherwise it will lead to loosing a decrement if some error occurs.
	var (
		timeseries = r.Timeseries
		metadata   = r.Metadata
	)
	release := func() { FinishWriteRequest(r) }
	switch numTs, numMeta := len(timeseries), len(metadata); {
	case numTs > 0 && numMeta == 0:
		// Write request contains only time-series.
		n, err := ingestor.ingestTimeseries(timeseries, release)
		return n, 0, err
	case numTs == 0 && numMeta == 0:
		release()
		return 0, 0, nil
	case numMeta > 0 && numTs == 0:
		// Write request contains only metadata.
		n, err := ingestor.ingestMetadata(metadata, release)
		return 0, n, err
	default:
	}
	release = func() {
		// We do not want to re-initialize the write-request when ingesting concurrently.
		// A concurrent ingestion may not have read the write-request and we initialized it
		// leading to data loss.
		FinishWriteRequest(nil)
	}
	res := make(chan result, 2)
	defer close(res)

	go func() {
		n, err := ingestor.ingestTimeseries(timeseries, release)
		res <- result{series, n, err}
	}()
	go func() {
		n, err := ingestor.ingestMetadata(metadata, release)
		res <- result{meta, n, err}
	}()

	var (
		samplesRowsInserted  uint64
		metadataRowsInserted uint64
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
		case series:
			samplesRowsInserted = response.numRows
			err = mergeErr(err, response.err, "ingesting timeseries")
		case meta:
			metadataRowsInserted = response.numRows
			err = mergeErr(err, response.err, "ingesting metadata")
		}
	}
	// WriteRequests can contain pointers into the original buffer we deserialized
	// them out of, and can be quite large in and of themselves. In order to prevent
	// memory blowup, and to allow faster deserializing, we recycle the WriteRequest
	// here, allowing it to be either garbage collected or reused for a new request.
	// In order for this to work correctly, any data we wish to keep using (e.g.
	// samples) must no longer be reachable from req.
	FinishWriteRequest(r)
	return samplesRowsInserted, metadataRowsInserted, err
}

func (ingestor *DBIngestor) ingestTimeseries(timeseries []prompb.TimeSeries, releaseMem func()) (uint64, error) {
	var (
		totalSamplesRows uint64
		dataSamples      = make(map[string][]model.Samples)
	)
	for i := range timeseries {
		ts := &timeseries[i]
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
	releaseMem()

	samplesRowsInserted, errSamples := ingestor.dispatcher.InsertTs(model.Data{Rows: dataSamples, ReceivedTime: time.Now()})
	if errSamples == nil && samplesRowsInserted != totalSamplesRows {
		return samplesRowsInserted, fmt.Errorf("failed to insert all the data! Expected: %d, Got: %d", totalSamplesRows, samplesRowsInserted)
	}
	return samplesRowsInserted, errSamples
}

// ingestMetadata ingests metric metadata received from Prometheus. It runs as a secondary routine, independent from
// the main dataflow (i.e., samples ingestion) since metadata ingestion is not as frequent as that of samples.
func (ingestor *DBIngestor) ingestMetadata(metadata []prompb.MetricMetadata, releaseMem func()) (uint64, error) {
	num := len(metadata)
	data := make([]model.Metadata, num)
	for i := 0; i < num; i++ {
		tmp := metadata[i]
		data[i] = model.Metadata{
			MetricFamily: tmp.MetricFamilyName,
			Unit:         tmp.Unit,
			Type:         tmp.Type.String(),
			Help:         tmp.Help,
		}
	}
	releaseMem()
	rowsInserted, errMetadata := ingestor.dispatcher.InsertMetadata(data)
	if errMetadata != nil {
		return 0, errMetadata
	}
	return rowsInserted, nil
}

// Parts of metric creation not needed to insert data
func (ingestor *DBIngestor) CompleteMetricCreation() error {
	return ingestor.dispatcher.CompleteMetricCreation()
}

// Close closes the ingestor
func (ingestor *DBIngestor) Close() {
	ingestor.dispatcher.Close()
}
