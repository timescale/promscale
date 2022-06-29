// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/atomic"

	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/ingestor/trace"
	"github.com/timescale/promscale/pkg/pgmodel/metrics"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/tracer"
)

type Cfg struct {
	AsyncAcks               bool
	NumCopiers              int
	DisableEpochSync        bool
	IgnoreCompressedChunks  bool
	InvertedLabelsCacheSize uint64
}

// DBIngestor ingest the TimeSeries data into Timescale database.
type DBIngestor struct {
	sCache     cache.SeriesCache
	dispatcher model.Dispatcher
	tWriter    trace.Writer
	closed     *atomic.Bool
}

// NewPgxIngestor returns a new Ingestor that uses connection pool and a metrics cache
// for caching metric table names.
func NewPgxIngestor(conn pgxconn.PgxConn, cache cache.MetricCache, sCache cache.SeriesCache, eCache cache.PositionCache, cfg *Cfg) (*DBIngestor, error) {
	dispatcher, err := newPgxDispatcher(conn, cache, sCache, eCache, cfg)
	if err != nil {
		return nil, err
	}
	return &DBIngestor{
		sCache:     sCache,
		dispatcher: dispatcher,
		tWriter:    trace.NewWriter(conn),
		closed:     atomic.NewBool(false),
	}, nil
}

// NewPgxIngestorForTests returns a new Ingestor that write to PostgreSQL using PGX
// with an empty config, a new default size metrics cache and a non-ha-aware data parser
func NewPgxIngestorForTests(conn pgxconn.PgxConn, cfg *Cfg) (*DBIngestor, error) {
	if cfg == nil {
		cfg = &Cfg{InvertedLabelsCacheSize: cache.DefaultConfig.InvertedLabelsCacheSize}
	}
	cacheConfig := cache.DefaultConfig
	c := cache.NewMetricCache(cacheConfig)
	s := cache.NewSeriesCache(cacheConfig, nil)
	e := cache.NewExemplarLabelsPosCache(cacheConfig)
	return NewPgxIngestor(conn, c, s, e, cfg)
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

func (ingestor *DBIngestor) IngestTraces(ctx context.Context, traces pdata.Traces) error {
	if ingestor.closed.Load() {
		return fmt.Errorf("ingestor is closed and can't ingest traces")
	}
	return ingestor.tWriter.InsertTraces(ctx, traces)
}

// Ingest transforms and ingests the timeseries data into Timescale database.
// input:
//     tts the []Timeseries to insert
//     req the WriteRequest backing tts. It will be added to our WriteRequest
//         pool when it is no longer needed.
func (ingestor *DBIngestor) Ingest(ctx context.Context, r *prompb.WriteRequest) (numInsertablesIngested uint64, numMetadataIngested uint64, err error) {
	if ingestor.closed.Load() {
		return 0, 0, fmt.Errorf("ingestor is closed and can't ingest metrics")
	}
	ctx, span := tracer.Default().Start(ctx, "db-ingest")
	defer span.End()
	metrics.IngestorActiveWriteRequests.With(prometheus.Labels{"type": "metric", "kind": "sample_or_metadata"}).Inc()
	defer metrics.IngestorActiveWriteRequests.With(prometheus.Labels{"type": "metric", "kind": "sample_or_metadata"}).Dec()
	var (
		timeseries = r.Timeseries
		metadata   = r.Metadata
		size       = r.Size()
	)
	release := func() { FinishWriteRequest(r) }

	defer func(size int) {
		if err == nil {
			metrics.IngestorBytes.With(prometheus.Labels{"type": "metric"}).Add(float64(size))
		}
	}(size)

	switch numTs, numMeta := len(timeseries), len(metadata); {
	case numTs > 0 && numMeta == 0:
		// Write request contains only time-series.
		numInsertablesIngested, err = ingestor.ingestTimeseries(ctx, timeseries, release)
		return numInsertablesIngested, 0, err
	case numTs == 0 && numMeta == 0:
		release()
		return 0, 0, nil
	case numMeta > 0 && numTs == 0:
		// Write request contains only metadata.
		numMetadataIngested, err = ingestor.ingestMetadata(ctx, metadata, release)
		return 0, numMetadataIngested, err
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
		n, err := ingestor.ingestTimeseries(ctx, timeseries, release)
		res <- result{series, n, err}
	}()
	go func() {
		n, err := ingestor.ingestMetadata(ctx, metadata, release)
		res <- result{meta, n, err}
	}()

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
			numInsertablesIngested = response.numRows
			err = mergeErr(err, response.err, "ingesting timeseries")
		case meta:
			numMetadataIngested = response.numRows
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
	return numInsertablesIngested, numMetadataIngested, err
}

func (ingestor *DBIngestor) ingestTimeseries(ctx context.Context, timeseries []prompb.TimeSeries, releaseMem func()) (uint64, error) {
	ctx, span := tracer.Default().Start(ctx, "ingest-timeseries")
	defer span.End()
	var (
		totalRowsExpected uint64

		insertables = make(map[string][]model.Insertable)
	)

	for i := range timeseries {
		var (
			err        error
			series     *model.Series
			metricName string

			ts = &timeseries[i]
		)
		if len(ts.Labels) == 0 {
			continue
		}
		// Normalize and canonicalize ts.Labels.
		// After this point ts.Labels should never be used again.
		series, metricName, err = ingestor.sCache.GetSeriesFromProtos(ts.Labels)
		if err != nil {
			return 0, err
		}
		if metricName == "" {
			return 0, errors.ErrNoMetricName
		}

		if len(ts.Samples) > 0 {
			samples, count, err := ingestor.samples(series, ts)
			if err != nil {
				return 0, fmt.Errorf("samples: %w", err)
			}
			totalRowsExpected += uint64(count)
			insertables[metricName] = append(insertables[metricName], samples)
		}
		if len(ts.Exemplars) > 0 {
			exemplars, count, err := ingestor.exemplars(series, ts)
			if err != nil {
				return 0, fmt.Errorf("exemplars: %w", err)
			}
			totalRowsExpected += uint64(count)
			insertables[metricName] = append(insertables[metricName], exemplars)
		}
		// we're going to free req after this, but we still need the samples,
		// so nil the field
		ts.Samples = nil
		ts.Exemplars = nil
	}
	releaseMem()

	numInsertablesIngested, errSamples := ingestor.dispatcher.InsertTs(ctx, model.Data{Rows: insertables, ReceivedTime: time.Now()})
	if errSamples == nil && numInsertablesIngested != totalRowsExpected {
		return numInsertablesIngested, fmt.Errorf("failed to insert all the data! Expected: %d, Got: %d", totalRowsExpected, numInsertablesIngested)
	}
	return numInsertablesIngested, errSamples
}

func (ingestor *DBIngestor) samples(l *model.Series, ts *prompb.TimeSeries) (model.Insertable, int, error) {
	return model.NewPromSamples(l, ts.Samples), len(ts.Samples), nil
}

func (ingestor *DBIngestor) exemplars(l *model.Series, ts *prompb.TimeSeries) (model.Insertable, int, error) {
	return model.NewPromExemplars(l, ts.Exemplars), len(ts.Exemplars), nil
}

// ingestMetadata ingests metric metadata received from Prometheus. It runs as a secondary routine, independent from
// the main dataflow (i.e., samples ingestion) since metadata ingestion is not as frequent as that of samples.
func (ingestor *DBIngestor) ingestMetadata(ctx context.Context, metadata []prompb.MetricMetadata, releaseMem func()) (uint64, error) {
	_, span := tracer.Default().Start(ctx, "ingest-metadata")
	defer span.End()
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
	numMetadataIngested, errMetadata := ingestor.dispatcher.InsertMetadata(ctx, data)
	if errMetadata != nil {
		return 0, errMetadata
	}
	return numMetadataIngested, nil
}

func (ingestor *DBIngestor) Dispatcher() model.Dispatcher {
	return ingestor.dispatcher
}

func (ingestor *DBIngestor) SeriesCache() cache.SeriesCache {
	return ingestor.sCache
}

// Parts of metric creation not needed to insert data
func (ingestor *DBIngestor) CompleteMetricCreation(ctx context.Context) error {
	return ingestor.dispatcher.CompleteMetricCreation(ctx)
}

// Close closes the ingestor
func (ingestor *DBIngestor) Close() {
	if ingestor.closed.Load() {
		return
	}
	ingestor.closed.Store(true)
	ingestor.dispatcher.Close()
}
