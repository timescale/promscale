// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/timescale/promscale/pkg/api/parser"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/util"
)

type writeStage func(http.ResponseWriter, *http.Request, *Metrics) bool

type writeHandler struct {
	stages  []writeStage
	metrics *Metrics
}

func (wh *writeHandler) addStages(stages ...writeStage) {
	for _, stage := range stages {
		if stage == nil {
			continue
		}
		wh.stages = append(wh.stages, stage)
	}
}

func (wh *writeHandler) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, stage := range wh.stages {
			if !stage(w, r, wh.metrics) {
				return
			}
		}
	})
}

// Write returns an http.Handler that is responsible for data ingest.
func Write(inserter ingestor.DBInserter, dataParser *parser.DefaultParser, elector *util.Elector, metrics *Metrics) http.Handler {
	wh := writeHandler{metrics: metrics}
	wh.addStages(
		validateWriteHeaders,
		checkLegacyHA(elector),
		decodeSnappy,
		ingest(inserter, dataParser),
	)
	return wh.handler()
}

func validateWriteHeaders(w http.ResponseWriter, r *http.Request, m *Metrics) bool {
	// validate headers from https://github.com/prometheus/prometheus/blob/2bd077ed9724548b6a631b6ddba48928704b5c34/storage/remote/client.go
	if r.Method != "POST" {
		validateError(w, fmt.Sprintf("HTTP Method %s instead of POST", r.Method), m)
		return false
	}

	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		validateError(w, "Error parsing media type from Content-Type header", m)
		return false
	}
	switch mediaType {
	case "application/x-protobuf":
		if !strings.Contains(r.Header.Get("Content-Encoding"), "snappy") {
			validateError(w, fmt.Sprintf("non-snappy compressed data got: %s", r.Header.Get("Content-Encoding")), m)
			return false
		}

		remoteWriteVersion := r.Header.Get("X-Prometheus-Remote-Write-Version")
		if remoteWriteVersion == "" {
			validateError(w, "Missing X-Prometheus-Remote-Write-Version header", m)
			return false
		}

		if !strings.HasPrefix(remoteWriteVersion, "0.1.") {
			validateError(w, fmt.Sprintf("unexpected Remote-Write-Version %s, expected 0.1.X", remoteWriteVersion), m)
			return false
		}
	case "application/json":
		// Don't need any other header checks for JSON content type.
	case "text/plain", "application/openmetrics":
		// Don't need any other header checks for text content type.
	default:
		validateError(w, "unsupported data format (not protobuf, JSON, or text format)", m)
		return false
	}

	return true
}

func checkLegacyHA(elector *util.Elector) func(http.ResponseWriter, *http.Request, *Metrics) bool {
	if elector == nil {
		return nil
	}

	return func(w http.ResponseWriter, r *http.Request, m *Metrics) bool {
		// We need to record this time even if we're not the leader as it's
		// used to determine if we're eligible to become the leader.
		atomic.StoreInt64(&m.LastRequestUnixNano, time.Now().UnixNano())

		shouldWrite, err := elector.IsLeader()
		if err != nil {
			m.LeaderGauge.Set(0)
			log.Error("msg", "IsLeader check failed", "err", err)
			return false
		}
		if !shouldWrite {
			m.LeaderGauge.Set(0)
			log.DebugRateLimited("msg", fmt.Sprintf("Election id %v: Instance is not a leader. Can't write data", elector.ID()))
			return false
		}

		m.LeaderGauge.Set(1)
		return true
	}
}

type readCloser struct {
	reader io.Reader
	closer io.Closer
}

func (rc *readCloser) Read(p []byte) (n int, err error) {
	return rc.reader.Read(p)
}

func (rc *readCloser) Close() error {
	return rc.closer.Close()
}

func decodeSnappy(w http.ResponseWriter, r *http.Request, m *Metrics) bool {
	snappyEncoding := strings.Contains(r.Header.Get("Content-Encoding"), "snappy")
	if !snappyEncoding {
		return true
	}
	buf := make([]byte, 10)
	size, err := r.Body.Read(buf)
	if err != nil {
		invalidRequestError(w, "snappy decode error", "payload too short or corrupt", m)
		return false
	}

	//Setup multi-reader so we can read the complete contents.
	mr := io.MultiReader(bytes.NewBuffer(buf[:size]), r.Body)

	if detectSnappyStreamFormat(buf) {
		r.Body = &readCloser{
			reader: snappy.NewReader(mr),
			closer: r.Body,
		}
		return true
	}

	compressed, err := ioutil.ReadAll(mr)
	if err != nil {
		invalidRequestError(w, "request body read error", err.Error(), m)
		return false
	}

	// Snappy block format.
	decoded, err := snappy.Decode(nil, compressed)
	if err != nil {
		invalidRequestError(w, "snappy decode error", err.Error(), m)
		return false
	}

	body := bytes.NewBuffer(decoded)
	r.Body = &readCloser{
		reader: body,
		closer: r.Body,
	}
	return true
}

func ingest(inserter ingestor.DBInserter, dataParser *parser.DefaultParser) func(http.ResponseWriter, *http.Request, *Metrics) bool {
	return func(w http.ResponseWriter, r *http.Request, m *Metrics) bool {
		req := ingestor.NewWriteRequest()
		err := dataParser.ParseRequest(r, req)
		if err != nil {
			ingestor.FinishWriteRequest(req)
			invalidRequestError(w, "parser error", err.Error(), m)
			return false
		}

		// if samples in write request are empty the we do not need to
		// proceed further
		if len(req.Timeseries) == 0 && len(req.Metadata) == 0 {
			ingestor.FinishWriteRequest(req)
			return false
		}

		var receivedBatchCount uint64

		for _, ts := range req.Timeseries {
			receivedBatchCount += uint64(len(ts.Samples))
		}

		m.ReceivedSamples.Add(float64(receivedBatchCount))
		begin := time.Now()

		numSamples, numMetadata, err := inserter.Ingest(req)
		if err != nil {
			log.Warn("msg", "Error sending samples to remote storage", "err", err, "num_samples", numSamples)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			m.FailedSamples.Add(float64(receivedBatchCount - numSamples))
			return false
		}

		duration := time.Since(begin).Seconds()

		m.SentSamples.Add(float64(numSamples))
		m.SentMetadata.Add(float64(numMetadata))
		m.SentBatchDuration.Observe(duration)

		m.WriteThroughput.SetCurrent(util.ThroughputValues{Samples: getCounterValue(m.SentSamples), Metadata: getCounterValue(m.SentMetadata)})

		select {
		case d := <-m.WriteThroughput.Values:
			log.Info("msg", "write-throughput", "samples/sec", d.Samples, "metadata/sec", d.Metadata)
		default:
		}

		return true
	}
}

var promClientPool = sync.Pool{New: func() interface{} { return new(io_prometheus_client.Metric) }}

func getCounterValue(counter prometheus.Counter) float64 {
	dtoMetric := promClientPool.Get().(*io_prometheus_client.Metric)
	if err := counter.Write(dtoMetric); err != nil {
		log.Warn("msg", "Error reading counter value", "err", err, "counter", counter)
	}
	val := dtoMetric.GetCounter().GetValue()
	promClientPool.Put(dtoMetric)
	return val
}

func invalidRequestError(w http.ResponseWriter, msg, err string, m *Metrics) {
	log.Error("msg", msg, "err", err)
	http.Error(w, err, http.StatusBadRequest)
	m.InvalidWriteReqs.Inc()
}

func validateError(w http.ResponseWriter, err string, metrics *Metrics) {
	invalidRequestError(w, "Write header validation error", err, metrics)
}

func detectSnappyStreamFormat(input []byte) bool {
	if len(input) < 10 {
		return false
	}

	// Checking for "magic chunk" which signals the start of snappy stream encoding.
	// More info can be found here:
	// https://github.com/google/snappy/blob/01a566f825e6083318bda85c862c859e198bd98a/framing_format.txt#L68-L81
	if string(input[0:10]) != "\xff\x06\x00\x00\x73\x4e\x61\x50\x70\x59" {
		return false
	}

	return true
}
