// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"bytes"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/timescale/promscale/pkg/api/parser"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/tracer"
)

type writeStage func(http.ResponseWriter, *http.Request) bool

type writeHandler struct {
	stages []writeStage
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
			if !stage(w, r) {
				return
			}
		}
	})
}

// Write returns an http.Handler that is responsible for data ingest.
func Write(
	inserter ingestor.DBInserter,
	dataParser *parser.DefaultParser,
	updateMetrics func(code string, duration, receivedSamples, receivedMetadata float64),
) http.Handler {
	wh := writeHandler{}
	wh.addStages(
		validateWriteHeaders,
		decodeSnappy,
		ingest(inserter, dataParser, updateMetrics),
	)
	return wh.handler()
}

func validateWriteHeaders(w http.ResponseWriter, r *http.Request) bool {
	// validate headers from https://github.com/prometheus/prometheus/blob/2bd077ed9724548b6a631b6ddba48928704b5c34/storage/remote/client.go
	_, span := tracer.Default().Start(r.Context(), "validate-write-headers")
	defer span.End()
	if r.Method != "POST" {
		validateError(w, fmt.Sprintf("HTTP Method %s instead of POST", r.Method), metrics)
		return false
	}

	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		validateError(w, "Error parsing media type from Content-Type header", metrics)
		return false
	}
	switch mediaType {
	case "application/x-protobuf":
		if !strings.Contains(r.Header.Get("Content-Encoding"), "snappy") {
			validateError(w, fmt.Sprintf("non-snappy compressed data got: %s", r.Header.Get("Content-Encoding")), metrics)
			return false
		}

		remoteWriteVersion := r.Header.Get("X-Prometheus-Remote-Write-Version")
		if remoteWriteVersion == "" {
			validateError(w, "Missing X-Prometheus-Remote-Write-Version header", metrics)
			return false
		}

		if !strings.HasPrefix(remoteWriteVersion, "0.1.") {
			validateError(w, fmt.Sprintf("unexpected Remote-Write-Version %s, expected 0.1.X", remoteWriteVersion), metrics)
			return false
		}
	case "application/json":
		// Don't need any other header checks for JSON content type.
	case "text/plain", "application/openmetrics":
		// Don't need any other header checks for text content type.
	default:
		validateError(w, "unsupported data format (not protobuf, JSON, or text format)", metrics)
		return false
	}

	return true
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

type funcCloser func() error

func (f funcCloser) Close() error {
	return f()
}

func decodeSnappy(w http.ResponseWriter, r *http.Request) bool {
	_, span := tracer.Default().Start(r.Context(), "decode-snappy")
	defer span.End()

	snappyEncoding := strings.Contains(r.Header.Get("Content-Encoding"), "snappy")
	if !snappyEncoding {
		return true
	}
	buf := make([]byte, 10)
	originalBody := r.Body
	size, err := r.Body.Read(buf)
	if err != nil {
		invalidRequestError(w, "snappy decode error", "payload too short or corrupt", metrics)
		return false
	}

	//Setup multi-reader so we can read the complete contents.
	mr := io.MultiReader(bytes.NewBuffer(buf[:size]), r.Body)

	if detectSnappyStreamFormat(buf) {
		r.Body = &readCloser{
			reader: snappy.NewReader(mr),
			closer: originalBody,
		}
		return true
	}

	compressed := compressedBufPool.Get().(*bytes.Buffer)
	compressed.Reset()
	defer compressedBufPool.Put(compressed)

	_, err = compressed.ReadFrom(mr)
	if err != nil {
		invalidRequestError(w, "request body read error", err.Error(), metrics)
		return false
	}

	b := decodedBufPool.Get().(*bytes.Buffer)
	b.Reset()
	n, err := snappy.DecodedLen(compressed.Bytes())
	if err != nil {
		invalidRequestError(w, "snappy decode length error", err.Error(), metrics)
		return false
	}
	b.Grow(n)

	// Snappy block format.
	decoded, err := snappy.Decode(b.Bytes()[:n], compressed.Bytes())
	if err != nil {
		invalidRequestError(w, "snappy decode error", err.Error(), metrics)
		return false
	}

	newBuf := bytes.NewBuffer(decoded)
	r.Body = &readCloser{
		reader: newBuf,
		closer: funcCloser(func() error {
			decodedBufPool.Put(newBuf)
			return originalBody.Close()
		}),
	}
	return true
}

var compressedBufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

var decodedBufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func ingest(
	inserter ingestor.DBInserter,
	dataParser *parser.DefaultParser,
	updateMetrics func(code string, durationSeconds, receivedSamples, receivedMetadata float64),
) func(http.ResponseWriter, *http.Request) bool {
	return func(w http.ResponseWriter, r *http.Request) bool {
		begin := time.Now()
		statusCode := "400"
		numSamplesReceived := uint64(0)
		numMetadataReceived := uint64(0)
		defer func() {
			updateMetrics(
				statusCode,
				time.Since(begin).Seconds(),
				float64(numSamplesReceived), float64(numMetadataReceived),
			)
		}()
		ctx, span := tracer.Default().Start(r.Context(), "ingest")
		defer span.End()

		req := ingestor.NewWriteRequest()
		err := dataParser.ParseRequest(r, req)
		if err != nil {
			ingestor.FinishWriteRequest(req)
			invalidRequestError(w, "parser error", err.Error(), metrics)
			return false
		}
		numSamplesReceived = uint64(getTotalSamples(req))
		numMetadataReceived = uint64(len(req.Metadata))

		// if samples in write request are empty then we do not need to
		// proceed further
		if len(req.Timeseries) == 0 && len(req.Metadata) == 0 {
			statusCode = "2xx"
			ingestor.FinishWriteRequest(req)
			return false
		}

		numSamples, _, err := inserter.IngestMetrics(ctx, req)
		if err != nil {
			statusCode = "500"
			log.Warn("msg", "Error sending samples to remote storage", "err", err, "num_samples", numSamples)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return false
		}
		statusCode = "2xx"
		return true
	}
}

func invalidRequestError(w http.ResponseWriter, msg, err string, m *Metrics) {
	log.Error("msg", msg, "err", err)
	http.Error(w, err, http.StatusBadRequest)
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

func getTotalSamples(wr *prompb.WriteRequest) int {
	total := 0
	for _, ts := range wr.Timeseries {
		total += len(ts.Samples)
	}
	return total
}
