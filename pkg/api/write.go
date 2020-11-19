package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/util"
)

func Write(writer pgmodel.DBInserter, elector *util.Elector, metrics *Metrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		// we treat invalid requests as the same as no request for
		// leadership-timeout purposes
		if !validateWriteHeaders(w, r) {
			metrics.InvalidWriteReqs.Inc()
			return
		}

		// We need to record this time even if we're not the leader as it's
		// used to determine if we're eligible to become the leader.
		atomic.StoreInt64(&metrics.LastRequestUnixNano, time.Now().UnixNano())

		shouldWrite, err := isWriter(elector)
		if err != nil {
			metrics.LeaderGauge.Set(0)
			log.Error("msg", "IsLeader check failed", "err", err)
			return
		}
		if !shouldWrite {
			metrics.LeaderGauge.Set(0)
			log.Debug("msg", fmt.Sprintf("Election id %v: Instance is not a leader. Can't write data", elector.ID()))
			return
		}

		metrics.LeaderGauge.Set(1)

		req, err, logMsg := loadWriteRequest(r)
		if err != nil {
			log.Error("msg", logMsg, "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		ts := req.GetTimeseries()
		receivedBatchCount := 0

		for _, t := range ts {
			receivedBatchCount = receivedBatchCount + len(t.Samples)
		}

		metrics.ReceivedSamples.Add(float64(receivedBatchCount))
		begin := time.Now()

		numSamples, err := writer.Ingest(req.GetTimeseries(), req)
		if err != nil {
			log.Warn("msg", "Error sending samples to remote storage", "err", err, "num_samples", numSamples)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			metrics.FailedSamples.Add(float64(receivedBatchCount))
			return
		}

		duration := time.Since(begin).Seconds()

		metrics.SentSamples.Add(float64(numSamples))
		metrics.SentBatchDuration.Observe(duration)

		metrics.WriteThroughput.SetCurrent(getCounterValue(metrics.SentSamples))

		select {
		case d := <-metrics.WriteThroughput.Values:
			log.Info("msg", "Samples write throughput", "samples/sec", d)
		default:
		}

	})
}

func loadWriteRequest(r *http.Request) (*prompb.WriteRequest, error, string) {
	var req *prompb.WriteRequest

	switch r.Header.Get("Content-Type") {
	case "application/x-protobuf":
		reqBuf, err, msg := decodeSnappyBody(r.Body)
		if err != nil {
			return nil, err, msg
		}

		req = pgmodel.NewWriteRequest()
		if err := proto.Unmarshal(reqBuf, req); err != nil {
			pgmodel.FinishWriteRequest(req)
			return nil, err, "Protobuf unmarshal error"
		}
	case "application/json":
		var (
			i    importPayload
			body io.Reader = r.Body
			err  error
			msg  string
		)

		snappyEncoding := strings.Contains(r.Header.Get("Content-Encoding"), "snappy")

		if snappyEncoding {
			body, err, msg = decodeSnappy(r.Body)

			if err != nil {
				return nil, err, msg
			}
		}

		dec := json.NewDecoder(body)
		req = pgmodel.NewWriteRequest()

		for {
			if err := dec.Decode(&i); err == io.EOF {
				break
			} else if err != nil {
				pgmodel.FinishWriteRequest(req)
				return nil, err, "JSON decode error"
			}

			req = appendImportPayload(i, req)
		}
	default:
		// Cannot get here because of header validation.
		return nil, fmt.Errorf("unsupported data format (not protobuf or json)"), "Write header validation error"
	}

	return req, nil, ""
}

func isWriter(elector *util.Elector) (bool, error) {
	if elector != nil {
		shouldWrite, err := elector.IsLeader()
		return shouldWrite, err
	}
	return true, nil
}

func decodeSnappy(r io.Reader) (io.Reader, error, string) {
	buf := make([]byte, 10)
	_, err := r.Read(buf)
	if err != nil {
		return nil, err, "snappy payload too short or corrupt"
	}

	//Setup multi-reader so we can read the complete contents.
	mr := io.MultiReader(bytes.NewBuffer(buf), r)

	if detectSnappyStreamFormat(buf) {
		return snappy.NewReader(mr), nil, ""
	}

	// Snappy block format.
	reqBuf, err, msg := decodeSnappyBody(mr)
	if err != nil {
		return nil, err, msg
	}

	return bytes.NewBuffer(reqBuf), nil, ""
}

func decodeSnappyBody(r io.Reader) ([]byte, error, string) {
	compressed, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err, "Read error"
	}

	buf, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, err, "Snappy decode error"
	}

	return buf, nil, ""
}

func getCounterValue(counter prometheus.Counter) float64 {
	dtoMetric := &io_prometheus_client.Metric{}
	if err := counter.Write(dtoMetric); err != nil {
		log.Warn("msg", "Error reading counter value", "err", err, "counter", counter)
	}
	return dtoMetric.GetCounter().GetValue()
}

func validateWriteHeaders(w http.ResponseWriter, r *http.Request) bool {
	// validate headers from https://github.com/prometheus/prometheus/blob/2bd077ed9724548b6a631b6ddba48928704b5c34/storage/remote/client.go
	if r.Method != "POST" {
		buildWriteError(w, fmt.Sprintf("HTTP Method %s instead of POST", r.Method))
		return false
	}

	switch r.Header.Get("Content-Type") {
	case "application/x-protobuf":
		if !strings.Contains(r.Header.Get("Content-Encoding"), "snappy") {
			buildWriteError(w, fmt.Sprintf("non-snappy compressed data got: %s", r.Header.Get("Content-Encoding")))
			return false
		}

		remoteWriteVersion := r.Header.Get("X-Prometheus-Remote-Write-Version")
		if remoteWriteVersion == "" {
			buildWriteError(w, "Missing X-Prometheus-Remote-Write-Version header")
			return false
		}

		if !strings.HasPrefix(remoteWriteVersion, "0.1.") {
			buildWriteError(w, fmt.Sprintf("unexpected Remote-Write-Version %s, expected 0.1.X", remoteWriteVersion))
			return false
		}
	case "application/json":
		// Don't need any other header checks for JSON content type.
	default:
		buildWriteError(w, "unsupported data format (not protobuf or json)")
		return false
	}

	return true
}

func buildWriteError(w http.ResponseWriter, err string) {
	log.Error("msg", "Write header validation error", "err", err)
	http.Error(w, err, http.StatusBadRequest)
}

type importPayload struct {
	Labels  map[string]string `json:"labels"`
	Samples []importSample    `json:"samples"`
}

type importSample struct {
	Timestamp int64
	Value     float64
}

func (i *importSample) UnmarshalJSON(data []byte) error {
	var (
		s  string
		v  []interface{}
		ok bool
	)
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	if len(v) != 2 {
		return fmt.Errorf("invalid sample, should contain timestamp and value: %v", v)
	}

	// Verify that timestamp doesn't contain fractions so we don't silently
	// drop precision.
	s = string(data)
	idx := strings.IndexRune(s, '.')
	if idx != -1 && idx < strings.IndexRune(s, ',') {
		return fmt.Errorf("timestamp cannot contain decimal parts in sample: %s", data)
	}

	floatTimestamp, ok := v[0].(float64)
	if !ok {
		return fmt.Errorf("cannot convert timestamp to int64 type for sample: %s", data)
	}
	i.Timestamp = int64(floatTimestamp)

	i.Value, ok = v[1].(float64)
	if !ok {
		return fmt.Errorf("cannot convert value to float64 type for sample: %v", v)
	}

	return nil
}

func appendImportPayload(i importPayload, req *prompb.WriteRequest) *prompb.WriteRequest {
	if len(i.Samples) == 0 || len(i.Labels) == 0 {
		return req
	}

	ts := prompb.TimeSeries{}
	ts.Labels = make([]prompb.Label, 0, len(i.Labels))
	for name, value := range i.Labels {
		ts.Labels = append(ts.Labels, prompb.Label{
			Name:  name,
			Value: value,
		})
	}

	for _, sample := range i.Samples {
		ts.Samples = append(ts.Samples, prompb.Sample{
			Timestamp: sample.Timestamp,
			Value:     sample.Value,
		})
	}

	req.Timeseries = append(req.Timeseries, ts)
	return req
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
