package api

import (
	"fmt"
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

		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Error("msg", "Read error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			log.Error("msg", "Decode error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		req := pgmodel.NewWriteRequest()
		if err := proto.Unmarshal(reqBuf, req); err != nil {
			log.Error("msg", "Unmarshal error", "err", err.Error())
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

func isWriter(elector *util.Elector) (bool, error) {
	if elector != nil {
		shouldWrite, err := elector.IsLeader()
		return shouldWrite, err
	}
	return true, nil
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

	if !strings.Contains(r.Header.Get("Content-Encoding"), "snappy") {
		buildWriteError(w, fmt.Sprintf("non-snappy compressed data got: %s", r.Header.Get("Content-Encoding")))
		return false
	}

	if r.Header.Get("Content-Type") != "application/x-protobuf" {
		buildWriteError(w, "non-protobuf data")
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

	return true
}

func buildWriteError(w http.ResponseWriter, err string) {
	log.Error("msg", "Write header validation error", "err", err)
	http.Error(w, err, http.StatusBadRequest)
}
