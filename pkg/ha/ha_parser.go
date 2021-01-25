package ha

import (
	"fmt"
	"time"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

type haParser struct {
	service *Service
}

func NewHAParser(service *Service) *haParser {
	return &haParser{
		service: service,
	}
}

// ParseData parses timeseries into a set of samplesInfo infos per-metric.
// returns: map[metric name][]SamplesInfo, total rows to insert
// When Prometheus & Promscale are running HA mode the below parseData is used
// to validate leader replica samples & ha_locks in TimescaleDB.
func (h *haParser) ParseData(tts []prompb.TimeSeries) (map[string][]model.SamplesInfo, int, error) {
	dataSamples := make(map[string][]model.SamplesInfo)
	rows := 0

	var minT, maxT int64

	var replicaName, clusterName string
	if len(tts) > 0 {
		s, _, err := model.LabelProtosToLabels(tts[0].Labels)
		if err != nil {
			return nil, rows, err
		}
		replicaName = s.GetReplicaName()
		clusterName = s.GetClusterName()
	}

	if replicaName == "" || clusterName == "" {
		err := fmt.Errorf(
			"ha mode is enabled and one/both of the %s, %s labels is/are empty",
			model.ClusterNameLabel,
			model.ReplicaNameLabel,
		)
		return nil, rows, err
	}

	for i := range tts {
		t := &tts[i]
		if len(t.Samples) == 0 {
			continue
		}

		if t.Samples[0].Timestamp < minT || minT == 0 {
			minT = t.Samples[0].Timestamp
		}

		if t.Samples[len(t.Samples)-1].Timestamp > maxT {
			maxT = t.Samples[len(t.Samples)-1].Timestamp
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

	// as prometheus remote-write sends timestamps in
	// milliseconds converting them into time.Time
	// Note: time package doesn't offer any milli-sec utilities
	// so manually performing conversion to time.Time.
	minTUnix := time.Unix(0, minT * int64(1000000))
	maxTUnix := time.Unix(0, maxT * int64(1000000))

	ok, err := h.service.checkInsert(minTUnix, maxTUnix, clusterName, replicaName)
	if err != nil {
		return nil, rows, fmt.Errorf("could not check ha lock: %#v", err)
	}
	if !ok {
		log.Debug("msg", "the samples aren't from the leader prom instance. skipping the insert")
		return nil, 0, nil
	}

	return dataSamples, rows, nil
}
