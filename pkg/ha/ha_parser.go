package ha

import (
	"fmt"

	promModel "github.com/prometheus/common/model"
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

	if len(tts) == 0 {
		return dataSamples, rows, nil
	}

	s, _, err := model.LabelProtosToLabels(tts[0].Labels)
	if err != nil {
		return nil, rows, err
	}
	replicaName := s.GetReplicaName()
	clusterName := s.GetClusterName()

	if err := checkClusterAndReplicaLabelAreSet(clusterName, replicaName); err != nil {
		return nil, rows, err
	}

	// find samples time range
	var (
	    minTUnix, maxTUnix int64
	    minTWasSet = false
	)
	for i := range tts {
		t := &tts[i]
		if len(t.Samples) == 0 {
			continue
		}

		for _, sample := range t.Samples {
			if sample.Timestamp < minTUnix || !minTWasSet {
				minTUnix = sample.Timestamp
				minTWasSet = true
			}

			if sample.Timestamp > maxTUnix {
				maxTUnix = sample.Timestamp
			}
		}
	}

	minT := promModel.Time(minTUnix).Time()
	maxT := promModel.Time(maxTUnix).Time()
	allowInsert, acceptedMinT, err := h.service.CheckInsert(minT, maxT, clusterName, replicaName)
	if err != nil {
		return nil, rows, fmt.Errorf("could not check ha lock: %#v", err)
	}
	if !allowInsert {
		log.Debug("msg", "the samples aren't from the leader prom instance. skipping the insert")
		return nil, 0, nil
	}

	// insert allowed -> parse samples
	acceptedMinTUnix := int64(promModel.TimeFromUnixNano(acceptedMinT.UnixNano()))
	for i := range tts {
		t := &tts[i]

		t.Samples = filterSamples(t.Samples, acceptedMinTUnix)
		if len(t.Samples) == 0 {
			continue
		}

		// Drop __replica__ labelSet from samples
		for ind, value := range t.Labels {
			if value.Name == model.ReplicaNameLabel && value.Value == replicaName {
				t.Labels = append(t.Labels[:ind], t.Labels[ind+1:]...)
				break
			}
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

	return dataSamples, rows, nil
}

func filterSamples(samples []prompb.Sample, acceptedMinTUnix int64) []prompb.Sample {
	numAccepted := 0
	for _, sample := range samples {
		if sample.Timestamp < acceptedMinTUnix {
			continue
		}
		samples[numAccepted] = sample
		numAccepted++
	}
	return samples[:numAccepted]
}

func checkClusterAndReplicaLabelAreSet(cluster, replica string) error {
	if cluster == "" && replica == "" {
		return fmt.Errorf("HA enabled, but both %s and %s labels are empty",
			model.ClusterNameLabel,
			model.ReplicaNameLabel,
		)
	} else if cluster == "" {
		return fmt.Errorf("HA enabled, but %s label is empty; %s set to: %s",
			model.ClusterNameLabel,
			model.ReplicaNameLabel,
			replica,
		)
	} else if replica == "" {
		return fmt.Errorf("HA enabled, but %s label is empty; %s set to: %s",
			model.ReplicaNameLabel,
			model.ClusterNameLabel,
			cluster,
		)
	}
	return nil
}
