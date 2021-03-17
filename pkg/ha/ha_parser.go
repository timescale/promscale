// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ha

import (
	"fmt"
	promModel "github.com/prometheus/common/model"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

const ReplicaNameLabel = "__replica__"
const ClusterNameLabel = "cluster"

type haParser struct {
	service *Service
	scache  cache.SeriesCache
}

func NewHAParser(service *Service, scache cache.SeriesCache) *haParser {
	return &haParser{
		service: service,
		scache:  scache,
	}
}

// ParseData parses timeseries into a set of samplesInfo infos per-metric.
// returns: map[metric name][]SamplesInfo, total rows to insert
// When Prometheus & Promscale are running HA mode the below parseData is used
// to validate leader replica samples & ha_locks in TimescaleDB.
func (h *haParser) ParseData(tts []prompb.TimeSeries) (map[string][]model.Samples, int, error) {
	if len(tts) == 0 {
		return nil, 0, nil
	}

	clusterName, replicaName := haLabels(tts[0].Labels)

	if err := validateClusterLabels(clusterName, replicaName); err != nil {
		return nil, 0, err
	}

	// find samples time range
	minTUnix, maxTUnix := findDataTimeRange(tts)

	minT := promModel.Time(minTUnix).Time()
	maxT := promModel.Time(maxTUnix).Time()
	allowInsert, acceptedMinT, err := h.service.CheckLease(minT, maxT, clusterName, replicaName)
	if err != nil {
		return nil, 0, fmt.Errorf("could not check ha lease: %#v", err)
	}
	if !allowInsert {
		log.Debug("msg", "the samples aren't from the leader prom instance. skipping the insert")
		return nil, 0, nil
	}

	// insert allowed -> parse samples
	acceptedMinTUnix := int64(promModel.TimeFromUnixNano(acceptedMinT.UnixNano()))
	dataSamples := make(map[string][]model.Samples)
	rows := 0
	for i := range tts {
		t := &tts[i]

		if minTUnix < acceptedMinTUnix {
			t.Samples = filterSamples(t.Samples, acceptedMinTUnix)
		}
		if len(t.Samples) == 0 {
			continue
		}

		// Drop __replica__ labelSet from samples,
		// we don't want samples from the same Prometheus
		// HA set to become different series.
		for ind, value := range t.Labels {
			if value.Name == ReplicaNameLabel && value.Value == replicaName {
				t.Labels = append(t.Labels[:ind], t.Labels[ind+1:]...)
				break
			}
		}

		// Normalize and canonicalize t.Labels.
		// After this point t.Labels should never be used again.
		seriesLabels, metricName, err := h.scache.GetSeriesFromProtos(t.Labels)
		if err != nil {
			return nil, 0, err
		}
		if metricName == "" {
			return nil, 0, errors.ErrNoMetricName
		}

		sample := model.NewPromSample(seriesLabels, t.Samples)
		rows += len(t.Samples)

		dataSamples[metricName] = append(dataSamples[metricName], sample)
		// we're going to free req after this, but we still need the samples,
		// so nil the field
		t.Samples = nil
	}

	return dataSamples, rows, nil
}

// findDataTimeRange finds the minimum and maximum timestamps in a set of samples
func findDataTimeRange(tts []prompb.TimeSeries) (minTUnix int64, maxTUnix int64) {
	timesWereSet := false
	for i := range tts {
		t := &tts[i]
		if len(t.Samples) == 0 {
			continue
		}

		for _, sample := range t.Samples {
			if !timesWereSet {
				timesWereSet = true
				minTUnix = sample.Timestamp
				maxTUnix = sample.Timestamp
				continue
			}
			if sample.Timestamp < minTUnix {
				minTUnix = sample.Timestamp
			}

			if sample.Timestamp > maxTUnix {
				maxTUnix = sample.Timestamp
			}
		}
	}
	return minTUnix, maxTUnix
}

func haLabels(labels []prompb.Label) (cluster, replica string) {
	for _, label := range labels {
		if label.Name == ClusterNameLabel {
			cluster = label.Value
		} else if label.Name == ReplicaNameLabel {
			replica = label.Value
		}
	}
	return cluster, replica
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
	for i := numAccepted; i < len(samples); i++ {
		samples[i] = prompb.Sample{}
	}
	return samples[:numAccepted]
}

func validateClusterLabels(cluster, replica string) error {
	if cluster == "" && replica == "" {
		return fmt.Errorf("HA enabled, but both %s and %s labels are empty",
			ClusterNameLabel,
			ReplicaNameLabel,
		)
	} else if cluster == "" {
		return fmt.Errorf("HA enabled, but %s label is empty; %s set to: %s",
			ClusterNameLabel,
			ReplicaNameLabel,
			replica,
		)
	} else if replica == "" {
		return fmt.Errorf("HA enabled, but %s label is empty; %s set to: %s",
			ReplicaNameLabel,
			ClusterNameLabel,
			cluster,
		)
	}
	return nil
}
