// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ha

import (
	"fmt"
	"net/http"

	"github.com/prometheus/common/model"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/prompb"
)

const ReplicaNameLabel = "__replica__"
const ClusterNameLabel = "cluster"

// Filter is a HA filter which filters data based on lease information it
// gets from the lease service.
type Filter struct {
	service *Service
}

// NewFilter creates a new Filter based on the provided Service.
func NewFilter(service *Service) *Filter {
	return &Filter{
		service: service,
	}
}

// FilterData validates and filters timeseries based on lease info from the service.
// When Prometheus & Promscale are running HA mode the below FilterData is used
// to validate leader replica samples & ha_locks in TimescaleDB.
func (h *Filter) Process(_ *http.Request, wr *prompb.WriteRequest) error {
	tts := wr.Timeseries
	if len(tts) == 0 {
		return nil
	}

	clusterName, replicaName := haLabels(tts[0].Labels)

	if err := validateClusterLabels(clusterName, replicaName); err != nil {
		return err
	}

	// find samples time range
	minTUnix, maxTUnix := findDataTimeRange(tts)

	minT := model.Time(minTUnix).Time()
	maxT := model.Time(maxTUnix).Time()
	allowInsert, acceptedMinT, err := h.service.CheckLease(minT, maxT, clusterName, replicaName)
	if err != nil {
		return fmt.Errorf("could not check ha lease: %#v", err)
	}
	if !allowInsert {
		log.Debug("msg", "the samples aren't from the leader prom instance. skipping the insert")
		wr.Timeseries = wr.Timeseries[:0]
		return nil
	}

	// insert allowed -> parse samples
	acceptedMinTUnix := int64(model.TimeFromUnixNano(acceptedMinT.UnixNano()))
	for i := range tts {
		t := &tts[i]

		if minTUnix < acceptedMinTUnix {
			t.Samples = filterSamples(t.Samples, acceptedMinTUnix)
		}
		if len(t.Samples) == 0 {
			// Empty the labels for posterity.
			t.Labels = make([]prompb.Label, 0)
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
	}

	return nil
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
