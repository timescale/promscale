// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ha

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/common/model"
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
	defer finalFiltering(wr)
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
	allowInsert, leaseStart, err := h.service.CheckLease(minT, maxT, clusterName, replicaName)
	if err != nil {
		return fmt.Errorf("could not check ha lease: %#v", err)
	}

	// Short-circuit for no possible backfill data.
	if !minT.Before(leaseStart) {
		if !allowInsert {
			wr.Timeseries = wr.Timeseries[:0]
		}
		return nil
	}

	hasBackfill, err := h.filterBackfill(wr, minT, leaseStart, clusterName, replicaName)
	if err != nil {
		return fmt.Errorf("could not check backfill ha lease: %#v", err)
	}

	switch {
	case !hasBackfill && allowInsert:
		// Remove all backfill data.
		filterOutSampleRange(wr, minTUnix, toPromModelTime(leaseStart))
	case hasBackfill && !allowInsert:
		// Remove all data in the current lease.
		filterOutSampleRange(wr, toPromModelTime(leaseStart), maxTUnix+1) //maxTUnix+1 because filterSamples treats end as exclusive.
	case !hasBackfill && !allowInsert:
		// No data to insert.
		wr.Timeseries = wr.Timeseries[:0]
	default:
		// This case covers the instance when we have backfill and data in current lease to ingest.
		// The data has already been filtered out so there is nothing to do.
	}
	return nil
}

func toPromModelTime(t time.Time) int64 {
	return int64(model.TimeFromUnixNano(t.UnixNano()))
}

func (h *Filter) filterBackfill(wr *prompb.WriteRequest, minTIncl, maxTExcl time.Time, cluster, replica string) (bool, error) {
	hasBackfill := false
	backfillStart := minTIncl
	for {
		if !backfillStart.Before(maxTExcl) {
			break
		}

		keepRangeStart, keepRangeEnd, err := h.service.GetBackfillLeaseRange(backfillStart, maxTExcl, cluster, replica)
		if err != nil {
			if err == ErrNoLeasesInRange {
				// Nothing else to keep.
				break
			}
			return false, fmt.Errorf("could not check backfill ha lease: %#v", err)
		}

		hasBackfill = true
		// Filter out all samples before the keep range.
		if !keepRangeStart.Before(backfillStart) {
			filterOutSampleRange(
				wr,
				toPromModelTime(backfillStart),
				toPromModelTime(keepRangeStart),
			)
		}

		backfillStart = keepRangeEnd
	}

	// If we had backfill, we need to filter out the range after the keep range
	// till the start of the lease (end of possible backfill).
	if hasBackfill {
		filterOutSampleRange(
			wr,
			toPromModelTime(backfillStart),
			toPromModelTime(maxTExcl),
		)
	}

	return hasBackfill, nil
}

func filterOutSampleRange(wr *prompb.WriteRequest, timeStartIncl, timeEndExcl int64) {
	for i := range wr.Timeseries {
		t := &wr.Timeseries[i]
		numAccepted := 0
		for j := range t.Samples {
			sample := t.Samples[j]
			if sample.Timestamp >= timeStartIncl && sample.Timestamp < timeEndExcl {
				continue
			}
			t.Samples[numAccepted] = sample
			numAccepted++
		}
		for j := numAccepted; j < len(t.Samples); j++ {
			t.Samples[j] = prompb.Sample{}
		}
		t.Samples = t.Samples[:numAccepted]
	}
}

// finalFiltering goes through all the `Timeseries` of a `WriteRequest` filtering
// out any instances without any samples. If the timeseries does contain samples,
// it filters out the HA replica labels so it won't create different series
// based on that label value.
func finalFiltering(wr *prompb.WriteRequest) {
	numAccepted := 0
	for i := range wr.Timeseries {
		t := &wr.Timeseries[i]
		if len(t.Samples) == 0 {
			continue
		}
		wr.Timeseries[numAccepted] = *t
		numAccepted++
		// Drop __replica__ labelSet from samples,
		// we don't want samples from the same Prometheus
		// HA set to become different series.
		for ind, value := range t.Labels {
			if value.Name == ReplicaNameLabel {
				t.Labels = append(t.Labels[:ind], t.Labels[ind+1:]...)
				break
			}
		}
	}
	for j := numAccepted; j < len(wr.Timeseries); j++ {
		wr.Timeseries[j] = prompb.TimeSeries{}
	}
	wr.Timeseries = wr.Timeseries[:numAccepted]
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
