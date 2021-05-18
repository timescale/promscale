// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ha

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/timescale/promscale/pkg/ha/client"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

func TestHaParserParseData(t *testing.T) {

	leaseStart := time.Unix(1, 0)
	leaseUntil := leaseStart.Add(2 * time.Second)
	behindLease := leaseStart.Add(-500 * time.Millisecond)
	behindLease2 := leaseStart.Add(-250 * time.Millisecond)
	// As Prometheus remote write sends sample timestamps
	// in milli-seconds converting the test samples to milliseconds
	inLeaseTimestamp := leaseStart.Add(time.Second).UnixNano() / 1000000
	behindLeaseTimestamp := (behindLease.UnixNano() / 1000000)
	behindLeaseTimestamp2 := (behindLease2.UnixNano() / 1000000)
	aheadLeaseTimestamp := leaseUntil.Add(time.Second).UnixNano() / 1000000

	tests := []struct {
		name             string
		args             *prompb.WriteRequest
		wanted           *prompb.WriteRequest
		wantErr          bool
		resultError      error
		cluster          string
		setClusterStates []client.LeaseDBState
	}{
		{
			name:    "No timeseries",
			args:    &prompb.WriteRequest{},
			wantErr: false,
			wanted:  &prompb.WriteRequest{},
			cluster: "",
		},
		{
			name: "HA enabled but __replica__ && cluster are empty.",
			args: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: model.MetricNameLabelName, Value: "test"},
						},
						Samples: []prompb.Sample{
							{Timestamp: inLeaseTimestamp, Value: 0.1},
						},
					},
				},
			},
			wantErr:     true,
			resultError: fmt.Errorf("HA enabled, but both cluster and __replica__ labels are empty"),
			cluster:     "",
		},
		{
			name: "HA enabled but __replica__ is empty.",
			args: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: model.MetricNameLabelName, Value: "test"},
							{Name: ClusterNameLabel, Value: "cluster1"},
						},
						Samples: []prompb.Sample{
							{Timestamp: inLeaseTimestamp, Value: 0.1},
						},
					},
				},
			},
			wantErr:     true,
			resultError: fmt.Errorf("HA enabled, but __replica__ label is empty; cluster set to: cluster1"),
			cluster:     "cluster1",
		},
		{
			name: "HA enabled but cluster is empty.",
			args: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: model.MetricNameLabelName, Value: "test"},
							{Name: ReplicaNameLabel, Value: "replica1"},
						},
						Samples: []prompb.Sample{
							{Timestamp: inLeaseTimestamp, Value: 0.1},
						},
					},
				},
			},
			wantErr:     true,
			resultError: fmt.Errorf("HA enabled, but cluster label is empty; __replica__ set to: replica1"),
		},
		{
			name: "HA enabled parse samples from leader prom instance.",
			args: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: model.MetricNameLabelName, Value: "test"},
							{Name: ReplicaNameLabel, Value: "replica1"},
							{Name: ClusterNameLabel, Value: "cluster1"},
						},
						Samples: []prompb.Sample{
							{Timestamp: inLeaseTimestamp, Value: 0.1},
						},
					},
				},
			},
			wantErr: false,
			wanted: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: model.MetricNameLabelName, Value: "test"},
							{Name: ClusterNameLabel, Value: "cluster1"},
						},
						Samples: []prompb.Sample{
							{Timestamp: inLeaseTimestamp, Value: 0.1},
						},
					},
				},
			},
			cluster: "cluster1",
			setClusterStates: []client.LeaseDBState{
				{
					Cluster:    "cluster1",
					Leader:     "replica1",
					LeaseStart: leaseStart,
					LeaseUntil: leaseUntil,
				},
			},
		},
		{
			name: "HA enabled parse samples from standby prom instance.",
			args: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: model.MetricNameLabelName, Value: "test"},
							{Name: ReplicaNameLabel, Value: "replica2"},
							{Name: ClusterNameLabel, Value: "cluster2"},
						},
						Samples: []prompb.Sample{
							{Timestamp: inLeaseTimestamp, Value: 0.1},
						},
					},
				},
			},
			wantErr: false,
			wanted: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{},
			},
			cluster: "cluster2",
			setClusterStates: []client.LeaseDBState{
				{
					Cluster:    "cluster2",
					Leader:     "replica1",
					LeaseStart: leaseStart,
					LeaseUntil: leaseUntil,
				},
			},
		},
		{
			name: "HA enabled parse samples from leader prom instance.",
			args: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: model.MetricNameLabelName, Value: "test"},
							{Name: ReplicaNameLabel, Value: "replica1"},
							{Name: ClusterNameLabel, Value: "cluster3"},
						},
						Samples: []prompb.Sample{
							{Timestamp: inLeaseTimestamp, Value: 0.1},
						},
					},
				},
			},
			wantErr: false,
			wanted: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: model.MetricNameLabelName, Value: "test"},
							{Name: ClusterNameLabel, Value: "cluster3"},
						},
						Samples: []prompb.Sample{
							{Timestamp: inLeaseTimestamp, Value: 0.1},
						},
					},
				},
			},
			cluster: "cluster3",
			setClusterStates: []client.LeaseDBState{
				{
					Cluster:    "cluster3",
					Leader:     "replica1",
					LeaseStart: leaseStart,
					LeaseUntil: leaseUntil,
				},
			},
		},
		{
			name: "HA enabled parse from leader & samples are in interval [leaseStart-X, leaseUntil)",
			args: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: model.MetricNameLabelName, Value: "test"},
							{Name: ReplicaNameLabel, Value: "replica1"},
							{Name: ClusterNameLabel, Value: "cluster3"},
						},
						Samples: []prompb.Sample{
							{Timestamp: behindLeaseTimestamp, Value: 0.1},
							{Timestamp: inLeaseTimestamp, Value: 0.2},
						},
					},
				},
			},
			wantErr: false,
			wanted: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: model.MetricNameLabelName, Value: "test"},
							{Name: ClusterNameLabel, Value: "cluster3"},
						},
						Samples: []prompb.Sample{
							{Timestamp: inLeaseTimestamp, Value: 0.2},
						},
					},
				},
			},
			cluster: "cluster3",
			setClusterStates: []client.LeaseDBState{
				{
					Cluster:    "cluster3",
					Leader:     "replica1",
					LeaseStart: leaseStart,
					LeaseUntil: leaseUntil,
				},
			},
		},
		{
			name: "HA enabled parse from leader & samples are in interval [leaseStart, leaseUntil+X].",
			args: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: model.MetricNameLabelName, Value: "test"},
							{Name: ReplicaNameLabel, Value: "replica1"},
							{Name: ClusterNameLabel, Value: "cluster3"},
						},
						Samples: []prompb.Sample{
							{Timestamp: aheadLeaseTimestamp, Value: 0.1},
						},
					},
				},
			},
			wantErr: false,
			wanted: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: model.MetricNameLabelName, Value: "test"},
							{Name: ClusterNameLabel, Value: "cluster3"},
						},
						Samples: []prompb.Sample{
							{Timestamp: aheadLeaseTimestamp, Value: 0.1},
						},
					},
				},
			},
			cluster: "cluster3",
			setClusterStates: []client.LeaseDBState{
				{
					Cluster:    "cluster3",
					Leader:     "replica1",
					LeaseStart: leaseStart,
					LeaseUntil: leaseUntil,
				},
			},
		},
		{
			name: "HA enabled, parse samples from standby instance. ReadLeaseState returns the updated leader as standby prom instance.",
			args: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: model.MetricNameLabelName, Value: "test"},
							{Name: ReplicaNameLabel, Value: "replica2"},
							{Name: ClusterNameLabel, Value: "cluster4"},
						},
						Samples: []prompb.Sample{
							{Timestamp: inLeaseTimestamp, Value: 0.1},
						},
					},
				},
			},
			wantErr: false,
			wanted: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: model.MetricNameLabelName, Value: "test"},
							{Name: ClusterNameLabel, Value: "cluster4"},
						},
						Samples: []prompb.Sample{
							{Timestamp: inLeaseTimestamp, Value: 0.1},
						},
					},
				},
			},
			cluster: "cluster4",
			setClusterStates: []client.LeaseDBState{
				{
					Cluster:    "cluster4",
					Leader:     "replica2",
					LeaseStart: leaseStart,
					LeaseUntil: leaseUntil,
				},
			},
		},
		{
			name: "HA enabled parse from standby. ReadLeaseState returns the updated leader as standby prom instance but samples aren't part lease range.",
			args: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: model.MetricNameLabelName, Value: "test"},
							{Name: ReplicaNameLabel, Value: "replica2"},
							{Name: ClusterNameLabel, Value: "cluster5"},
						},
						Samples: []prompb.Sample{
							{Timestamp: behindLeaseTimestamp, Value: 0.1},
						},
					},
				},
			},
			wantErr: false,
			wanted: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{},
			},
			cluster: "cluster5",
			setClusterStates: []client.LeaseDBState{
				{
					Cluster:    "cluster5",
					Leader:     "replica2",
					LeaseStart: leaseStart,
					LeaseUntil: leaseUntil,
				},
			},
		},
		{
			name: "HA backfilling from previous leases",
			args: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: model.MetricNameLabelName, Value: "test"},
							{Name: ReplicaNameLabel, Value: "replica1"},
							{Name: ClusterNameLabel, Value: "cluster5"},
						},
						Samples: []prompb.Sample{
							{Timestamp: behindLeaseTimestamp, Value: 0.1},
							{Timestamp: behindLeaseTimestamp2, Value: 0.2},
						},
					},
				},
			},
			wantErr: false,
			wanted: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: model.MetricNameLabelName, Value: "test"},
							{Name: ClusterNameLabel, Value: "cluster5"},
						},
						Samples: []prompb.Sample{
							{Timestamp: behindLeaseTimestamp, Value: 0.1},
						},
					},
				},
			},
			cluster: "cluster5",
			setClusterStates: []client.LeaseDBState{
				{
					Cluster:    "cluster5",
					Leader:     "replica1",
					LeaseStart: behindLease,
					LeaseUntil: behindLease2,
				},
				{
					Cluster:    "cluster5",
					Leader:     "replica2",
					LeaseStart: leaseStart,
					LeaseUntil: leaseUntil,
				},
			},
		},
		{
			name: "Not allowed to insert backfill or in lease",
			args: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: model.MetricNameLabelName, Value: "test"},
							{Name: ReplicaNameLabel, Value: "replica1"},
							{Name: ClusterNameLabel, Value: "cluster5"},
						},
						Samples: []prompb.Sample{
							{Timestamp: inLeaseTimestamp, Value: 0.3},
							{Timestamp: behindLeaseTimestamp, Value: 0.1},
							{Timestamp: behindLeaseTimestamp2, Value: 0.2},
						},
					},
				},
			},
			wantErr: false,
			wanted: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{},
			},
			cluster: "cluster5",
			setClusterStates: []client.LeaseDBState{
				{
					Cluster:    "cluster5",
					Leader:     "replica2",
					LeaseStart: leaseStart,
					LeaseUntil: leaseUntil,
				},
			},
		},
	}
	for _, c := range tests {
		t.Run(c.name, func(t *testing.T) {
			service := MockNewHAService()
			if c.setClusterStates != nil {
				SetLeaderInMockService(service, c.setClusterStates)
			}
			h := NewFilter(service)
			err := h.Process(nil, c.args)
			if err != nil {
				if !c.wantErr {
					t.Fatalf("Process() returned unexpected error: %s", err.Error())
				}
				if err.Error() != c.resultError.Error() {
					t.Fatalf("Process() error = %v, wantErr %v", err, c.resultError)
				}
				return
			} else if c.wantErr {
				t.Fatalf("wanted error from Process, got nil")
			}

			if !reflect.DeepEqual(c.wanted, c.args) {
				t.Fatalf("unexpected result from Process:\ngot\n%+v\nwant\n%+v\n", c.args, c.wanted)
			}
		})
	}
}

func TestFilterSamples(t *testing.T) {
	testCases := []struct {
		wr        *prompb.WriteRequest
		timeStart int64
		timeEnd   int64
		expected  *prompb.WriteRequest
	}{
		{
			wr: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Samples: []prompb.Sample{},
					},
					{
						Samples: []prompb.Sample{
							{Timestamp: 1},
							{Timestamp: 2},
							{Timestamp: 3},
						},
					},
					{
						Samples: []prompb.Sample{
							{Timestamp: 5},
							{Timestamp: 6},
							{Timestamp: 7},
							{Timestamp: 8},
							{Timestamp: 9},
							{Timestamp: 10},
						},
					},
					{
						Samples: []prompb.Sample{
							{Timestamp: 10},
							{Timestamp: 11},
							{Timestamp: 12},
						},
					},
					{
						Samples: []prompb.Sample{
							{Timestamp: 11},
							{Timestamp: 6},
							{Timestamp: 7},
							{Timestamp: 1},
							{Timestamp: 9},
							{Timestamp: 10},
						},
					},
				},
			},
			timeStart: 5,
			timeEnd:   10,
			expected: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Samples: []prompb.Sample{},
					},
					{
						Samples: []prompb.Sample{
							{Timestamp: 1},
							{Timestamp: 2},
							{Timestamp: 3},
						},
					},
					{
						Samples: []prompb.Sample{
							{Timestamp: 10},
						},
					},
					{
						Samples: []prompb.Sample{
							{Timestamp: 10},
							{Timestamp: 11},
							{Timestamp: 12},
						},
					},
					{
						Samples: []prompb.Sample{
							{Timestamp: 11},
							{Timestamp: 1},
							{Timestamp: 10},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		filterOutSampleRange(tc.wr, tc.timeStart, tc.timeEnd)
		if !reflect.DeepEqual(*tc.expected, *tc.wr) {
			t.Fatalf("unexpected output.\nexpected: %v\n got: %v", tc.expected, tc.wr)
		}
	}

}
