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
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

func TestHaParserParseData(t *testing.T) {
	type fields struct {
		service *Service
	}

	leaseStart := time.Unix(1, 0)
	leaseUntil := leaseStart.Add(2 * time.Second)
	// As Prometheus remote write sends sample timestamps
	// in milli-seconds converting the test samples to milliseconds
	inLeaseTimestamp := leaseStart.Add(time.Second).UnixNano() / 1000000
	behindLeaseTimestamp := (leaseStart.UnixNano() / 1000000) - 1
	aheadLeaseTimestamp := leaseUntil.Add(time.Second).UnixNano() / 1000000

	mockService := MockNewHAService(nil)

	tests := []struct {
		name            string
		fields          fields
		args            *prompb.WriteRequest
		wanted          *prompb.WriteRequest
		wantErr         bool
		resultError     error
		cluster         string
		setClusterState *client.LeaseDBState
	}{
		{
			name:   "Test: HA enabled but __replica__ && cluster are empty.",
			fields: fields{service: mockService},
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
			name:   "Test: HA enabled but __replica__ is empty.",
			fields: fields{service: mockService},
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
			name:   "Test: HA enabled but cluster is empty.",
			fields: fields{service: mockService},
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
			name:   "Test: HA enabled parse samples from leader prom instance.",
			fields: fields{service: mockService},
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
			setClusterState: &client.LeaseDBState{
				Cluster:    "cluster1",
				Leader:     "replica1",
				LeaseStart: leaseStart,
				LeaseUntil: leaseUntil,
			},
		},
		{
			name:   "Test: HA enabled parse samples from standby prom instance.",
			fields: fields{service: mockService},
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
			setClusterState: &client.LeaseDBState{
				Cluster:    "cluster2",
				Leader:     "replica1",
				LeaseStart: leaseStart,
				LeaseUntil: leaseUntil,
			},
		},
		{
			name:   "Test: HA enabled parse samples from leader prom instance.",
			fields: fields{service: mockService},
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
			setClusterState: &client.LeaseDBState{
				Cluster:    "cluster3",
				Leader:     "replica1",
				LeaseStart: leaseStart,
				LeaseUntil: leaseUntil,
			},
		},
		{
			name:   "Test: HA enabled parse from leader & samples are in interval [leaseStart-X, leaseUntil)",
			fields: fields{service: mockService},
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
			setClusterState: &client.LeaseDBState{
				Cluster:    "cluster3",
				Leader:     "replica1",
				LeaseStart: leaseStart,
				LeaseUntil: leaseUntil,
			},
		},
		{
			name:   "Test: HA enabled parse from leader & samples are in interval [leaseStart, leaseUntil+X].",
			fields: fields{service: mockService},
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
			setClusterState: &client.LeaseDBState{
				Cluster:    "cluster3",
				Leader:     "replica1",
				LeaseStart: leaseStart,
				LeaseUntil: leaseUntil,
			},
		},
		{
			name:   "Test: HA enabled, parse samples from standby instance. ReadLeaseState returns the updated leader as standby prom instance.",
			fields: fields{service: mockService},
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
			setClusterState: &client.LeaseDBState{
				Cluster:    "cluster4",
				Leader:     "replica2",
				LeaseStart: leaseStart,
				LeaseUntil: leaseUntil,
			},
		},
		{
			name:   "Test: HA enabled parse from standby. ReadLeaseState returns the updated leader as standby prom instance but samples aren't part lease range.",
			fields: fields{service: mockService},
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
				Timeseries: []prompb.TimeSeries{
					{
						Samples: make([]prompb.Sample, 0),
						Labels:  make([]prompb.Label, 0),
					},
				},
			},
			cluster: "cluster5",
			setClusterState: &client.LeaseDBState{
				Cluster:    "cluster5",
				Leader:     "replica2",
				LeaseStart: leaseStart,
				LeaseUntil: leaseUntil,
			},
		},
	}
	for _, c := range tests {
		t.Run(c.name, func(t *testing.T) {
			if c.setClusterState != nil {
				state := c.setClusterState
				SetLeaderInMockService(c.fields.service, state.Cluster, state.Leader, state.LeaseStart, state.LeaseUntil)
			}
			h := &Filter{
				service: c.fields.service,
			}
			err := h.Process(nil, c.args)
			if err != nil {
				if !c.wantErr {
					t.Fatalf("FilterData() returned unexpected error: %s", err.Error())
				}
				if err.Error() != c.resultError.Error() {
					t.Fatalf("FilterData() error = %v, wantErr %v", err, c.resultError)
				}
				return
			} else if c.wantErr {
				t.Fatalf("wanted error from FilterData, got nil")
			}

			if !reflect.DeepEqual(c.wanted, c.args) {
				t.Fatalf("unexpected result from FilterData:\ngot\n%+v\nwant\n%+v\n", c.args, c.wanted)
			}
		})
	}
}

func TestFilterSamples(t *testing.T) {
	testCases := []struct {
		samples      []prompb.Sample
		acceptedMinT int64
		expected     []prompb.Sample
	}{
		{samples: []prompb.Sample{}, acceptedMinT: 1, expected: []prompb.Sample{}},
		{
			samples:      []prompb.Sample{{Timestamp: 1}, {Timestamp: 2}, {Timestamp: 3}},
			acceptedMinT: 2,
			expected:     []prompb.Sample{{Timestamp: 2}, {Timestamp: 3}},
		},
		{
			samples:      []prompb.Sample{{Timestamp: 3}, {Timestamp: 1}, {Timestamp: 2}},
			acceptedMinT: 2,
			expected:     []prompb.Sample{{Timestamp: 3}, {Timestamp: 2}},
		},
	}

	for _, tc := range testCases {
		out := filterSamples(tc.samples, tc.acceptedMinT)
		pointerOut, _ := fmt.Printf("%p", out)
		pointerSamples, _ := fmt.Printf("%p", tc.samples)
		if pointerOut != pointerSamples {
			t.Fatal("function returned new slice instead of modifying existing")
		}
		if !reflect.DeepEqual(tc.expected, out) {
			t.Fatalf("unexpected output.\nexpected: %v\n got: %v", tc.expected, out)
		}
		for i := len(tc.expected); i < len(tc.samples); i++ {
			if !reflect.DeepEqual(tc.samples[i], prompb.Sample{}) {
				t.Fatalf("filtered out samples were not nulled out")
			}
		}
	}

}
