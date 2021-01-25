package ha

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

func Test_haParser_ParseData(t *testing.T) {
	type fields struct {
		service *Service
	}
	type args struct {
		tts []prompb.TimeSeries
	}

	clusterInfo := []*haLockState{
		{
			cluster:    "cluster1",
			leader:     "replica1",
			leaseStart: time.Now().Add(-5 * time.Minute),
			leaseUntil: time.Now().Add(2 * time.Minute),
		},
		{
			cluster:    "cluster2",
			leader:     "replica1",
			leaseStart: time.Now().Add(-5 * time.Minute),
			leaseUntil: time.Now().Add(2 * time.Minute),
		},
		{
			cluster:    "cluster3",
			leader:     "replica1",
			leaseStart: time.Now().Add(-5 * time.Minute),
			leaseUntil: time.Now().Add(2 * time.Minute),
		},
		{
			cluster:    "cluster4",
			leader:     "replica1",
			leaseStart: time.Now().Add(-5 * time.Minute),
			leaseUntil: time.Now().Add(2 * time.Minute),
		},
		{
			cluster:    "cluster5",
			leader:     "replica1",
			leaseStart: time.Now().Add(-5 * time.Minute),
			leaseUntil: time.Now().Add(2 * time.Minute),
		},
	}

	mockService := MockNewHAService(clusterInfo)

	// As Prometheus remote write sends sample timestamps
	// in milli-seconds converting the test samples to milliseconds
	sampleTimestamp := time.Now().UnixNano() / 1000000
	behindLeaseTimestamp := time.Now().Add(-10 * time.Minute).UnixNano() / 1000000
	aheadLeaseTimestamp := time.Now().Add(5 * time.Minute).UnixNano() / 1000000

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string][]model.SamplesInfo
		want1   int
		wantErr bool
		error   error
		cluster string
	}{
		{
			name:   "Test: HA enabled but __replica__ && __cluster__ are empty.",
			fields: fields{service: mockService},
			args: args{tts: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: sampleTimestamp, Value: 0.1},
					},
				},
			}},
			wantErr: true,
			error:   fmt.Errorf("ha mode is enabled and one/both of the __cluster__, __replica__ labels is/are empty"),
			cluster: "",
		},
		{
			name:   "Test: HA enabled but __replica__ is empty.",
			fields: fields{service: mockService},
			args: args{tts: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
						{Name: model.ClusterNameLabel, Value: "cluster1"},
					},
					Samples: []prompb.Sample{
						{Timestamp: sampleTimestamp, Value: 0.1},
					},
				},
			}},
			wantErr: true,
			error:   fmt.Errorf("ha mode is enabled and one/both of the __cluster__, __replica__ labels is/are empty"),
			cluster: "cluster1",
		},
		{
			name:   "Test: HA enabled but __cluster__ is empty.",
			fields: fields{service: mockService},
			args: args{tts: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
						{Name: model.ReplicaNameLabel, Value: "replica1"},
					},
					Samples: []prompb.Sample{
						{Timestamp: sampleTimestamp, Value: 0.1},
					},
				},
			}},
			wantErr: true,
			error:   fmt.Errorf("ha mode is enabled and one/both of the __cluster__, __replica__ labels is/are empty"),
		},
		{
			name:   "Test: HA enabled parse samples from leader prom instance.",
			fields: fields{service: mockService},
			args: args{tts: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
						{Name: model.ReplicaNameLabel, Value: "replica1"},
						{Name: model.ClusterNameLabel, Value: "cluster1"},
					},
					Samples: []prompb.Sample{
						{Timestamp: sampleTimestamp, Value: 0.1},
					},
				},
			}},
			wantErr: false,
			want: map[string][]model.SamplesInfo{
				"test": {
					{
						Labels: &model.Labels{
							Names:      []string{model.ClusterNameLabel, model.MetricNameLabelName, model.ReplicaNameLabel},
							Values:     []string{"cluster1", "test", "replica1"},
							MetricName: "test",
							Str:        "\v\u0000__cluster__\b\u0000cluster1\b\u0000__name__\u0004\u0000test\v\u0000__replica__\b\u0000replica1",
						},
						SeriesID: -1,
						Samples: []prompb.Sample{
							{
								Value:     0.1,
								Timestamp: sampleTimestamp,
							},
						},
					},
				},
			},
			want1:   1,
			cluster: "cluster1",
		},
		{
			name:   "Test: HA enabled parse samples from standby prom instance.",
			fields: fields{service: mockService},
			args: args{tts: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
						{Name: model.ReplicaNameLabel, Value: "replica2"},
						{Name: model.ClusterNameLabel, Value: "cluster2"},
					},
					Samples: []prompb.Sample{
						{Timestamp: sampleTimestamp, Value: 0.1},
					},
				},
			}},
			wantErr: false,
			want:    nil,
			want1:   0,
			cluster: "cluster2",
		},
		{
			name:   "Test: HA enabled parse samples from leader prom instance.",
			fields: fields{service: mockService},
			args: args{tts: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
						{Name: model.ReplicaNameLabel, Value: "replica1"},
						{Name: model.ClusterNameLabel, Value: "cluster3"},
					},
					Samples: []prompb.Sample{
						{Timestamp: sampleTimestamp, Value: 0.1},
					},
				},
			}},
			wantErr: false,
			want: map[string][]model.SamplesInfo{
				"test": {
					{
						Labels: &model.Labels{
							Names:      []string{model.ClusterNameLabel, model.MetricNameLabelName, model.ReplicaNameLabel},
							Values:     []string{"cluster3", "test", "replica1"},
							MetricName: "test",
							Str:        "\v\u0000__cluster__\b\u0000cluster3\b\u0000__name__\u0004\u0000test\v\u0000__replica__\b\u0000replica1",
						},
						SeriesID: -1,
						Samples: []prompb.Sample{
							{
								Value:     0.1,
								Timestamp: sampleTimestamp,
							},
						},
					},
				},
			},
			want1:   1,
			cluster: "cluster3",
		},
		{
			name:   "Test: HA enabled parse samples from leader prom instance & samples are out of lease interval 1.",
			fields: fields{service: mockService},
			args: args{tts: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
						{Name: model.ReplicaNameLabel, Value: "replica1"},
						{Name: model.ClusterNameLabel, Value: "cluster3"},
					},
					Samples: []prompb.Sample{
						{Timestamp: behindLeaseTimestamp, Value: 0.1},
					},
				},
			}},
			wantErr: false,
			want:    nil,
			want1:   0,
			cluster: "cluster3",
		},
		{
			name:   "Test: HA enabled parse samples from leader prom instance & samples are out of lease interval 2.",
			fields: fields{service: mockService},
			args: args{tts: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
						{Name: model.ReplicaNameLabel, Value: "replica1"},
						{Name: model.ClusterNameLabel, Value: "cluster3"},
					},
					Samples: []prompb.Sample{
						{Timestamp: aheadLeaseTimestamp, Value: 0.1},
					},
				},
			}},
			wantErr: false,
			want:    map[string][]model.SamplesInfo{
				"test": {
					{
						Labels: &model.Labels{
							Names:      []string{model.ClusterNameLabel, model.MetricNameLabelName, model.ReplicaNameLabel},
							Values:     []string{"cluster3", "test", "replica1"},
							MetricName: "test",
							Str:        "\v\u0000__cluster__\b\u0000cluster3\b\u0000__name__\u0004\u0000test\v\u0000__replica__\b\u0000replica1",
						},
						SeriesID: -1,
						Samples: []prompb.Sample{
							{
								Value:     0.1,
								Timestamp: aheadLeaseTimestamp,
							},
						},
					},
				},
			},
			want1:   1,
			cluster: "cluster3",
		},
		{
			name:   "Test: HA enabled parse samples from standby prom instance. readLockState returns the updated leader as standby prom instance but samples aren't part lease range.",
			fields: fields{service: mockService},
			args: args{tts: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
						{Name: model.ReplicaNameLabel, Value: "replica2"},
						{Name: model.ClusterNameLabel, Value: "cluster5"},
					},
					Samples: []prompb.Sample{
						{Timestamp: sampleTimestamp, Value: 0.1},
					},
				},
			}},
			wantErr: false,
			want:    nil,
			want1:   0,
			cluster: "cluster5",
		},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &haParser{
				service: tt.fields.service,
			}
			got, got1, err := h.ParseData(tt.args.tts)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseData() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("ParseData() got1 = %v, want %v", got1, tt.want1)
			}

			// If we are ingesting data means we are updating the maxTimeSeen & maxTimeSeenInstance details
			// Also make sure again that only samples from current leader are ingested.
			// validate whether the details are updated.
			//time.Sleep(2*time.Second)
			if got1 > 0 {
				for _, obj := range tt.args.tts[0].Labels {
					if obj.Name == "__replica__" {
						f := tt.want["test"]
						if f != nil {
							s, _ := h.service.state.Load(tt.cluster)
							state := s.(*State)
							stateView := state.clone()
							if obj.Value != stateView.leader || f[0].Samples[0].Timestamp != stateView.maxTimeSeen.UnixNano() / 1000000 {
								t.Errorf("max time seen isn't updated to latest samples info")
							}
						}
					}
				}
			}
		})
	}
}
