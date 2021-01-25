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

	leaseStart := time.Unix(1, 0)
	leaseUntil := leaseStart.Add(2 * time.Second)
	// As Prometheus remote write sends sample timestamps
	// in milli-seconds converting the test samples to milliseconds
	inLeaseTimestamp := leaseStart.Add(time.Second).UnixNano() / 1000000
	behindLeaseTimestamp := leaseStart.Add(-time.Second).UnixNano() / 1000000
	aheadLeaseTimestamp := leaseUntil.Add(time.Second).UnixNano() / 1000000
	clusterInfo := []*haLockState{
		{
			cluster:    "cluster1",
			leader:     "replica1",
			leaseStart: leaseStart,
			leaseUntil: leaseUntil,
		},
		{
			cluster:    "cluster2",
			leader:     "replica1",
			leaseStart: leaseStart,
			leaseUntil: leaseUntil,
		},
		{
			cluster:    "cluster3",
			leader:     "replica1",
			leaseStart: leaseStart,
			leaseUntil: leaseUntil,
		},
		{
			cluster:    "cluster4",
			leader:     "replica2",
			leaseStart: leaseStart,
			leaseUntil: leaseUntil,
		},
		{
			cluster:    "cluster5",
			leader:     "replica2",
			leaseStart: leaseStart,
			leaseUntil: leaseUntil,
		},
	}

	mockService := MockNewHAService(clusterInfo)

	tests := []struct {
		name        string
		fields      fields
		args        args
		wantSamples map[string][]model.SamplesInfo
		wantNumRows int
		wantErr     bool
		error       error
		cluster     string
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
						{Timestamp: inLeaseTimestamp, Value: 0.1},
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
						{Timestamp: inLeaseTimestamp, Value: 0.1},
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
						{Timestamp: inLeaseTimestamp, Value: 0.1},
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
						{Timestamp: inLeaseTimestamp, Value: 0.1},
					},
				},
			}},
			wantErr: false,
			wantSamples: map[string][]model.SamplesInfo{
				"test": {
					{
						Labels: &model.Labels{
							Names:      []string{model.ClusterNameLabel, model.MetricNameLabelName},
							Values:     []string{"cluster1", "test"},
							MetricName: "test",
							Str:        "\v\u0000__cluster__\b\u0000cluster1\b\u0000__name__\u0004\u0000test",
						},
						SeriesID: -1,
						Samples: []prompb.Sample{
							{
								Value:     0.1,
								Timestamp: inLeaseTimestamp,
							},
						},
					},
				},
			},
			wantNumRows: 1,
			cluster:     "cluster1",
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
						{Timestamp: inLeaseTimestamp, Value: 0.1},
					},
				},
			}},
			wantErr:     false,
			wantSamples: nil,
			wantNumRows: 0,
			cluster:     "cluster2",
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
						{Timestamp: inLeaseTimestamp, Value: 0.1},
					},
				},
			}},
			wantErr: false,
			wantSamples: map[string][]model.SamplesInfo{
				"test": {
					{
						Labels: &model.Labels{
							Names:      []string{model.ClusterNameLabel, model.MetricNameLabelName},
							Values:     []string{"cluster3", "test"},
							MetricName: "test",
							Str:        "\v\u0000__cluster__\b\u0000cluster3\b\u0000__name__\u0004\u0000test",
						},
						SeriesID: -1,
						Samples: []prompb.Sample{
							{
								Value:     0.1,
								Timestamp: inLeaseTimestamp,
							},
						},
					},
				},
			},
			wantNumRows: 1,
			cluster:     "cluster3",
		},
		{
			name:   "Test: HA enabled parse from leader & samples are in interval [leaseStart-X, leaseUntil]",
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
			wantErr:     false,
			wantSamples: map[string][]model.SamplesInfo{},
			wantNumRows: 0,
			cluster:     "cluster3",
		},
		{
			name:   "Test: HA enabled parse from leader & samples are in interval [leaseStart, leaseUntil+X].",
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
			wantSamples: map[string][]model.SamplesInfo{
				"test": {
					{
						Labels: &model.Labels{
							Names:      []string{model.ClusterNameLabel, model.MetricNameLabelName},
							Values:     []string{"cluster3", "test"},
							MetricName: "test",
							Str:        "\v\u0000__cluster__\b\u0000cluster3\b\u0000__name__\u0004\u0000test",
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
			wantNumRows: 1,
			cluster:     "cluster3",
		},
		{
			name:   "Test: HA enabled, parse samples from standby instance. readLockState returns the updated leader as standby prom instance.",
			fields: fields{service: mockService},
			args: args{tts: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
						{Name: model.ReplicaNameLabel, Value: "replica2"},
						{Name: model.ClusterNameLabel, Value: "cluster4"},
					},
					Samples: []prompb.Sample{
						{Timestamp: inLeaseTimestamp, Value: 0.1},
					},
				},
			}},
			wantErr: false,
			wantSamples: map[string][]model.SamplesInfo{
				"test": {
					{
						Labels: &model.Labels{
							Names:      []string{model.ClusterNameLabel, model.MetricNameLabelName},
							Values:     []string{"cluster4", "test"},
							MetricName: "test",
							Str:        "\v\u0000__cluster__\b\u0000cluster4\b\u0000__name__\u0004\u0000test",
						},
						SeriesID: -1,
						Samples: []prompb.Sample{
							{
								Value:     0.1,
								Timestamp: inLeaseTimestamp,
							},
						},
					},
				},
			},
			wantNumRows: 1,
			cluster:     "cluster4",
		},
		{
			name:   "Test: HA enabled parse from standby. readLockState returns the updated leader as standby prom instance but samples aren't part lease range.",
			fields: fields{service: mockService},
			args: args{tts: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
						{Name: model.ReplicaNameLabel, Value: "replica2"},
						{Name: model.ClusterNameLabel, Value: "cluster5"},
					},
					Samples: []prompb.Sample{
						{Timestamp: behindLeaseTimestamp, Value: 0.1},
					},
				},
			}},
			wantErr:     false,
			wantSamples: map[string][]model.SamplesInfo{},
			wantNumRows: 0,
			cluster:     "cluster5",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &haParser{
				service: tt.fields.service,
			}
			gotSamplesPerMetric, gotTotalRows, err := h.ParseData(tt.args.tts)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotSamplesPerMetric, tt.wantSamples) {
				t.Errorf("ParseData() gotSamplesPerMetric = %v, wantSamples %v", gotSamplesPerMetric, tt.wantSamples)
			}
			if gotTotalRows != tt.wantNumRows {
				t.Errorf("ParseData() gotTotalRows = %v, wantSamples %v", gotTotalRows, tt.wantNumRows)
			}

			// If we are ingesting data means we are updating the maxTimeSeen & maxTimeSeenInstance details
			// Also make sure again that only samples from current leader are ingested.
			// validate whether the details are updated.
			//time.Sleep(2*time.Second)
			if gotTotalRows > 0 {
				for _, obj := range tt.args.tts[0].Labels {
					if obj.Name == "__replica__" {
						f := tt.wantSamples["test"]
						if f != nil {
							s, _ := h.service.state.Load(tt.cluster)
							state := s.(*State)
							stateView := state.clone()
							if obj.Value != stateView.leader || f[0].Samples[0].Timestamp != stateView.maxTimeSeen.UnixNano()/1000000 {
								t.Errorf("max time seen isn't updated to latest samples info")
							}
						}
					}
				}
			}
		})
	}
}
