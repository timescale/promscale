// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ha

import (
	"fmt"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"reflect"
	"testing"
	"time"

	"github.com/timescale/promscale/pkg/ha/client"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

type wanted struct {
	labels  []prompb.Label
	samples []prompb.Sample
}

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
	behindLeaseTimestamp := (leaseStart.UnixNano()/1000000)-1
	aheadLeaseTimestamp := leaseUntil.Add(time.Second).UnixNano() / 1000000
	clusterInfo := []*client.LeaseDBState{
		{
			Cluster:    "cluster1",
			Leader:     "replica1",
			LeaseStart: leaseStart,
			LeaseUntil: leaseUntil,
		},
		{
			Cluster:    "cluster2",
			Leader:     "replica1",
			LeaseStart: leaseStart,
			LeaseUntil: leaseUntil,
		},
		{
			Cluster:    "cluster3",
			Leader:     "replica1",
			LeaseStart: leaseStart,
			LeaseUntil: leaseUntil,
		},
		{
			Cluster:    "cluster4",
			Leader:     "replica2",
			LeaseStart: leaseStart,
			LeaseUntil: leaseUntil,
		},
		{
			Cluster:    "cluster5",
			Leader:     "replica2",
			LeaseStart: leaseStart,
			LeaseUntil: leaseUntil,
		},
	}

	mockService := MockNewHAService(clusterInfo)

	tests := []struct {
		name        string
		fields      fields
		args        args
		wanted      map[string][]wanted
		wantNumRows int
		wantErr     bool
		error       error
		cluster     string
	}{
		{
			name:   "Test: HA enabled but __replica__ && cluster are empty.",
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
			error:   fmt.Errorf("ha mode is enabled and one/both of the cluster, __replica__ labels is/are empty"),
			cluster: "",
		},
		{
			name:   "Test: HA enabled but __replica__ is empty.",
			fields: fields{service: mockService},
			args: args{tts: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
						{Name: ClusterNameLabel, Value: "cluster1"},
					},
					Samples: []prompb.Sample{
						{Timestamp: inLeaseTimestamp, Value: 0.1},
					},
				},
			}},
			wantErr: true,
			error:   fmt.Errorf("ha mode is enabled and one/both of the cluster, __replica__ labels is/are empty"),
			cluster: "cluster1",
		},
		{
			name:   "Test: HA enabled but cluster is empty.",
			fields: fields{service: mockService},
			args: args{tts: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabelName, Value: "test"},
						{Name: ReplicaNameLabel, Value: "replica1"},
					},
					Samples: []prompb.Sample{
						{Timestamp: inLeaseTimestamp, Value: 0.1},
					},
				},
			}},
			wantErr: true,
			error:   fmt.Errorf("ha mode is enabled and one/both of the cluster, __replica__ labels is/are empty"),
		},
		{
			name:   "Test: HA enabled parse samples from leader prom instance.",
			fields: fields{service: mockService},
			args: args{tts: []prompb.TimeSeries{
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
			}},
			wantErr: false,
			wanted: map[string][]wanted{
				"test": {{
					labels: []prompb.Label{
						{Name: ClusterNameLabel, Value: "cluster1"},
						{Name: model.MetricNameLabelName, Value: "test"},
					},
					samples: []prompb.Sample{{Value: 0.1, Timestamp: inLeaseTimestamp}},
				}}},
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
						{Name: ReplicaNameLabel, Value: "replica2"},
						{Name: ClusterNameLabel, Value: "cluster2"},
					},
					Samples: []prompb.Sample{
						{Timestamp: inLeaseTimestamp, Value: 0.1},
					},
				},
			}},
			wantErr:     false,
			wanted:      nil,
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
						{Name: ReplicaNameLabel, Value: "replica1"},
						{Name: ClusterNameLabel, Value: "cluster3"},
					},
					Samples: []prompb.Sample{
						{Timestamp: inLeaseTimestamp, Value: 0.1},
					},
				},
			}},
			wantErr: false,
			wanted: map[string][]wanted{
				"test": {
					{
						labels: []prompb.Label{{Name: ClusterNameLabel, Value: "cluster3"}, {Name: model.MetricNameLabelName, Value: "test"}},
						samples: []prompb.Sample{
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
			name:   "Test: HA enabled parse from leader & samples are in interval [leaseStart-X, leaseUntil)",
			fields: fields{service: mockService},
			args: args{tts: []prompb.TimeSeries{
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
			}},
			wantErr: false,
			wanted: map[string][]wanted{
				"test": {
					{
						labels: []prompb.Label{
							{Name: ClusterNameLabel, Value: "cluster3"},
							{Name: model.MetricNameLabelName, Value: "test"},
						},
						samples: []prompb.Sample{
							{
								Value:     0.2,
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
			name:   "Test: HA enabled parse from leader & samples are in interval [leaseStart, leaseUntil+X].",
			fields: fields{service: mockService},
			args: args{tts: []prompb.TimeSeries{
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
			}},
			wantErr: false,
			wanted: map[string][]wanted{
				"test": {
					{
						labels: []prompb.Label{
							{Name: ClusterNameLabel, Value: "cluster3"},
							{Name: model.MetricNameLabelName, Value: "test"},
						},
						samples: []prompb.Sample{
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
			name:   "Test: HA enabled, parse samples from standby instance. ReadLeaseState returns the updated leader as standby prom instance.",
			fields: fields{service: mockService},
			args: args{tts: []prompb.TimeSeries{
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
			}},
			wantErr: false,
			wanted: map[string][]wanted{
				"test": {
					{
						labels: []prompb.Label{
							{Name: ClusterNameLabel, Value: "cluster4"},
							{Name: model.MetricNameLabelName, Value: "test"},
						},
						samples: []prompb.Sample{
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
			name:   "Test: HA enabled parse from standby. ReadLeaseState returns the updated leader as standby prom instance but samples aren't part lease range.",
			fields: fields{service: mockService},
			args: args{tts: []prompb.TimeSeries{
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
			}},
			wantErr:     false,
			wanted:      map[string][]wanted{},
			wantNumRows: 0,
			cluster:     "cluster5",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &haParser{
				service: tt.fields.service,
				scache:  cache.NewSeriesCache(cache.DefaultSeriesCacheSize),
			}
			gotSamplesPerMetric, gotTotalRows, err := h.ParseData(tt.args.tts)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if errStr := compareSamples(tt.wanted, gotSamplesPerMetric, h.scache); errStr != "" {
				t.Fatal(errStr)
				return
			}

			if gotTotalRows != tt.wantNumRows {
				t.Errorf("ParseData() gotTotalRows = %v, wantSamples %v", gotTotalRows, tt.wantNumRows)
			}
		})
	}
}

func compareSamples(wantedResponse map[string][]wanted, receivedResponse map[string][]model.Samples, scache cache.SeriesCache) string {
	if wantedResponse == nil && receivedResponse != nil {
		return fmt.Sprintf("want nil samples, got: %v ", receivedResponse)
	} else if wantedResponse != nil && receivedResponse == nil {
		return fmt.Sprintf("got nil samples; want: %v", wantedResponse)
	} else if wantedResponse == nil {
		return ""
	}

	for metric, wantedSamplesInfos := range wantedResponse {
		receivedSamplesInfos, ok := receivedResponse[metric]
		if !ok {
			return fmt.Sprintf("wanted sample infos for metric [%s] weren't present", metric)
		}
		if len(wantedSamplesInfos) != len(receivedSamplesInfos) {
			return fmt.Sprintf("wanted [%d] sample infos for metric [%s]; got [%d]",
				len(wantedSamplesInfos), metric, len(receivedSamplesInfos),
			)
		}
		for i, wantedSamplesInfo := range wantedSamplesInfos {
			receivedSamplesInfo := receivedSamplesInfos[i]
			wantedSeries, _, _ := scache.GetSeriesFromProtos(wantedSamplesInfo.labels)
			if !wantedSeries.Equal(receivedSamplesInfo.GetSeries()) {
				return fmt.Sprintf("series for metric [%s] are not equal\nwant: %v\ngot: %v",
					metric, wantedSeries, receivedSamplesInfo.GetSeries(),
				)
			}

			wantedBatch := model.NewSamplesBatch()
			wantedSamplesInfoConverted := model.NewPromSample(wantedSeries, wantedSamplesInfo.samples)
			wantedBatch.Append(wantedSamplesInfoConverted)
			receivedBatch := model.NewSamplesBatch()
			receivedBatch.Append(receivedSamplesInfo)
			wantedBatch.GetSeriesSamples()
			for wantedBatch.Next() && receivedBatch.Next() {
				wantedTS, wantedVal, wantedSerId, wantedEpoch := wantedBatch.Values()
				receivedTS, receivedVal, receivedSerId, receivedEpoch := receivedBatch.Values()
				if wantedTS != receivedTS || wantedVal != receivedVal || wantedSerId != receivedSerId ||
					wantedEpoch != receivedEpoch {
					return fmt.Sprintf("samples missmatch for metric [%s]\n want: %v\n got: %v", metric, wantedSamplesInfo, receivedSamplesInfo)
				}
			}
		}
	}
	return ""
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
