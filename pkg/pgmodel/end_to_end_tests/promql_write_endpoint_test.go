package end_to_end_tests

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/timescale/timescale-prometheus/pkg/api"
	"github.com/timescale/timescale-prometheus/pkg/internal/testhelpers"
	"github.com/timescale/timescale-prometheus/pkg/pgmodel"
	"github.com/timescale/timescale-prometheus/pkg/prompb"
	"github.com/timescale/timescale-prometheus/pkg/util"
)

type dataGenerator struct {
	metricGroup int //nonoverlapping metrics
	queueID     int

	metricsPerGroup    int
	labelSetsPerMetric int
	maxSamples         int64
	deltaTime          int64
	deltaValue         float64
	currentTime        int64
}

func (t *dataGenerator) generateTimeseries() []prompb.TimeSeries {
	metrics := []prompb.TimeSeries{}
	for metric := 0; metric < t.metricsPerGroup; metric++ {
		for instance := 0; instance < t.labelSetsPerMetric; instance++ {
			labelSet := []prompb.Label{
				{Name: pgmodel.MetricNameLabelName, Value: fmt.Sprintf("metric_%d_%d", t.metricGroup, metric)},
				{Name: "foo", Value: fmt.Sprintf("bar_%d", t.queueID)}, //queues have non-overlapping label sets
				{Name: "instance", Value: fmt.Sprintf("%d", instance)},
			}

			if instance%2 == 0 {
				labelSet = append(labelSet,
					prompb.Label{Name: "sometimes_label", Value: "constant"})
			}

			metrics = append(metrics, prompb.TimeSeries{
				Labels: labelSet,
			})
		}

	}

	numSamples := rand.Int63n(t.maxSamples)
	for i := range metrics {
		metrics[i].Samples = t.generateSamples(numSamples)
	}
	t.currentTime += t.maxSamples*t.deltaTime + int64(1)
	return metrics
}

func (t *dataGenerator) generateSamples(count int64) []prompb.Sample {
	samples := make([]prompb.Sample, 0, 3)
	i := int64(0)
	for i < count {
		samples = append(samples, prompb.Sample{
			Timestamp: t.currentTime + (t.deltaTime * int64(i)),
			Value:     float64(t.metricGroup*10000) + (t.deltaValue * float64(i)),
		})
		i++
	}

	return samples
}

func getHTTPWriteRequest(protoRequest *prompb.WriteRequest) (*http.Request, error) {
	data, err := proto.Marshal(protoRequest)
	if err != nil {
		return nil, err
	}

	body := string(snappy.Encode(nil, data))
	u, err := url.Parse(fmt.Sprintf("http://%s:%d/write", testhelpers.PromHost, testhelpers.PromPort.Int()))

	if err != nil {
		return nil, err
	}
	return http.NewRequest(
		"POST",
		u.String(),
		strings.NewReader(body),
	)
}

func sendWriteRequest(t testing.TB, router *route.Router, ts []prompb.TimeSeries) {
	req, err := getHTTPWriteRequest(&prompb.WriteRequest{Timeseries: ts})
	if err != nil {
		t.Fatalf("unable to create PromQL label names request: %v", err)
	}

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	tsResp := rec.Result()
	if rec.Code != 200 {
		t.Fatal(rec.Code)
	}

	_, err = ioutil.ReadAll(tsResp.Body)
	if err != nil {
		t.Fatalf("unexpected error returned when reading connector response body:\n%s\n", err.Error())
	}
	defer tsResp.Body.Close()
}

func verifyTimeseries(t testing.TB, db *pgxpool.Pool, tsSlice []prompb.TimeSeries) {
	for tsIdx := range tsSlice {
		ts := tsSlice[tsIdx]
		name := ""
		names := []string{}
		values := []string{}
		for labelIdx := range ts.Labels {
			label := ts.Labels[labelIdx]
			if label.Name == pgmodel.MetricNameLabelName {
				name = label.Value

			}
			names = append(names, label.Name)
			values = append(values, label.Value)
		}
		if name == "" {
			t.Fatal("No ts series metric name found")
		}
		for sampleIdx := range ts.Samples {
			sample := ts.Samples[sampleIdx]
			rows, err := db.Query(context.Background(), fmt.Sprintf("SELECT value FROM prom_data.%s WHERE time = $1 and series_id = (SELECT series_id FROM _prom_catalog.get_or_create_series_id_for_kv_array($2, $3, $4))",
				name), model.Time(sample.Timestamp).Time(), name, names, values)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			count := 0
			for rows.Next() {
				var val *float64
				err := rows.Scan(&val)
				if err != nil {
					t.Fatal(err)
				}
				if val == nil {
					t.Fatal("NULL value")
				}
				if *val != sample.Value {
					t.Errorf("Unexpected value: got %v, unexpected %v", *val, sample.Value)
				}
				count++
			}
			if count != 1 {
				t.Errorf("Unexpected count: %d", count)
			}
		}
	}
}

func getWriteRouter(t testing.TB, db *pgxpool.Pool) *route.Router {
	r, err := pgmodel.NewPgxIngestor(db)
	if err != nil {
		t.Fatal(err)
	}

	writeHandler := api.Write(r, nil, &api.Metrics{
		LeaderGauge:       prometheus.NewGauge(prometheus.GaugeOpts{}),
		ReceivedSamples:   prometheus.NewCounter(prometheus.CounterOpts{}),
		FailedSamples:     prometheus.NewCounter(prometheus.CounterOpts{}),
		SentSamples:       prometheus.NewCounter(prometheus.CounterOpts{}),
		SentBatchDuration: prometheus.NewHistogram(prometheus.HistogramOpts{}),
		WriteThroughput:   util.NewThroughputCalc(time.Second),
	})

	router := route.New()
	router.Post("/write", writeHandler.ServeHTTP)
	return router
}

func sendConcurrentWrites(t testing.TB, db *pgxpool.Pool, queues int, metricGroups int, totalRequests int, duplicates bool) {
	router := getWriteRouter(t, db)

	wg := sync.WaitGroup{}
	for i := 0; i < queues; i++ {
		for m := 0; m < metricGroups; m++ {
			queueIdx := i
			midx := m
			wg.Add(1)
			go func() {
				defer wg.Done()
				dg := dataGenerator{
					metricGroup: midx,
					queueID:     queueIdx,

					metricsPerGroup:    2,
					labelSetsPerMetric: 2,
					maxSamples:         5,
					deltaTime:          2000,
					deltaValue:         3,
					currentTime:        startTime,
				}
				tss := [][]prompb.TimeSeries{}
				for requestNo := 0; requestNo < totalRequests; requestNo++ {
					ts := dg.generateTimeseries()
					sendWriteRequest(t, router, ts)
					if duplicates {
						sendWriteRequest(t, router, ts)
					}
					tss = append(tss, ts)
				}
				for i := range tss {
					verifyTimeseries(t, db, tss[i])
				}
			}()
		}
	}
	wg.Wait()
}

func TestWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		sendConcurrentWrites(t, db, 2, 2, 5, false)
		sendConcurrentWrites(t, db, 2, 2, 3, true)
	})
}
