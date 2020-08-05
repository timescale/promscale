package end_to_end_tests

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/common/model"
)

type sample struct {
	Metric model.Metric       `json:"metric"`
	Values []model.SamplePair `json:"values"`
	Value  model.SamplePair   `json:"value"`
}

type samples []sample

func (s samples) Len() int           { return len(s) }
func (s samples) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s samples) Less(i, j int) bool { return s[i].Metric.Before(s[j].Metric) }

type resultComparator func(promContent []byte, tsContent []byte) error

func testRequest(req *http.Request, handler http.Handler, client *http.Client, comparator resultComparator) func(*testing.T) {
	return func(t *testing.T) {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		tsResp := rec.Result()
		promResp, promErr := client.Do(req)

		if promErr != nil {
			t.Fatalf("unexpected error returned from Prometheus client:\n%s\n", promErr.Error())
		}

		promContent, err := ioutil.ReadAll(promResp.Body)

		if err != nil {
			t.Fatalf("unexpected error returned when reading Prometheus response body:\n%s\n", err.Error())
		}
		defer promResp.Body.Close()

		tsContent, err := ioutil.ReadAll(tsResp.Body)

		if err != nil {
			t.Fatalf("unexpected error returned when reading connector response body:\n%s\n", err.Error())
		}
		defer tsResp.Body.Close()

		err = comparator(promContent, tsContent)
		if err != nil {
			t.Fatal(err)
		}
	}
}
