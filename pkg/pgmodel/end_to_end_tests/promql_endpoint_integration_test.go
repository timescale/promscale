package end_to_end_tests

import (
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/prometheus/common/model"
)

type sample struct {
	Metric model.Metric       `json:"metric"`
	Values []model.SamplePair `json:"values"`
	Value  model.SamplePair   `json:"value"`
}

type requestCase struct {
	req *http.Request
	log string
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

func testRequestConcurrent(requestCases []requestCase, handler http.Handler, client *http.Client, comparator resultComparator) func(*testing.T) {
	return func(t *testing.T) {

		perm := rand.Perm(len(requestCases))

		wg := sync.WaitGroup{}

		for i := 0; i < len(perm); i++ {
			req := requestCases[perm[i]].req
			log := requestCases[perm[i]].log
			wg.Add(1)
			go func() {
				defer wg.Done()

				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				tsResp := rec.Result()
				promResp, promErr := client.Do(req)

				if promErr != nil {
					t.Errorf("unexpected error returned from Prometheus client:\n%s\n", promErr.Error())
					return
				}

				promContent, err := ioutil.ReadAll(promResp.Body)

				if err != nil {
					t.Errorf("unexpected error returned when reading Prometheus response body:\n%s\n", err.Error())
					return
				}
				defer promResp.Body.Close()

				tsContent, err := ioutil.ReadAll(tsResp.Body)

				if err != nil {
					t.Errorf("unexpected error returned when reading connector response body:\n%s\n", err.Error())
					return
				}
				defer tsResp.Body.Close()

				err = comparator(promContent, tsContent)
				if err != nil {
					t.Errorf("%s gives %s", log, err)
					return
				}
			}()
		}
		wg.Wait()
	}
}
