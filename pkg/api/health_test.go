package api

import (
	"fmt"
	"github.com/timescale/timescale-prometheus/pkg/log"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

var (
	healthOKHeaderMap = http.Header{
		"Content-Length": []string{"0"},
	}
)

type mockHealthChecker struct {
	returnErr error
}

func (m *mockHealthChecker) HealthCheck() error {
	return m.returnErr
}

func TestHealth(t *testing.T) {
	log.Init("debug")
	testCases := []struct {
		name                   string
		httpStatus             int
		healthCheckerReturnErr error
	}{
		{
			name:       "no error",
			httpStatus: http.StatusOK,
		},
		{
			name:                   "error",
			httpStatus:             http.StatusInternalServerError,
			healthCheckerReturnErr: fmt.Errorf("some error"),
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			mock := &mockHealthChecker{
				returnErr: c.healthCheckerReturnErr,
			}

			healthHandle := Health(mock)

			test := GenerateHandleTester(t, healthHandle)
			w := test("GET", strings.NewReader(""))

			if w.Code != c.httpStatus {
				t.Errorf("Health page didn't return correct status: got %v wanted %v", w.Code, c.httpStatus)
			}

			header := w.Header()

			if c.httpStatus == http.StatusOK {
				if !reflect.DeepEqual(header, healthOKHeaderMap) {
					t.Errorf("Did not get correct headers for http.StatusOK:\ngot\n%#v\nwanted\n%#v\n", header, healthOKHeaderMap)
				}
			} else {
				if strings.TrimSpace(w.Body.String()) != c.healthCheckerReturnErr.Error() {
					t.Errorf("Unexpected body content:\ngot\n%s\nwanted\n%s", w.Body.String(), c.healthCheckerReturnErr.Error())
				}
			}

		})
	}
}
