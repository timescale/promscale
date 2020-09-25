package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/query"
)

func TestLabels(t *testing.T) {
	_ = log.Init(log.Config{
		Level: "debug",
	})
	testCases := []struct {
		name        string
		querier     *mockQuerier
		expectCode  int
		expectError string
	}{
		{
			name:        "Error on get label names",
			expectCode:  http.StatusInternalServerError,
			expectError: "internal",
			querier:     &mockQuerier{labelNamesErr: fmt.Errorf("error on label names")},
		}, {
			name:       "All good",
			expectCode: http.StatusOK,
			querier:    &mockQuerier{labelNames: []string{"a"}},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			handler := labelsHandler(query.NewQueryable(tc.querier))
			w := doLabels(t, handler)

			if w.Code != tc.expectCode {
				t.Errorf("Unexpected HTTP status code received: got %d wanted %d", w.Code, tc.expectCode)
				return
			}
			if tc.expectError != "" {
				var er errResponse
				_ = json.NewDecoder(bytes.NewReader(w.Body.Bytes())).Decode(&er)
				if tc.expectError != er.ErrorType {
					t.Errorf("expected error of type %s, got %s", tc.expectError, er.ErrorType)
				}
				return
			}
			var res response
			_ = json.NewDecoder(bytes.NewReader(w.Body.Bytes())).Decode(&res)
			if len(res.Warnings) > 0 {
				t.Errorf("unexpected warnings: %v", res.Warnings)
			}
			var resStr []string
			for _, s := range res.Data.([]interface{}) {
				resStr = append(resStr, s.(string))
			}
			if !reflect.DeepEqual(resStr, tc.querier.labelNames) {
				t.Errorf("expected: %v, got: %v", tc.querier.labelNames, res.Data)
			}
		})

	}

}

func doLabels(t *testing.T, queryHandler http.Handler) *httptest.ResponseRecorder {
	req, err := http.NewRequestWithContext(context.Background(), "GET", "http://localhost:9090/labels", nil)
	if err != nil {
		t.Errorf("%v", err)
	}
	req.Header.Set(
		"Content-Type",
		"application/x-www-form-urlencoded; param=value",
	)
	w := httptest.NewRecorder()

	queryHandler.ServeHTTP(w, req)
	return w
}
