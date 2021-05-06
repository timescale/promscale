// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package json

import (
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/prompb"
)

func TestParseRequest(t *testing.T) {

	testCases := []struct {
		name          string
		body          string
		response      prompb.WriteRequest
		responseError string
	}{
		{
			name: "happy path",
			body: `{"labels":{"labelName":"labelValue"},"samples":[[1,2],[2,2],[3,2]]}`,
			response: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{{Name: "labelName", Value: "labelValue"}},
						Samples: []prompb.Sample{
							{
								Timestamp: 1,
								Value:     2,
							},
							{
								Timestamp: 2,
								Value:     2,
							},
							{
								Timestamp: 3,
								Value:     2,
							},
						},
					},
				},
			},
		},
		{
			name: "multiple JSON objects",
			body: `{"labels":{"labelName":"labelValue"},"samples":[[1,2],[2,2],[3,2]]}
{"labels":{"labelName":"otherLabelValue"},"samples":[[1,1],[2,1],[3,1]]}`,
			response: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{{Name: "labelName", Value: "labelValue"}},
						Samples: []prompb.Sample{
							{
								Timestamp: 1,
								Value:     2,
							},
							{
								Timestamp: 2,
								Value:     2,
							},
							{
								Timestamp: 3,
								Value:     2,
							},
						},
					},
					{
						Labels: []prompb.Label{{Name: "labelName", Value: "otherLabelValue"}},
						Samples: []prompb.Sample{
							{
								Timestamp: 1,
								Value:     1,
							},
							{
								Timestamp: 2,
								Value:     1,
							},
							{
								Timestamp: 3,
								Value:     1,
							},
						},
					},
				},
			},
		},
		{
			name:          "invalid json",
			body:          `invalid`,
			responseError: "JSON decode error: invalid character 'i' looking for beginning of value",
		},
		{
			name:          "invalid samples JSON",
			body:          `{"labels":{"labelName":"labelValue"},"samples":"invalid"}`,
			responseError: "JSON decode error: cannot unmarshal string into array of (timestamp,value) arrays",
		},
		{
			name:          "invalid samples JSON, too many array members",
			body:          `{"labels":{"labelName":"labelValue"},"samples":[[1,2,3]]}`,
			responseError: "JSON decode error: invalid sample, should contain only timestamp and value: [1 2 3]",
		},
		{
			name:          "invalid samples JSON, too little array members",
			body:          `{"labels":{"labelName":"labelValue"},"samples":[[1]]}`,
			responseError: "JSON decode error: invalid sample, should contain only timestamp and value: [1]",
		},
		{
			name:          "invalid samples JSON, float timestamp not allowed",
			body:          `{"labels":{"labelName":"labelValue"},"samples":[[1.1,1]]}`,
			responseError: "JSON decode error: timestamp cannot contain decimal parts in sample: [1.1 1]",
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			req := &http.Request{
				Body: ioutil.NopCloser(strings.NewReader(c.body)),
			}
			response := ingestor.NewWriteRequest()
			defer ingestor.FinishWriteRequest(response)

			err := ParseRequest(req, response)

			if c.responseError != "" {
				errVal := "nil"
				if err != nil {
					errVal = err.Error()
				}
				if errVal != c.responseError {
					t.Fatalf("unexpected error occured:\ngot\n%s\nwanted\n%s\n", errVal, c.responseError)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error occured, none expected: %s", err)
			}

			if !reflect.DeepEqual(*response, c.response) {
				t.Fatalf("unexpected result:\ngot\n%+v\nwanted\n%+v\n", *response, c.response)
			}
		})
	}

}
