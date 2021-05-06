// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package parser

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/prompb"
)

type mockPreprocessor struct {
	sampleFilter func(int64) bool
	returnErr    error
}

func (m *mockPreprocessor) Process(_ *http.Request, wr *prompb.WriteRequest) error {
	if m.returnErr != nil {
		return m.returnErr
	}
	numTSAccepted := 0
	for i, ts := range wr.Timeseries {
		numAccepted := 0
		for _, sample := range ts.Samples {
			if m.sampleFilter(sample.Timestamp) {
				continue
			}

			wr.Timeseries[i].Samples[numAccepted] = sample
			numAccepted++
		}
		wr.Timeseries[i].Samples = wr.Timeseries[i].Samples[:numAccepted]

		if len(wr.Timeseries[i].Samples) == 0 {
			continue
		}
		wr.Timeseries[numTSAccepted] = wr.Timeseries[i]
		numTSAccepted++
	}
	wr.Timeseries = wr.Timeseries[:numTSAccepted]

	return nil
}

func TestParseRequest(t *testing.T) {
	testCases := []struct {
		name          string
		body          string
		contentType   string
		response      prompb.WriteRequest
		responseError string
		preprocessor  Preprocessor
	}{
		{
			name:          "unsupported content type",
			contentType:   "*",
			responseError: "parser error: unsupported format",
		},
		{
			name: "no data",
			body: ``,
		},
		{
			name:          "parser error",
			body:          `*`,
			responseError: "parser error: JSON decode error: invalid character '*' looking for beginning of value",
		},
		{
			name: "no preprocessors",
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
			name: "samples filtered by timestamp",
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
						},
					},
				},
			},
			preprocessor: &mockPreprocessor{sampleFilter: func(i int64) bool { return i > 1 }},
		},
		{
			name:         "samples filtered empty",
			body:         `{"labels":{"labelName":"labelValue"},"samples":[[1,2],[2,2],[3,2]]}`,
			preprocessor: &mockPreprocessor{sampleFilter: func(i int64) bool { return true }},
		},
		{
			name:          "preprocessor error",
			body:          `{"labels":{"labelName":"labelValue"},"samples":[[1,2],[2,2],[3,2]]}`,
			preprocessor:  &mockPreprocessor{returnErr: fmt.Errorf("some error")},
			responseError: "some error",
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			contentType := "application/json"
			if c.contentType != "" {
				contentType = c.contentType
			}
			req := &http.Request{
				Header: map[string][]string{
					"Content-Type": {contentType},
				},
				Body: ioutil.NopCloser(strings.NewReader(c.body)),
			}

			parser := NewParser()
			parser.AddPreprocessor(c.preprocessor)
			wr := ingestor.NewWriteRequest()

			err := parser.ParseRequest(req, wr)

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

			if wr.String() != c.response.String() {
				t.Fatalf("unexpected result:\ngot\n%v\nwanted\n%v\n", wr, c.response)
			}
		})
	}
}
