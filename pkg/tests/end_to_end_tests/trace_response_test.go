// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"encoding/gob"
	"fmt"
	"os"

	"github.com/golang/snappy"
	jaegerJSONModel "github.com/jaegertracing/jaeger/model/json"
)

type traceResponse struct {
	Services   []string
	Operations []string
	Trace      jaegerJSONModel.Trace
	Traces     []jaegerJSONModel.Trace
}

type traceResponsesStore struct {
	Responses []traceResponse
}

const jaegerQueryResponsesPath = "../testdata/jaeger_query_responses.sz"

//nolint:all
func storeJaegerQueryResponses(responses *traceResponsesStore) error {
	// #nosec
	f, err := os.OpenFile(jaegerQueryResponsesPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0777)
	if err != nil {
		return fmt.Errorf("opening responses file: %w", err)
	}
	defer f.Close()

	if err = f.Truncate(0); err != nil {
		return fmt.Errorf("truncating opened responses file: %w", err)
	}

	snappyWriter := snappy.NewBufferedWriter(f)

	enc := gob.NewEncoder(snappyWriter)
	if err = enc.Encode(responses); err != nil {
		return fmt.Errorf("encoding responses: %w", err)
	}

	if err = snappyWriter.Flush(); err != nil {
		return fmt.Errorf("flusing snappy-writer: %w", err)
	}
	if err = snappyWriter.Close(); err != nil {
		return fmt.Errorf("closing snappy-writer: %w", err)
	}

	return nil
}

func loadJaegerQueryResponses() (*traceResponsesStore, error) {
	f, err := os.Open(jaegerQueryResponsesPath)
	if err != nil {
		return nil, fmt.Errorf("opening jaeger query response: %w", err)
	}
	var store traceResponsesStore
	reader := gob.NewDecoder(snappy.NewReader(f))
	if err = reader.Decode(&store); err != nil {
		return nil, fmt.Errorf("decoding gob: %w", err)
	}
	tweakResults(&store)
	return &store, nil
}

// tweakResults modifies trivial stuff that are lost in serialization & deserialization of data.
func tweakResults(s *traceResponsesStore) {
	for i := range s.Responses {
		makeSliceEmptyIfNil(&s.Responses[i].Trace)
		for j := range s.Responses[i].Traces {
			makeSliceEmptyIfNil(&s.Responses[i].Traces[j])
		}
	}
}

func makeSliceEmptyIfNil(s *jaegerJSONModel.Trace) {
	for i := range s.Spans {
		if s.Spans[i].References == nil {
			s.Spans[i].References = []jaegerJSONModel.Reference{}
		}
		if s.Spans[i].Logs == nil {
			s.Spans[i].Logs = []jaegerJSONModel.Log{}
		}
	}
}
