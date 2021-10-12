package json

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"

	"github.com/timescale/promscale/pkg/prompb"
)

type jsonPayload struct {
	Labels  map[string]string `json:"labels"`
	Samples jsonSamples       `json:"samples"`
}

type jsonSamples []prompb.Sample

func (i *jsonSamples) UnmarshalJSON(data []byte) error {
	var vv [][]float64

	if err := json.Unmarshal(data, &vv); err != nil {
		return fmt.Errorf("cannot unmarshal string into array of (timestamp,value) arrays")
	}

	*i = make([]prompb.Sample, 0, len(vv))
	for _, tv := range vv {
		if len(tv) != 2 {
			return fmt.Errorf("invalid sample, should contain only timestamp and value: %v", tv)
		}

		t := int64(tv[0])

		// Verify that timestamp doesn't contain fractions so we don't silently
		// drop precision.
		if float64(t) != tv[0] {
			return fmt.Errorf("timestamp cannot contain decimal parts in sample: %v", tv)
		}

		*i = append(*i, prompb.Sample{Timestamp: t, Value: tv[1]})
	}

	return nil
}

type byLabelName []prompb.Label

func (ls byLabelName) Len() int           { return len(ls) }
func (ls byLabelName) Swap(i, j int)      { ls[i], ls[j] = ls[j], ls[i] }
func (ls byLabelName) Less(i, j int) bool { return ls[i].Name < ls[j].Name }

func generateProtoLabels(ll map[string]string) []prompb.Label {
	result := make([]prompb.Label, 0, len(ll))
	for name, value := range ll {
		result = append(result, prompb.Label{Name: name, Value: value})
	}

	sort.Sort(byLabelName(result))
	return result
}

// ParseRequest parses an incoming HTTP request as a JSON payload.
func ParseRequest(r *http.Request, wr *prompb.WriteRequest) error {
	dec := json.NewDecoder(r.Body)
	for {
		// Create a new map, otherwise previous alloc of map is re-used, leading to
		// label pairs that do not belong to this iteration, also being re-used.
		var i jsonPayload
		if err := dec.Decode(&i); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("JSON decode error: %w", err)
		}

		wr.Timeseries = append(wr.Timeseries, prompb.TimeSeries{
			Labels:  generateProtoLabels(i.Labels),
			Samples: i.Samples,
		})
	}

	return nil
}
