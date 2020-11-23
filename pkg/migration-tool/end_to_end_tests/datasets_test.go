package end_to_end_tests

import (
	"io/ioutil"
	"os"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

var wr prompb.WriteRequest

// generateRealTimeseries is used to read the real-world dataset from an
// external file. The dataset was generated using endpoints that are provided
// by the PromLabs PromQL Compliance Tester:
// https://github.com/promlabs/promql-compliance-tester/
// http://demo.promlabs.com:10000
// http://demo.promlabs.com:10001
// http://demo.promlabs.com:10002
func generateRealTimeseries() []prompb.TimeSeries {
	if len(wr.Timeseries) == 0 {
		f, err := os.Open("../../pgmodel/testdata/real-dataset.sz")
		if err != nil {
			panic(err)
		}

		compressed, err := ioutil.ReadAll(f)
		if err != nil {
			panic(err)
		}

		data, err := snappy.Decode(nil, compressed)
		if err != nil {
			panic(err)
		}

		err = wr.Unmarshal(data)

		if err != nil {
			panic(err)
		}
	}

	return wr.Timeseries
}
