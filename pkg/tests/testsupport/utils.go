// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package testsupport

import (
	"net/http"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/timescale/promscale/pkg/prompb"
)

func GetHTTPWriteRequest(url string, protoRequest *prompb.WriteRequest) (*http.Request, error) {
	data, err := proto.Marshal(protoRequest)
	if err != nil {
		return nil, err
	}

	body := string(snappy.Encode(nil, data))

	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(
		"POST",
		url,
		strings.NewReader(body),
	)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Encoding", "snappy")
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	return req, nil
}
