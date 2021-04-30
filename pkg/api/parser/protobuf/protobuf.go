package protobuf

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/gogo/protobuf/proto"
	"github.com/timescale/promscale/pkg/prompb"
)

// ParseRequest is responsible for populating the write request from the
// data in the request in protobuf format.
func ParseRequest(r *http.Request, wr *prompb.WriteRequest) error {
	reqBuf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("request body read error: %w", err)
	}

	if err = proto.Unmarshal(reqBuf, wr); err != nil {
		return fmt.Errorf("protobuf unmarshal error: %w", err)
	}

	return nil
}
