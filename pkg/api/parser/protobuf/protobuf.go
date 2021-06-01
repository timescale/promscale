package protobuf

import (
	"bytes"
	"fmt"
	"net/http"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/timescale/promscale/pkg/prompb"
)

// ParseRequest is responsible for populating the write request from the
// data in the request in protobuf format.
func ParseRequest(r *http.Request, wr *prompb.WriteRequest) error {
	b := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(b)
	b.Reset()

	_, err := b.ReadFrom(r.Body)
	if err != nil {
		return fmt.Errorf("request body read error: %w", err)
	}

	if err = proto.Unmarshal(b.Bytes(), wr); err != nil {
		return fmt.Errorf("protobuf unmarshal error: %w", err)
	}

	return r.Body.Close()
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}
