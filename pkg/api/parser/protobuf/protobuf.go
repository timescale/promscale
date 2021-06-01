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
	b.Reset()

	_, err := b.ReadFrom(r.Body)
	if err != nil {
		return fmt.Errorf("request body read error: %w", err)
	}

	if err = proto.Unmarshal(b.Bytes(), wr); err != nil {
		return fmt.Errorf("protobuf unmarshal error: %w", err)
	}

	r.Body.Close()
	bufPool.Put(b)
	return nil
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}
