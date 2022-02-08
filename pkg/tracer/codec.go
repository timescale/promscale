package tracer

import (
	"google.golang.org/grpc/encoding"
	"google.golang.org/protobuf/proto"
)

// clientCodec is used to force the gRPC client into the
// specific codec that works with OTEL exporter.
type clientCodec struct{}

var _ encoding.Codec = (*clientCodec)(nil)

func newClientCodec() *clientCodec {
	return &clientCodec{}
}

// Name implements encoding.Codec
func (c *clientCodec) Name() string {
	return "proto"
}

// Marshal implements encoding.Codec
func (c *clientCodec) Marshal(v interface{}) ([]byte, error) {
	return proto.Marshal(v.(proto.Message))
}

// Unmarshal implements encoding.Codec
func (c *clientCodec) Unmarshal(data []byte, v interface{}) error {
	return proto.Unmarshal(data, v.(proto.Message))
}
