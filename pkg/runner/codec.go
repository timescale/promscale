// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license
package runner

import (
	gogoproto "github.com/gogo/protobuf/proto"
	"google.golang.org/grpc/encoding"
)

//This file undoes what https://github.com/jaegertracing/jaeger/blob/master/pkg/gogocodec/codec.go does
//in that it forces everything (both jaeger and not jaeger) to use the new protobuf lib instead of the old one
func init() {
	encoding.RegisterCodec(newCodec())
}

type gogoCodec struct {
}

var _ encoding.Codec = (*gogoCodec)(nil)

func newCodec() *gogoCodec {
	return &gogoCodec{}
}

// Name implements encoding.Codec
func (c *gogoCodec) Name() string {
	return "proto"
}

// Marshal implements encoding.Code
func (c *gogoCodec) Marshal(v interface{}) ([]byte, error) {
	return gogoproto.Marshal(v.(gogoproto.Message))
}

// Unmarshal implements encoding.Codec
func (c *gogoCodec) Unmarshal(data []byte, v interface{}) error {
	return gogoproto.Unmarshal(data, v.(gogoproto.Message))
}
