package store

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/jaegertracing/jaeger/model"
)

const MEDIA_TYPE_ENCODED_BINARY = "data:application/octet-stream; base64,"
const MEDIA_TYPE_ENCODED_BINARY_LEN = len(MEDIA_TYPE_ENCODED_BINARY)

// decodeSpanBinaryTags decodes the tags with binary values that are present in
// the binaryTags sets for the span, process and logs.
//
// When writing binary tags we encode the slice of bytes into a base64 string
// representation and add the prefix `__ValueType_BINARY__`. Decoding implies
// removing the prefix and decoding the base64 string.
func decodeSpanBinaryTags(span *model.Span) {
	decodeBinaryTags(span.Tags)
	decodeBinaryTags(span.Process.Tags)
	for _, log := range span.Logs {
		decodeBinaryTags(log.Fields)
	}
}

func decodeBinaryTags(tags []model.KeyValue) {
	for i, tag := range tags {
		if tag.GetVType() != model.ValueType_STRING {
			continue
		}

		encoded := tag.VStr
		if !strings.HasPrefix(encoded, MEDIA_TYPE_ENCODED_BINARY) {
			continue
		}

		vBin, err := decodeBinaryTagValue(encoded)

		// If we can't decode it means that we didn't encode it in the
		// first place, so we should keep it as is.
		if err != nil {
			continue
		}
		tags[i] = model.KeyValue{
			Key:     tag.Key,
			VType:   model.ValueType_BINARY,
			VBinary: vBin,
		}
	}
}

func decodeBinaryTagValue(encoded string) ([]byte, error) {
	v := encoded[MEDIA_TYPE_ENCODED_BINARY_LEN:]
	return base64.StdEncoding.DecodeString(v)
}

func encodeBinaryTagToStr(tag model.KeyValue) model.KeyValue {
	value := fmt.Sprintf("%s%s", MEDIA_TYPE_ENCODED_BINARY, base64.StdEncoding.EncodeToString(tag.GetVBinary()))
	return model.KeyValue{
		Key:   tag.Key,
		VType: model.ValueType_STRING,
		VStr:  value,
	}
}

func encodeBinaryTags(span *model.Span) {
	for i, tag := range span.Tags {
		if !isBinaryTag(tag) {
			continue
		}
		span.Tags[i] = encodeBinaryTagToStr(tag)
	}

	for _, log := range span.Logs {
		for i, tag := range log.Fields {
			if !isBinaryTag(tag) {
				continue
			}
			log.Fields[i] = encodeBinaryTagToStr(tag)
		}
	}

	for i, tag := range span.Process.Tags {
		if !isBinaryTag(tag) {
			continue
		}
		span.Process.Tags[i] = encodeBinaryTagToStr(tag)
	}
}

func isBinaryTag(tag model.KeyValue) bool {
	return tag.GetVType() == model.ValueType_BINARY
}
