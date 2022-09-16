package store

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/jaegertracing/jaeger/model"
	"github.com/stretchr/testify/assert"
)

var binaryValue1 = []byte{66, 105, 110, 97, 114, 121}
var binaryValue2 = []byte{66, 105, 110, 97, 114, 121}

func keyValuesFixture(prefix string) []model.KeyValue {
	return []model.KeyValue{
		{
			Key:     fmt.Sprintf("%s-binary1-data", prefix),
			VBinary: binaryValue1,
			VType:   model.ValueType_BINARY,
		},
		{
			Key:   fmt.Sprintf("%s-string-data", prefix),
			VStr:  "My string",
			VType: model.ValueType_STRING,
		},
		{
			Key:    fmt.Sprintf("%s-int64-data", prefix),
			VInt64: 42,
			VType:  model.ValueType_INT64,
		},
		{
			Key:     fmt.Sprintf("%s-binary2-data", prefix),
			VBinary: binaryValue2,
			VType:   model.ValueType_BINARY,
		},
		{
			Key:      fmt.Sprintf("%s-float64-data", prefix),
			VFloat64: 42.42,
			VType:    model.ValueType_FLOAT64,
		},
		{
			Key:   fmt.Sprintf("%s-bool-data", prefix),
			VBool: true,
			VType: model.ValueType_BOOL,
		},
	}
}

func getExpectedStrV(key string, binaryValue []byte) model.KeyValue {
	return model.KeyValue{
		Key:   key,
		VStr:  fmt.Sprintf("%s%s", MEDIA_TYPE_ENCODED_BINARY, base64.StdEncoding.EncodeToString(binaryValue)),
		VType: model.ValueType_STRING,
	}
}

func assertEncoded(t *testing.T, prefix string, encodedTags []model.KeyValue) {
	// Binary values are at position 0 and 3
	key1 := fmt.Sprintf("%s-binary1-data", prefix)
	assert.Equal(t, getExpectedStrV(key1, binaryValue1), encodedTags[0])

	key2 := fmt.Sprintf("%s-binary2-data", prefix)
	assert.Equal(t, getExpectedStrV(key2, binaryValue2), encodedTags[3])
}

func assertOnlyBinariesModified(t *testing.T, binaryVIdx map[int]struct{}, expected []model.KeyValue, actual []model.KeyValue) {
	assert.Equal(t, len(expected), len(actual))
	for i, tag := range actual {
		_, isBinary := binaryVIdx[i]
		if isBinary {
			assert.NotEqual(t, expected[i], tag)
		} else {
			assert.Equal(t, expected[i], tag)
		}
	}

}

func TestEncodeBinaryTag(t *testing.T) {
	logs := []model.Log{
		{
			Fields: keyValuesFixture("log1"),
		},
		{
			Fields: keyValuesFixture("log2"),
		},
	}
	process := model.Process{
		Tags: keyValuesFixture("process"),
	}
	span := model.Span{
		Tags:    keyValuesFixture("span"),
		Process: &process,
		Logs:    logs,
	}

	encodeBinaryTags(&span)

	// All the binary items are in the same index position because we are using
	// the same []KeyValue fixture for span, process and logs;
	binaryVIdxs := map[int]struct{}{0: {}, 3: {}}

	assertEncoded(t, "span", span.Tags)
	assertOnlyBinariesModified(t, binaryVIdxs, keyValuesFixture("span"), span.Tags)
	assertEncoded(t, "process", span.Process.Tags)
	assertOnlyBinariesModified(t, binaryVIdxs, keyValuesFixture("process"), span.Process.Tags)
	assertEncoded(t, "log1", span.Logs[0].Fields)
	assertOnlyBinariesModified(t, binaryVIdxs, keyValuesFixture("log1"), span.Logs[0].Fields)
	assertEncoded(t, "log2", span.Logs[1].Fields)
	assertOnlyBinariesModified(t, binaryVIdxs, keyValuesFixture("log2"), span.Logs[1].Fields)
}

func keyValuesEncodedFixture() []model.KeyValue {
	return []model.KeyValue{
		{
			Key:   "binary1-data",
			VStr:  fmt.Sprintf("%s%s", MEDIA_TYPE_ENCODED_BINARY, base64.StdEncoding.EncodeToString(binaryValue1)),
			VType: model.ValueType_STRING,
		},
		{
			Key:   "string-data",
			VStr:  "My string",
			VType: model.ValueType_STRING,
		},
		{
			Key:    "int64-data",
			VInt64: 42,
			VType:  model.ValueType_INT64,
		},
		{
			Key:   "binary2-data",
			VStr:  fmt.Sprintf("%s%s", MEDIA_TYPE_ENCODED_BINARY, base64.StdEncoding.EncodeToString(binaryValue2)),
			VType: model.ValueType_STRING,
		},
		{
			Key:      "float64-data",
			VFloat64: 42.42,
			VType:    model.ValueType_FLOAT64,
		},
		{
			Key:   "bool-data",
			VBool: true,
			VType: model.ValueType_BOOL,
		},
		{
			Key:   "no-prefix",
			VStr:  base64.StdEncoding.EncodeToString(binaryValue1),
			VType: model.ValueType_STRING,
		},
		{
			Key:   "no-binary-with-prefix",
			VStr:  MEDIA_TYPE_ENCODED_BINARY + "a normal string tag",
			VType: model.ValueType_STRING,
		},
	}
}

func TestDecodeBinaryTags(t *testing.T) {
	fixture := keyValuesEncodedFixture()
	decodeBinaryTags(fixture)

	expectedBinaryV := model.KeyValue{
		Key:     "binary1-data",
		VBinary: binaryValue1,
		VType:   model.ValueType_BINARY,
	}
	expectedBinaryV2 := model.KeyValue{
		Key:     "binary2-data",
		VBinary: binaryValue2,
		VType:   model.ValueType_BINARY,
	}

	assert.Equal(t, expectedBinaryV, fixture[0])
	assert.Equal(t, expectedBinaryV2, fixture[3])

	// Only the string values that have the MEDIA_TYPE_ENCODED_BINARY prefix
	// and are followed by a valid base64 encoded string are modified.
	binaryVIdxs := map[int]struct{}{0: {}, 3: {}}
	assertOnlyBinariesModified(t, binaryVIdxs, keyValuesEncodedFixture(), fixture)
}
