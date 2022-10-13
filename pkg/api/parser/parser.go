package parser

import (
	"fmt"
	"mime"
	"net/http"

	"github.com/timescale/promscale/pkg/api/parser/json"
	"github.com/timescale/promscale/pkg/api/parser/protobuf"
	"github.com/timescale/promscale/pkg/api/parser/text"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/telemetry"
)

type formatParser func(*http.Request, *prompb.WriteRequest) error

// Preprocessor is used to transform the incoming write request before sending
// it for ingestion.
type Preprocessor interface {
	Process(*http.Request, *prompb.WriteRequest) error
}

// DefaultParser is in charge of parsing the incoming request based on the format
// and running the preprocessors on the format.
type DefaultParser struct {
	preprocessors []Preprocessor
	formatParsers map[string]formatParser
}

// NewParser returns a parser with the correct mapping of format and format parser.
func NewParser() *DefaultParser {
	return &DefaultParser{
		formatParsers: map[string]formatParser{
			"application/x-protobuf":       protobuf.ParseRequest,
			"application/json":             json.ParseRequest,
			"text/plain":                   text.ParseRequest,
			"application/openmetrics-text": text.ParseRequest,
		},
	}
}

// AddPreprocessor adds a Preprocessor to the array of preprocessors.
func (p *DefaultParser) AddPreprocessor(pre Preprocessor) {
	if pre == nil {
		return
	}
	p.preprocessors = append(p.preprocessors, pre)
}

// ParseRequest runs the correct parser on the format of the request and runs the
// preprocessors on the payload afterwards.
func (d DefaultParser) ParseRequest(r *http.Request, req *prompb.WriteRequest) error {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return fmt.Errorf("parser error: unable to parse format: %w", err)
	}
	parser, ok := d.formatParsers[mediaType]
	if !ok {
		return fmt.Errorf("parser error: unsupported format")
	}

	if err := parser(r, req); err != nil {
		return fmt.Errorf("parser error: %w", err)
	}

	updateTelemetry(mediaType)

	if len(req.Timeseries) == 0 {
		return nil
	}

	// run preprocessors
	for _, p := range d.preprocessors {
		err := p.Process(r, req)

		if err != nil {
			return err
		}

		if len(req.Timeseries) == 0 {
			return nil
		}
	}

	return nil
}

func InitTelemetry() {
	telemetry.Registry.Update("metrics_ingestion_path", "no_ingestion")
	telemetry.Registry.Update("metrics_ingestion_json_endpoint_used", "0")
}

func updateTelemetry(parser string) {
	switch parser {
	case "application/x-protobuf":
		telemetry.Registry.Update("metrics_ingestion_path", "protobuf")
	case "application/json":
		telemetry.Registry.Update("metrics_ingestion_path", "json")
		telemetry.Registry.Update("metrics_ingestion_json_endpoint_used", "1")
	case "text/plain":
		telemetry.Registry.Update("metrics_ingestion_path", "text_plain")
	case "application/openmetrics-text":
		telemetry.Registry.Update("metrics_ingestion_path", "text_open_metrics")
	default:
		telemetry.Registry.Update("metrics_ingestion_path", "none")
	}
}
