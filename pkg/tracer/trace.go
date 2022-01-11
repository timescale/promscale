package tracer

import (
	"flag"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultTracerName = "github.com/timescale/promscale"
)

// Config represents a tracing configuration used upon initialization.
type Config struct {
	JaegerCollectorEndpoint string
	SamplingRatio           float64
}

// ParseFlags parses the configuration flags for tracing.
func ParseFlags(fs *flag.FlagSet, cfg *Config) *Config {
	fs.StringVar(&cfg.JaegerCollectorEndpoint, "telemetry.trace.jaeger-endpoint", "", "Jaeger tracing collector thrift HTTP URL endpoint to send telemetry to (e.g. https://jaeger-collector:14268/api/traces).")
	fs.Float64Var(&cfg.SamplingRatio, "telemetry.trace.sampling-ratio", 0, "Trace sampling ratio, amount of spans to send to collector. Valid values from 0.0 (none) to 1.0 (all).")
	return cfg
}

func Default() trace.Tracer {
	return otel.Tracer(defaultTracerName)
}

func InitProvider(cfg *Config) (*tracesdk.TracerProvider, error) {
	// Create the Jaeger exporter
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(cfg.JaegerCollectorEndpoint)))
	if err != nil {
		return nil, err
	}
	tp := tracesdk.NewTracerProvider(
		// Always be sure to batch in production.
		tracesdk.WithBatcher(exp),
		tracesdk.WithSampler(tracesdk.TraceIDRatioBased(cfg.SamplingRatio)),
		// Record information about this application in an Resource.
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("promscale-connector-service"),
		)),
	)
	return tp, nil
}
