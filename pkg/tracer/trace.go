package tracer

import (
	"context"
	"flag"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	//"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"

	"go.opentelemetry.io/otel/trace"
)

const (
	defaultTracerName = "github.com/timescale/promscale"
)

var defaultTracer = otel.Tracer(defaultTracerName)

// Config represents a tracing configuration used upon initialization.
type Config struct {
	OtelCollectorEndpoint    string
	OtelCollectorTLSCertFile string
	OtelCollectorTLSKeyFile  string

	JaegerCollectorEndpoint string
	SamplingRatio           float64
}

// CanExportTraces checks if we have the necessary configurations to export traces telemetry.
func (c Config) CanExportTraces() bool {
	return c.OtelCollectorEndpoint != "" ||
		c.JaegerCollectorEndpoint != ""
}

// ParseFlags parses the configuration flags for tracing.
func ParseFlags(fs *flag.FlagSet, cfg *Config) *Config {
	fs.StringVar(&cfg.OtelCollectorEndpoint, "telemetry.trace.otel-endpoint", "", "OpenTelemetry tracing collector GRPC URL endpoint to send telemetry to. (i.e. otel-collector:4317)")
	fs.StringVar(&cfg.OtelCollectorTLSCertFile, "telemetry.trace.otel-tls-cert-file", "", "TLS Certificate file used for client authentication against the OTEL tracing collectore GRPC endpoint, leave blank to disable TLS.")
	fs.StringVar(&cfg.OtelCollectorTLSKeyFile, "telemetry.trace.otel-tls-key-file", "", "TLS Key file for client authentication against the OTEL tracing collectore GRPC endpoint, leave blank to disable TLS.")
	fs.StringVar(&cfg.JaegerCollectorEndpoint, "telemetry.trace.jaeger-endpoint", "", "Jaeger tracing collector thrift HTTP URL endpoint to send telemetry to (e.g. https://jaeger-collector:14268/api/traces).")
	fs.Float64Var(&cfg.SamplingRatio, "telemetry.trace.sampling-ratio", 1, "Trace sampling ratio, amount of spans to send to collector. Valid values from 0.0 (none) to 1.0 (all).")
	return cfg
}

func Default() trace.Tracer {
	return defaultTracer
}

func InitProvider(cfg *Config) (*tracesdk.TracerProvider, error) {
	var (
		err  error
		opts = make([]tracesdk.TracerProviderOption, 0)
	)

	if cfg.JaegerCollectorEndpoint != "" {
		// Create the Jaeger exporter.
		exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(cfg.JaegerCollectorEndpoint)))
		if err != nil {
			return nil, err
		}

		// Always be sure to batch in production.
		opts = append(opts, tracesdk.WithBatcher(exp))
	}

	if cfg.OtelCollectorEndpoint != "" {
		creds := insecure.NewCredentials()
		if cfg.OtelCollectorTLSCertFile != "" {
			creds, err = credentials.NewClientTLSFromFile(cfg.OtelCollectorTLSCertFile, cfg.OtelCollectorTLSKeyFile)
			if err != nil {
				return nil, err
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create GRPC connection to collector.
		conn, err := grpc.DialContext(
			ctx,
			cfg.OtelCollectorEndpoint,
			grpc.WithTransportCredentials(creds),
			grpc.WithDefaultCallOptions(grpc.ForceCodec(newClientCodec())),
			grpc.WithBlock(),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create gRPC connection to collector: %w", err)
		}

		// Create the OTEL exporter.
		exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
		if err != nil {
			return nil, fmt.Errorf("failed to create trace exporter: %w", err)
		}

		// Always be sure to batch in production.
		opts = append(opts, tracesdk.WithBatcher(exp))
	}

	if len(opts) == 0 {
		return nil, fmt.Errorf("tracing telemetry requires setting either the open telemetry collector endpoint or the jaeger endpoint")
	}

	opts = append(
		opts,
		tracesdk.WithSampler(tracesdk.TraceIDRatioBased(cfg.SamplingRatio)),
		// Record information about this application in an Resource.
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("promscale-connector-service"),
		)),
	)

	tp := tracesdk.NewTracerProvider(opts...)
	return tp, nil
}
