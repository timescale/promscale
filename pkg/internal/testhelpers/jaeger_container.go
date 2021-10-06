// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package testhelpers

import (
	"context"
	"fmt"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	jaegerImage        = "jaegertracing/all-in-one:latest"
	envSpanStorageType = "SPAN_STORAGE_TYPE"
)

func StartJaegerContainer() (container testcontainers.Container, host string, grpcCollectorPort, grpcQueryPort, uiPort nat.Port, err error) {
	grpcCollectorPort, grpcQueryPort, uiPort = "14250/tcp", "16685/tcp", "16686/tcp"
	req := testcontainers.ContainerRequest{
		Image:        jaegerImage,
		ExposedPorts: []string{string(grpcCollectorPort), string(grpcQueryPort), string(uiPort)},
		WaitingFor:   wait.ForListeningPort(grpcCollectorPort),
		Env: map[string]string{
			envSpanStorageType:           "memory",
			"COLLECTOR_ZIPKIN_HOST_PORT": ":9411",
		},
	}
	container, err = testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return container, host, grpcCollectorPort, grpcQueryPort, uiPort, fmt.Errorf("creating jaeger all-in-one container: %w", err)
	}

	host, err = container.Host(context.Background())
	if err != nil {
		return container, host, grpcCollectorPort, grpcQueryPort, uiPort, fmt.Errorf("host: %w", err)
	}

	mappedGrpcCollectorPort, err := container.MappedPort(context.Background(), grpcCollectorPort)
	if err != nil {
		return container, host, grpcCollectorPort, grpcQueryPort, uiPort, fmt.Errorf("grpc-port: %w", err)
	}

	mappedUIPort, err := container.MappedPort(context.Background(), uiPort)
	if err != nil {
		return container, host, grpcCollectorPort, grpcQueryPort, uiPort, fmt.Errorf("ui-port: %w", err)
	}

	mappedQueryPort, err := container.MappedPort(context.Background(), grpcQueryPort)
	if err != nil {
		return container, host, grpcCollectorPort, grpcQueryPort, uiPort, fmt.Errorf("query-port: %w", err)
	}

	return container, host, mappedGrpcCollectorPort, mappedQueryPort, mappedUIPort, nil
}
