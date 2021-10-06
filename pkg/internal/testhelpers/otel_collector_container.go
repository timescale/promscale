// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package testhelpers

import (
	"context"
	"fmt"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const otelCollectorImage = "otel/opentelemetry-collector:latest"

func StartOtelCollectorContainer(printLogs bool) (container testcontainers.Container, host string, err error) {
	req := testcontainers.ContainerRequest{
		Image:        otelCollectorImage,
		ExposedPorts: []string{string(grpcCollectorPort), string(grpcQueryPort), string(uiPort)},
		WaitingFor:   wait.ForListeningPort(grpcCollectorPort),
		Env: map[string]string{
			envSpanStorageType:           "memory",
			"COLLECTOR_ZIPKIN_HOST_PORT": ":9411",
		},
	}
	container, err = testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return container, host, fmt.Errorf("creating jaeger all-in-one container: %w", err)
	}

	if printLogs {
		container.FollowOutput(stdoutLogConsumer{"otel-collector"})
	}

	host, err = container.Host(context.Background())
	if err != nil {
		return container, host, fmt.Errorf("host: %w", err)
	}

	return
}
