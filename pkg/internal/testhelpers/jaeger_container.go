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

func StartJaegerContainer(printLogs bool) (container testcontainers.Container, host, containerIP string, grpcReceivingPort, grpcQueryPort, uiPort nat.Port, err error) {
	grpcReceivingPort, grpcQueryPort, uiPort = "14250/tcp", "16685/tcp", "16686/tcp"
	req := testcontainers.ContainerRequest{
		Image:        jaegerImage,
		ExposedPorts: []string{string(grpcReceivingPort), string(grpcQueryPort), string(uiPort)},
		WaitingFor:   wait.ForListeningPort(grpcReceivingPort),
		Env: map[string]string{
			envSpanStorageType: "memory",
		},
	}
	container, err = testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return container, host, containerIP, grpcReceivingPort, grpcQueryPort, uiPort, fmt.Errorf("creating jaeger all-in-one container: %w", err)
	}

	if printLogs {
		container.StartLogProducer(context.Background())
		container.FollowOutput(stdoutLogConsumer{"jaeger"})
	}

	host, err = container.Host(context.Background())
	if err != nil {
		return container, host, containerIP, grpcReceivingPort, grpcQueryPort, uiPort, fmt.Errorf("host: %w", err)
	}

	containerIP, err = container.ContainerIP(context.Background())
	if err != nil {
		return nil, host, "", "", "", "", err
	}

	mappedUIPort, err := container.MappedPort(context.Background(), uiPort)
	if err != nil {
		return container, host, containerIP, grpcReceivingPort, grpcQueryPort, uiPort, fmt.Errorf("ui-port: %w", err)
	}

	mappedQueryPort, err := container.MappedPort(context.Background(), grpcQueryPort)
	if err != nil {
		return container, host, containerIP, grpcReceivingPort, grpcQueryPort, uiPort, fmt.Errorf("query-port: %w", err)
	}
	// We send actual `grpcReceivingPort` grpc port, since this will be used internally in docker network.
	return container, host, containerIP, grpcReceivingPort, mappedQueryPort, mappedUIPort, nil
}
