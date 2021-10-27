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

type JaegerContainer struct {
	Container                 testcontainers.Container
	Host, ContainerIp         string
	GrpcReceivingPort, UIPort nat.Port
}

func StartJaegerContainer(printLogs bool) (jaegerContainer *JaegerContainer, err error) {
	grpcReceivingPort, uiPort := nat.Port("14250/tcp"), nat.Port("16686/tcp")
	req := testcontainers.ContainerRequest{
		Image:        jaegerImage,
		ExposedPorts: []string{string(grpcReceivingPort), string(uiPort)},
		WaitingFor:   wait.ForListeningPort(grpcReceivingPort),
		Env: map[string]string{
			envSpanStorageType: "memory",
		},
	}
	container, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return nil, fmt.Errorf("creating jaeger all-in-one container: %w", err)
	}

	if printLogs {
		if err = container.StartLogProducer(context.Background()); err != nil {
			return nil, fmt.Errorf("error starting log producer: %w", err)
		}
		container.FollowOutput(stdoutLogConsumer{"jaeger"})
	}

	host, err := container.Host(context.Background())
	if err != nil {
		return nil, fmt.Errorf("error getting container host: %w", err)
	}

	containerIP, err := container.ContainerIP(context.Background())
	if err != nil {
		return nil, fmt.Errorf("error getting container ip: %w", err)
	}

	mappedUIPort, err := container.MappedPort(context.Background(), uiPort)
	if err != nil {
		return nil, fmt.Errorf("error mapping ui-port: %w", err)
	}

	// We send actual `containerIP` & `grpcReceivingPort` grpc port, since this will be used internally in docker network.
	return &JaegerContainer{
		Container:         container,
		Host:              host,
		ContainerIp:       containerIP,
		GrpcReceivingPort: grpcReceivingPort,
		UIPort:            mappedUIPort,
	}, nil
}
