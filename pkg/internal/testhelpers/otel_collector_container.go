// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package testhelpers

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	otelCollectorImage = "otel/opentelemetry-collector:0.37.0"

	// Below config is valid with otel-collector 0.37.0
	otelCollectorConfig = `receivers:
  otlp:
    protocols:
      grpc:

processors:
  batch:

exporters:
  jaeger:
    endpoint: %s
    tls:
      insecure: true

extensions:
  health_check: {}

service:
  extensions:
    - health_check
  pipelines:
    traces:
      receivers:
        - otlp
      processors:
        - batch
      exporters:
        - jaeger

`
	healthGoodText = "Server available"
)

func StartOtelCollectorContainer(urlJaeger string, printLogs bool) (container testcontainers.Container, host string, port nat.Port, err error) {
	tempDirPath, err := TempDir("")
	if err != nil {
		return nil, host, port, fmt.Errorf("temp dir: %w", err)
	}

	configFile := filepath.Join(tempDirPath, "otel_collector_config.yml")
	config := fmt.Sprintf(otelCollectorConfig, urlJaeger)

	err = ioutil.WriteFile(configFile, []byte(config), 0777)
	if err != nil {
		return nil, "", "", err
	}

	grpcReceivingPort, healthCheckPort := nat.Port("4317/tcp"), nat.Port("13133/tcp")

	waitFor := wait.ForHTTP("")
	waitFor.Port = healthCheckPort // By default, wait.ForHTTP waits on :80, but since health check in otel-collector is on :13133, we override here.

	req := testcontainers.ContainerRequest{
		Image:        otelCollectorImage,
		ExposedPorts: []string{string(grpcReceivingPort), string(healthCheckPort)},
		WaitingFor:   waitFor,
		BindMounts: map[string]string{
			configFile: "/otel-local-config.yaml",
		},
		Cmd: []string{"--config=otel-local-config.yaml"},
	}

	container, err = testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{ContainerRequest: req})
	if err != nil {
		return container, host, port, fmt.Errorf("error creating otel-collector container: %w", err)
	}
	if err = container.Start(context.Background()); err != nil {
		return nil, "", "", fmt.Errorf("error starting otel container: %w", err)
	}

	if printLogs {
		if err = container.StartLogProducer(context.Background()); err != nil {
			return nil, "", "", fmt.Errorf("error starting log producer: %w", err)
		}
		container.FollowOutput(stdoutLogConsumer{"otel-collector"})
	}

	host, err = container.Host(context.Background())
	if err != nil {
		return container, host, port, fmt.Errorf("error getting container host: %w", err)
	}

	port, err = container.MappedPort(context.Background(), grpcReceivingPort)
	if err != nil {
		return nil, "", "", fmt.Errorf("error mapping port: %w", err)
	}

	// Do a quick health check to make sure everything is right.
	healthCheck, err := container.MappedPort(context.Background(), healthCheckPort)
	if err != nil {
		return nil, "", "", fmt.Errorf("error mapping health-check port: %w", err)
	}
	resp, err := http.Get(fmt.Sprintf("http://localhost:%s", healthCheck.Port()))
	if err != nil {
		return nil, "", "", fmt.Errorf("error checking health response: %w", err)
	}

	bSlice, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, "", "", fmt.Errorf("error reading health-check response: %w", err)
	}
	// Health check.
	if !strings.Contains(string(bSlice), healthGoodText) {
		return nil, "", "", fmt.Errorf("health check failed, received '%s' does not contain '%s'", string(bSlice), healthGoodText)
	}

	return
}
