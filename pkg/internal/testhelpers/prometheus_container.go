// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package testhelpers

import (
	"context"
	"os"
	"path/filepath"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const prometheusImage = "prom/prometheus:main"

// StartPromContainer starts a Prometheus container for use in testing
func StartPromContainer(storagePath string, ctx context.Context) (testcontainers.Container, string, nat.Port, error) {
	// Set the storage directories permissions so Prometheus can write to them.
	err := os.Chmod(storagePath, 0777) // #nosec
	if err != nil {
		return nil, "", "", err
	}

	err = os.Chmod(filepath.Join(storagePath, "wal"), 0777) // #nosec
	if err != nil {
		return nil, "", "", err
	}

	promConfigFile := filepath.Join(storagePath, "prometheus.yml")
	err = os.WriteFile(promConfigFile, []byte(emptyPromConfig), 0777) // #nosec
	if err != nil {
		return nil, "", "", err
	}
	prometheusPort := nat.Port("9090/tcp")
	req := testcontainers.ContainerRequest{
		Image:        prometheusImage,
		ExposedPorts: []string{string(prometheusPort)},
		WaitingFor:   wait.ForListeningPort(prometheusPort),
		Mounts: testcontainers.Mounts(
			testcontainers.BindMount(storagePath, "/prometheus"),
			testcontainers.BindMount(promConfigFile, "/etc/prometheus/prometheus.yml"),
		),
		Cmd: []string{
			// Default configuration.
			"--config.file=/etc/prometheus/prometheus.yml",
			"--storage.tsdb.path=/prometheus",
			"--web.console.libraries=/usr/share/prometheus/console_libraries",
			"--web.console.templates=/usr/share/prometheus/consoles",
			"--log.level=debug",

			// Enable features.
			"--enable-feature=promql-at-modifier,promql-negative-offset,exemplar-storage",

			// This is to stop Prometheus from messing with the data.
			"--storage.tsdb.retention.time=30y",
			"--storage.tsdb.min-block-duration=30y",
			"--storage.tsdb.max-block-duration=30y",
		},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", "", err
	}
	host, err := container.Host(ctx)
	if err != nil {
		return nil, "", "", err
	}

	port, err := container.MappedPort(ctx, prometheusPort)
	if err != nil {
		return nil, "", "", err
	}

	return container, host, port, nil
}
