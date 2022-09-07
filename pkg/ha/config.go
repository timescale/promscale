// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ha

import (
	"flag"
	"fmt"
)

const DefaultReplicaLabelName = "__replica__"
const DefaultClusterLabelName = "cluster"

var DefaultConfig = Config{
	Enabled:          false,
	ReplicaLabelName: DefaultReplicaLabelName,
	ClusterLabelName: DefaultClusterLabelName,
}

type Config struct {
	Enabled          bool
	ReplicaLabelName string
	ClusterLabelName string
}

func ParseFlags(fs *flag.FlagSet) *Config {
	cfg := Config{}

	fs.BoolVar(&cfg.Enabled, "metrics.high-availability", DefaultConfig.Enabled, "Enable external_labels based HA.")
	fs.StringVar(&cfg.ReplicaLabelName, "metrics.high-availability.replica-label-name", DefaultConfig.ReplicaLabelName,
		"Name of the label that provides a unique identifier for a Prometheus replica in HA mode.")
	fs.StringVar(&cfg.ClusterLabelName, "metrics.high-availability.cluster-label-name", DefaultConfig.ClusterLabelName,
		"Name of the label that uniquely identifies a cluster of Prometheus instances running in HA.")

	return &cfg
}

func (cfg *Config) Validate() error {
	// Do not validate other flags if HA is not enabled.
	if !cfg.Enabled {
		return nil
	}

	if cfg.ReplicaLabelName == cfg.ClusterLabelName {
		return fmt.Errorf("replica label name and cluster label name cannot be same")
	}

	return nil
}
