//go:build linux
// +build linux

// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package mem

import (
	"github.com/containerd/cgroups"
	v2 "github.com/containerd/cgroups/v2"
	"github.com/pbnjay/memory"
)

func SystemMemory() uint64 {
	//try getting the cgroup value, if not fallback to system value
	cgroup := getCGroupMemory()
	if validMemoryValue(cgroup) {
		return cgroup
	}
	system := memory.TotalMemory()
	if validMemoryValue(system) {
		return system
	}
	return 0
}

func validMemoryValue(value uint64) bool {
	// some systems return absurdly large values for unlimited
	// so cut-off at 10 petabytes
	// e.g. docker: https://unix.stackexchange.com/questions/420906/what-is-the-value-for-the-cgroups-limit-in-bytes-if-the-memory-is-not-restricte
	if value <= 0 || value > 10e15 {
		return false
	}
	return true
}

func getCGroupMemory() uint64 {
	v2 := getCGroupV2Memory()
	if validMemoryValue(v2) {
		return v2
	}
	v1 := getCGroupV1Memory()
	if validMemoryValue(v1) {
		return v1
	}
	return 0
}

func getCGroupV2Memory() uint64 {
	manager, err := v2.LoadManager("/sys/fs/cgroup", "/")
	if err != nil {
		return 0
	}
	metrics, err := manager.Stat()
	if err != nil {
		return 0
	}
	return metrics.Memory.UsageLimit
}

func getCGroupV1Memory() uint64 {
	c, err := cgroups.Load(cgroups.V1, cgroups.StaticPath("/"))
	if err != nil {
		return 0
	}
	stats, err := c.Stat(cgroups.IgnoreNotExist)
	if err != nil {
		return 0
	}
	if stats.Memory == nil {
		return 0
	}
	return stats.Memory.HierarchicalMemoryLimit
}
