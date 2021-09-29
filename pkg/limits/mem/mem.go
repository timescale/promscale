//go:build !linux
// +build !linux

// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package mem

import (
	"github.com/pbnjay/memory"
)

func SystemMemory() uint64 {
	return memory.TotalMemory()
}
