// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package limits

import (
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/limits/mem"
	"github.com/timescale/promscale/pkg/util"
)

var (
	MemoryTargetMetric = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Name:      "max_memory_target_bytes",
			Help:      "The system will try to keep memory usage below this target",
		})
)

// PercentageBytes represents a flag passed in as either a percentage value or a byte value.

type PercentageAbsoluteKind int8

const (
	Percentage PercentageAbsoluteKind = iota
	Absolute
)

// PercentageAbsoluteBytesFlag is a CLI flag type representing either an absolute or relative number of bytes
type PercentageAbsoluteBytesFlag struct {
	kind  PercentageAbsoluteKind
	value uint64
}

func (t *PercentageAbsoluteBytesFlag) SetPercent(percent int) {
	t.value = uint64(percent)
	t.kind = Percentage
}

func (t *PercentageAbsoluteBytesFlag) SetBytes(bytes uint64) {
	t.value = bytes
	t.kind = Absolute
}

func (t *PercentageAbsoluteBytesFlag) Get() (PercentageAbsoluteKind, uint64) {
	return t.kind, t.value
}

// Set implements the flag interface to set value from the CLI
func (t *PercentageAbsoluteBytesFlag) Set(val string) error {
	val = strings.TrimSpace(val)
	percentage := false
	if val[len(val)-1] == '%' {
		percentage = true
		val = val[:len(val)-1]
	}

	numeric, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return fmt.Errorf("Cannot parse target memory: %w", err)
	}

	if percentage {
		if numeric < 1 || numeric > 100 {
			return fmt.Errorf("Cannot set target memory: percentage must be in the [1,100] range")
		}
		t.SetPercent(int(numeric))
		return nil
	}
	if numeric < 1000 {
		return fmt.Errorf("Cannot set target memory: must be more than a 1000 bytes")
	}
	t.SetBytes(uint64(numeric))
	return nil
}

func (t *PercentageAbsoluteBytesFlag) String() string {
	switch t.kind {
	case Percentage:
		return fmt.Sprintf("%d%%", int(t.value))
	case Absolute:
		return fmt.Sprintf("%d", t.value)
	default:
		panic("unknown kind")
	}
}

type Config struct {
	targetMemoryFlag  PercentageAbsoluteBytesFlag
	TargetMemoryBytes uint64
}

// ParseFlags parses the configuration flags for logging.
func ParseFlags(fs *flag.FlagSet, cfg *Config) *Config {
	/* set defaults */
	sysMem := mem.SystemMemory()
	if sysMem > 0 {
		//if we can auto-detect the system memory, set default to 80%
		cfg.targetMemoryFlag.SetPercent(80)
	} else {
		//we can't detect system memory, try 1GB
		cfg.targetMemoryFlag.SetBytes(1e9)
	}

	fs.Var(&cfg.targetMemoryFlag, "memory-target", "Target for max amount of memory to use. "+
		"Specified in bytes or as a percentage of system memory (e.g. 80%).")
	return cfg
}

func Validate(cfg *Config) error {
	switch cfg.targetMemoryFlag.kind {
	case Percentage:
		sysMemory := mem.SystemMemory()
		if sysMemory == 0 {
			return fmt.Errorf("Cannot set target memory: specified in percentage terms but total system memory could not be determined")
		}
		cfg.TargetMemoryBytes = uint64(float64(sysMemory) * (float64(cfg.targetMemoryFlag.value) / 100.0))
	case Absolute:
		cfg.TargetMemoryBytes = cfg.targetMemoryFlag.value
	default:
		return fmt.Errorf("Unknown kind of input")
	}
	MemoryTargetMetric.Set(float64(cfg.TargetMemoryBytes))
	return nil
}

func init() {
	prometheus.MustRegister(
		MemoryTargetMetric,
	)
}
