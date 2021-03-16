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
			Name:      "memory_target_bytes",
			Help:      "The number of bytes of memory the system will target using",
		},
	)
)

type TargetMemory struct {
	percentage int    //integer [1-100], only set if specified as percentage
	bytes      uint64 //always set
}

func (t *TargetMemory) setPercent(percent int, sysMemory uint64) {
	t.percentage = percent
	t.bytes = uint64(float64(sysMemory) * (float64(percent) / 100))
}

func (t *TargetMemory) setBytes(bytes uint64) {
	t.percentage = 0
	t.bytes = bytes
}

func (t *TargetMemory) Set(val string) error {
	defer MemoryTargetMetric.Set(float64(t.Bytes()))
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
		sysMemory := mem.SystemMemory()
		if sysMemory == 0 {
			return fmt.Errorf("Cannot set target memory: specified in percentage terms but total system memory could not be determined")
		}
		t.setPercent(int(numeric), sysMemory)
		return nil
	}
	if numeric < 1000 {
		return fmt.Errorf("Cannot set target memory: must be more than a 1000 bytes")
	}
	t.setBytes(uint64(numeric))
	return nil
}

func (t *TargetMemory) String() string {
	if t.percentage > 0 {
		return fmt.Sprintf("%d%%", t.percentage)
	}
	return fmt.Sprintf("%d", t.bytes)
}

func (t *TargetMemory) Bytes() uint64 {
	return t.bytes
}

type Config struct {
	Memory TargetMemory
}

// ParseFlags parses the configuration flags for logging.
func ParseFlags(fs *flag.FlagSet, cfg *Config) *Config {
	sysMem := mem.SystemMemory()
	if sysMem > 0 {
		cfg.Memory.setPercent(80, sysMem)
	} else {
		cfg.Memory.setBytes(1e9) //1 GB if cannot determine system memory.
	}
	fs.Var(&cfg.Memory, "memory-target", "Target for amount of memory to use. "+
		"Specified in bytes of as a percentage of system memory (e.g. 80%).")
	return cfg
}

func init() {
	prometheus.MustRegister(MemoryTargetMetric)
}
