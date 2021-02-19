package cache

import (
	"math"
	"strings"
	"testing"

	promLabels "github.com/prometheus/prometheus/pkg/labels"
)

func TestBigLables(t *testing.T) {
	cache := NewSeriesCache(100)
	builder := strings.Builder{}
	builder.Grow(int(^uint16(0)) + 1) // one greater than uint16 max

	builder.WriteByte('a')
	for len(builder.String()) < math.MaxUint16 {
		builder.WriteString(builder.String())
	}

	l := promLabels.Labels{
		promLabels.Label{
			Name:  builder.String(),
			Value: "",
		},
	}

	_, err := cache.GetSeriesFromLabels(l)
	if err == nil {
		t.Errorf("expected error")
	}
}
