// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package cache

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/prompb"

	promLabels "github.com/prometheus/prometheus/model/labels"
)

func TestBigLabels(t *testing.T) {
	cache := NewSeriesCache(DefaultConfig, nil)
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

func TestGenerateKey(t *testing.T) {
	var labels = []prompb.Label{
		{Name: "__name__", Value: "test"},
		{Name: "hell", Value: "oworld"},
		{Name: "hello", Value: "world"},
	}
	var keyBuffer = new(bytes.Buffer)
	var metricName, err = generateKey(labels, keyBuffer)

	require.Nil(t, err)

	require.Equal(t, "test", metricName)
	require.Equal(t, []byte("\x08\x00__name__\x04\x00test\x04\x00hell\x06\x00oworld\x05\x00hello\x05\x00world"), keyBuffer.Bytes())
}

func TestGrowSeriesCache(t *testing.T) {
	testCases := []struct {
		name         string
		sleep        time.Duration
		cacheGrowCnt int
	}{
		{
			name:         "Grow criteria satisfied - we shoulnd't be evicting elements",
			sleep:        time.Millisecond,
			cacheGrowCnt: 1,
		},
		{
			name:         "Growth criteria not satisfied - we can keep evicting old elements",
			sleep:        time.Second * 2,
			cacheGrowCnt: 0,
		},
	}

	t.Setenv("IS_TEST", "true")
	evictionMaxAge = time.Second // tweaking it to not wait too long
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache := NewSeriesCache(Config{SeriesCacheInitialSize: 100, SeriesCacheMemoryMaxBytes: DefaultConfig.SeriesCacheMemoryMaxBytes}, nil)
			cacheGrowCounter := 0
			for i := 0; i < 200; i++ {
				cache.Insert(i, i, 1)
				if i%100 == 0 {
					time.Sleep(tc.sleep)
				}
				if cache.shouldGrow() {
					cache.grow()
					cacheGrowCounter++
				}
			}
			require.Equal(t, tc.cacheGrowCnt, cacheGrowCounter, "series cache unexpectedly grow")
		})
	}

}
