package querier

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/timescale/promscale/pkg/pgxconn"
)

const (
	defaultDurationBetweenSamples = 15 * time.Second
	low                           = 200
	high                          = 2000
	noRollupSchema                = ""
)

type rollupDecider struct {
	conn                 pgxconn.PgxConn
	cache                map[string]time.Duration // schema_name: resolution
	resolutionInASCOrder []time.Duration
}

func (r *rollupDecider) refresh() error {
	refreshInterval := time.NewTicker(time.Minute)
	defer refreshInterval.Stop()
	for {
		var (
			schemaName []string
			resolution []time.Duration
		)
		err := r.conn.QueryRow(context.Background(), "SELECT array_agg(schema_name), array_agg(resolution) FROM _prom_catalog.rollup").Scan(&schemaName, &resolution)
		if err != nil {
			return fmt.Errorf("error fetching rollup details: %w", err)
		}
		cache := make(map[string]time.Duration)
		for i := 0; i < len(schemaName); i++ {
			cache[schemaName[i]] = resolution[i]
		}
		sort.Sort(sortDuration(resolution)) // From highest resolution (say 5m) to lowest resolution (say 1h).
		r.resolutionInASCOrder = resolution
		<-refreshInterval.C
	}
}

func (r *rollupDecider) decide(minSeconds, maxSeconds int64) (rollupSchemaName string) {
	estimateSamples := func(resolution time.Duration) int64 {
		return int64(float64(maxSeconds-minSeconds) / resolution.Seconds())
	}
	estimatedRawSamples := estimateSamples(defaultDurationBetweenSamples)
	if r.withinRange(estimatedRawSamples) || estimatedRawSamples < low {
		return noRollupSchema
	}
	acceptableResolution := []time.Duration{}
	for _, resolution := range r.cache {
		estimate := estimateSamples(resolution)
		if r.withinRange(estimate) {
			acceptableResolution = append(acceptableResolution, resolution)
		}
	}
	switch len(acceptableResolution) {
	case 0:
		// Find the highest resolution that is below upper limit and respond.
		for _, res := range r.resolutionInASCOrder {
			estimate := estimateSamples(res)
			if estimate < high {
				return r.getSchemaFor(res)
			}
		}

		lowestResolution := r.resolutionInASCOrder[len(r.resolutionInASCOrder)-1]
		// This is the best case in terms of size.
		// Example: If 1 hour is the lowest resolution, then all other resolutions will be way above 1 hour.
		// Hence, the best answer is 1 hour.
		return r.getSchemaFor(lowestResolution)
	case 1:
		// Debug stuff: easy to understand.
		return r.getSchemaFor(acceptableResolution[0])
	default:
		// Multiple resolutions fit here. Hence, choose the highest resolution for maximum granularity.
		return r.getSchemaFor(acceptableResolution[0])
	}
}

func (r *rollupDecider) withinRange(totalSamples int64) bool {
	return low <= totalSamples && totalSamples <= high
}

func (r *rollupDecider) getSchemaFor(resolution time.Duration) string {
	for schema, res := range r.cache {
		if res == resolution {
			return schema
		}
	}
	panic(fmt.Sprint(
		"No schema found for resolution",
		resolution,
		"Please open an issue at https://github.com/timescale/promscale/issues",
	)) // This will never be the case.
}

type sortDuration []time.Duration

func (s sortDuration) Len() int {
	return len(s)
}

func (s sortDuration) Less(i, j int) bool {
	return s[i].Seconds() < s[j].Seconds()
}

func (s sortDuration) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
