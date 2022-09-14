// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package querier

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const (
	defaultDurationBetweenSamples = 15 * time.Second
	low                           = 200
	high                          = 2000
	noRollupSchema                = ""
	noColumn                      = ""
)

var (
	errNoMetricMetadata           = fmt.Errorf("metric metadata not found")
	errNoMetricColumnRelationship = fmt.Errorf("metric column relation does not exist. Possible invalid metric type")
)

type rollupDecider struct {
	conn                  pgxconn.PgxConn
	schemaResolutionCache map[string]time.Duration // schema_name: resolution
	metricMetadataCache   map[string]string        // metric_name: metric_type
	resolutionInASCOrder  []time.Duration
}

func (r *rollupDecider) refresh() {
	refreshInterval := time.NewTicker(time.Minute)
	defer refreshInterval.Stop()
	for {
		if r.refreshRollupResolution() {
			r.refreshMetricMetadata()
		}
		<-refreshInterval.C
	}
}

func (r *rollupDecider) refreshRollupResolution() (proceedToNextStep bool) {
	var (
		schemaName []string
		resolution []time.Duration
	)
	rows, err := r.conn.Query(context.Background(), "SELECT schema_name, resolution FROM _prom_catalog.rollup")
	if err != nil {
		log.Error("msg", "fetching rollup details", "error", err.Error())
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var (
			sname string
			dur   time.Duration
		)
		err = rows.Scan(&sname, &dur)
		if err != nil {
			log.Error("msg", "error scanning rows", "error", err.Error())
			return false
		}
		schemaName = append(schemaName, sname)
		resolution = append(resolution, dur)
	}
	fmt.Println(resolution)
	if len(resolution) == 0 {
		// Optimisation: No need to proceed further.
		return false
	}
	resolutionCache := make(map[string]time.Duration)
	for i := 0; i < len(schemaName); i++ {
		resolutionCache[schemaName[i]] = resolution[i]
	}
	sort.Sort(sortDuration(resolution)) // From highest resolution (say 5m) to lowest resolution (say 1h).
	r.resolutionInASCOrder = resolution
	r.schemaResolutionCache = resolutionCache
	return true
}

func (r *rollupDecider) refreshMetricMetadata() {
	var metricName, metricType []string
	err := r.conn.QueryRow(context.Background(), "select array_agg(metric_family), array_agg(type) from _prom_catalog.metadata").Scan(&metricName, &metricType)
	if err != nil {
		log.Error("msg", "fetching metric metadata", "error", err.Error())
		return
	}
	metadataCache := make(map[string]string)
	for i := range metricName {
		metadataCache[metricName[i]] = metricType[i]
	}
	r.metricMetadataCache = metadataCache
}

func (r *rollupDecider) decide(minSeconds, maxSeconds int64) (rollupSchemaName string) {
	estimateSamples := func(resolution time.Duration) int64 {
		return int64(float64(maxSeconds-minSeconds) / resolution.Seconds())
	}
	//estimatedRawSamples := estimateSamples(defaultDurationBetweenSamples)
	//if r.withinRange(estimatedRawSamples) || estimatedRawSamples < low || len(r.resolutionInASCOrder) == 0 {
	//	return noRollupSchema
	//}
	var acceptableResolution []time.Duration
	for _, resolution := range r.schemaResolutionCache {
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
	for schema, res := range r.schemaResolutionCache {
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

// metricTypeColumnRelationship is a relationship between the metric type received for query and a computation of
// set of columns that together compute to `value` of sample. This is because in metric rollups, we do not have a `value`
// column, rather they are stored as separate utility columns like {COUNTER -> [first, last], GAUGE -> [sum, count, min, max]}.
//
// For knowing the reasoning behind this, please read the design doc on metric rollups.
var metricTypeColumnRelationship = map[string]string{
	"GAUGE": "(sum / count)",
}

func (r *rollupDecider) getValueColumnString(metricName string) (string, error) {
	metricType, ok := r.metricMetadataCache[metricName]
	if !ok {
		log.Debug("msg", fmt.Sprintf("metric metadata not found for %s. Refreshing and trying again", metricName))
		r.refreshMetricMetadata()
		metricType, ok = r.metricMetadataCache[metricName]
		if ok {
			goto checkMetricColumnRelationship
		}
		return noColumn, errNoMetricMetadata
	}
checkMetricColumnRelationship:
	columnString, ok := metricTypeColumnRelationship[metricType]
	if !ok {
		return noColumn, errNoMetricColumnRelationship
	}
	return columnString, nil
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
