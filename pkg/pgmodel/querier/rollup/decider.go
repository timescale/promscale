// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package rollup

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
)

type Manager struct {
	conn                  pgxconn.PgxConn
	schemaResolutionCache map[string]time.Duration // schema_name: resolution
	metricTypeCache       map[string]string        // metric_name: metric_type
	resolutionInASCOrder  []time.Duration
}

func NewManager(conn pgxconn.PgxConn) *Manager {
	rollup := &Manager{
		conn: conn,
	}
	go rollup.refresh()
	return rollup
}

func (r *Manager) refresh() {
	refreshInterval := time.NewTicker(time.Minute)
	defer refreshInterval.Stop()
	for {
		if r.refreshRollupResolution() {
			r.refreshMetricMetadata()
		}
		<-refreshInterval.C
	}
}

func (r *Manager) refreshRollupResolution() (proceedToNextStep bool) {
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

func (r *Manager) refreshMetricMetadata() {
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
	r.metricTypeCache = metadataCache
}

func (r *Manager) Decide(minSeconds, maxSeconds int64, metricName string) *Config {
	if len(r.resolutionInASCOrder) == 0 {
		return nil
	}
	estimateSamples := func(resolution time.Duration) int64 {
		return int64(float64(maxSeconds-minSeconds) / resolution.Seconds())
	}
	//estimatedRawSamples := estimateSamples(defaultDurationBetweenSamples)
	//if r.withinRange(estimatedRawSamples) || estimatedRawSamples < low || len(r.resolutionInASCOrder) == 0 {
	//	return nil
	//}
	//-- DEBUG: to return always the lowest resolution.
	//return r.getConfig(r.resolutionInASCOrder[len(r.resolutionInASCOrder)-1])
	//return nil
	//return r.getConfig(time.Hour)
	//return r.getConfig(time.Minute * 5)
	var acceptableResolution []time.Duration
	for _, resolution := range r.schemaResolutionCache {
		estimate := estimateSamples(resolution)
		fmt.Println("resolution=>", resolution, "estimate=>", estimate)
		if r.withinRange(estimate) {
			acceptableResolution = append(acceptableResolution, resolution)
		}
	}
	fmt.Println("acceptableResolution", acceptableResolution)
	if len(acceptableResolution) == 0 {
		// No resolution was found to fit within the permitted range.
		// Hence, find the highest resolution that is below upper (max) limit and respond.
		for _, res := range r.resolutionInASCOrder {
			estimate := estimateSamples(res)
			if estimate < high {
				return r.getConfig(res)
			}
		}
		// None of the resolutions were below the upper limit. Hence, respond with the lowest available resolution.
		lowestResolution := r.resolutionInASCOrder[len(r.resolutionInASCOrder)-1]
		// This is the best case in terms of size.
		// Example: If 1 hour is the lowest resolution, then all other resolutions will be way above 1 hour.
		// Hence, the best answer is 1 hour.
		//
		// Note: For understanding resolutions, in a case of resolutions [1m, 5m, 15m, 1h, 1w],
		// 1m is the highest resolution (since maximum granularity) and 1w is the lowest resolution (due to lowest granularity).
		return r.getConfig(lowestResolution)
	}
	// Choose the highest resolution for maximum granularity.
	return r.getConfig(acceptableResolution[0])
}

func (r *Manager) withinRange(totalSamples int64) bool {
	return low <= totalSamples && totalSamples <= high
}

// metricTypeColumnRelationship is a relationship between the metric type received for query and a computation of
// set of columns that together compute to `value` of sample. This is because in metric rollups, we do not have a `value`
// column, rather they are stored as separate utility columns like {COUNTER -> [first, last], GAUGE -> [sum, count, min, max]}.
//
// For knowing the reasoning behind this, please read the design doc on metric rollups.
var metricTypeColumnRelationship = map[string]string{
	"GAUGE": "(sum / count)",
}

func (r *Manager) getConfig(resolution time.Duration) *Config {
	schemaName := r.getSchemaFor(resolution)
	return &Config{
		schemaName: schemaName,
		interval:   r.schemaResolutionCache[schemaName],
		managerRef: r,
	}
}

func (r *Manager) getSchemaFor(resolution time.Duration) string {
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
