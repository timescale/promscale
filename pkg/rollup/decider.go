// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package rollup

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const (
	DefaultSchema         = "prom_data"
	upperLimit            = 5000 // Maximum samples allowed
	refreshRollupInterval = time.Minute * 30
)

type rollupInfo struct {
	schemaName      string
	refreshInterval time.Duration
}

type Decider struct {
	conn       pgxconn.PgxConn
	ctx        context.Context
	refreshMtx sync.RWMutex

	scrapeInterval      time.Duration
	downsamplingEnabled bool

	supportedMetrics map[string]struct{}
	rollups          []rollupInfo // {schemaName, refreshInterval} in ascending order of interval. Lesser the interval, more the granularity.
}

func NewDecider(ctx context.Context, conn pgxconn.PgxConn, scrapeInterval time.Duration) (*Decider, error) {
	helper := &Decider{
		ctx:            ctx,
		conn:           conn,
		scrapeInterval: scrapeInterval,
	}
	if err := helper.runRefreshRoutine(refreshRollupInterval); err != nil {
		return nil, fmt.Errorf("refresh: %w", err)
	}
	return helper, nil
}

// Decide returns the schema name of the rollups that should be used for querying.
// The returned schema represents a downsampled Prometheus data that should provide optimal
// granularity for querying.
//
// If no rollups exists or if downsampling is disabled, DefaultSchema (i.e., "prom_data") is returned.
func (h *Decider) Decide(minTs, maxTs int64) string {
	h.refreshMtx.RLock()
	defer h.refreshMtx.RUnlock()

	if !h.downsamplingEnabled || len(h.rollups) == 0 {
		return DefaultSchema
	}
	estimateSamples := func(interval time.Duration) int64 {
		return int64(float64(maxTs-minTs) / interval.Seconds())
	}

	numRawSamples := estimateSamples(h.scrapeInterval)
	if numRawSamples < upperLimit {
		return DefaultSchema
	}

	for _, info := range h.rollups {
		samples := estimateSamples(info.refreshInterval) // Interval between 2 samples.
		if samples < upperLimit {
			// h.rollups is sorted by interval. So, the first rollup that is below upper limit is our answer.
			// This is because it gives the maximum granularity while being in acceptable limits.
			return info.schemaName
		}
	}
	// All rollups are above upper limit. Hence, send the schema of the highest interval so the granularity
	// is minimum and we do not affect the performance of PromQL engine.
	highestInterval := h.rollups[len(h.rollups)-1]
	return highestInterval.schemaName
}

func (h *Decider) SupportsRollup(metricName string) bool {
	_, rollupExists := h.supportedMetrics[metricName]
	return rollupExists
}

func (h *Decider) Refresh() error {
	h.refreshMtx.Lock()
	defer h.refreshMtx.Unlock()

	if err := h.refreshDownsamplingState(); err != nil {
		return fmt.Errorf("downsampling state: %w", err)
	}
	if err := h.refreshSupportedMetrics(); err != nil {
		return fmt.Errorf("metric-type: %w", err)
	}
	if err := h.refreshRollup(); err != nil {
		return fmt.Errorf("rollup: %w", err)
	}
	return nil
}

func (h *Decider) runRefreshRoutine(refreshInterval time.Duration) error {
	if err := h.Refresh(); err != nil {
		return fmt.Errorf("refresh: %w", err)
	}
	go func() {
		t := time.NewTicker(refreshInterval)
		defer t.Stop()
		for {
			select {
			case <-h.ctx.Done():
				return
			case <-t.C:
			}
			if err := h.Refresh(); err != nil {
				log.Error("msg", "error refreshing rollups", "error", err.Error())
			}
		}
	}()
	return nil
}

func (h *Decider) refreshDownsamplingState() error {
	var state bool
	if err := h.conn.QueryRow(h.ctx, "SELECT prom_api.get_automatic_downsample()::BOOLEAN").Scan(&state); err != nil {
		return fmt.Errorf("fetching automatic downsampling state: %w", err)
	}
	h.downsamplingEnabled = state
	return nil
}

const supportedMetricsQuery = `SELECT m.metric_name AS supported_metrics FROM _prom_catalog.metric_rollup mr INNER JOIN _prom_catalog.metric m ON mr.metric_id = m.id GROUP BY supported_metrics;`

func (h *Decider) refreshSupportedMetrics() error {
	rows, err := h.conn.Query(h.ctx, supportedMetricsQuery)
	if err != nil {
		return fmt.Errorf("fetching supported metrics for rollups: %w", err)
	}
	defer rows.Close()

	h.supportedMetrics = make(map[string]struct{}) // metric_name: metric_type
	for rows.Next() {
		var supportedMetric string
		err = rows.Scan(&supportedMetric)
		if err != nil {
			return fmt.Errorf("error scanning the fetched supported metric: %w", err)
		}
		h.supportedMetrics[supportedMetric] = struct{}{}
	}
	return nil
}

func (h *Decider) refreshRollup() error {
	rows, err := h.conn.Query(h.ctx, "SELECT schema_name, resolution FROM _prom_catalog.rollup ORDER BY resolution ASC")
	if err != nil {
		return fmt.Errorf("fetching rollup: %w", err)
	}
	defer rows.Close()
	h.rollups = []rollupInfo{}
	for rows.Next() {
		var (
			schemaName      string
			refreshInterval time.Duration
		)
		if err = rows.Scan(&schemaName, &refreshInterval); err != nil {
			return fmt.Errorf("error scanning rows: %w", err)
		}
		h.rollups = append(h.rollups, rollupInfo{schemaName: schemaName, refreshInterval: refreshInterval})
	}
	return nil
}
