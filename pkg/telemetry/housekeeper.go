// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package telemetry

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/util"
)

const housekeeperLockId = 0x2D829A932AAFCEDE // Random.

type housekeeper struct {
	engineCopy    *engineImpl
	lock          util.AdvisoryLock
	isHousekeeper bool
	stop          chan struct{}
}

// Try tries to become a housekeeper of the current Promscale session.
func (t *housekeeper) Try() (success bool, err error) {
	if t.isHousekeeper {
		// If already a housekeeper, we need to explicitly say that Try was false,
		// since pg_try_advisory_lock() returns true for the same lockID for that
		// Promscale session.
		return false, nil
	}
	acquired, err := t.lock.GetAdvisoryLock()
	if err != nil {
		return false, fmt.Errorf("attemping telemetry pg-advisory-lock: %w", err)
	}
	if acquired {
		t.isHousekeeper = true
	}
	return acquired, nil
}

func (t *housekeeper) Start() error {
	if !t.isHousekeeper {
		return fmt.Errorf("not a housekeeper: cannot do housekeeping")
	}

	t.stop = make(chan struct{})
	go t.housekeeping()

	return nil
}

func (t *housekeeper) Stop() error {
	t.stop <- struct{}{}
	close(t.stop)
	_, err := t.lock.Unlock()
	if err != nil {
		return fmt.Errorf("unlock: %w", err)
	}
	t.lock.Close()
	return nil
}

func (t *housekeeper) Sync() error {
	informationTableStats, err := getInstanceInformationStats(t.engineCopy.conn)
	if err != nil {
		return fmt.Errorf("get instance stats: %w", err)
	}
	t.engineCopy.writeToTimescaleMetadataTable(convertStatsToMetadata(informationTableStats))
	return nil
}

const housekeepingDuration = time.Hour

func (t *housekeeper) housekeeping() {
	for {
		select {
		case <-t.stop:
			return
		case <-time.After(housekeepingDuration):
		}
		informationTableStats, err := getInstanceInformationStats(t.engineCopy.conn)
		if err == nil {
			t.engineCopy.writeToTimescaleMetadataTable(convertStatsToMetadata(informationTableStats))
		} else {
			log.Debug("msg", "error getting instance information table stats", "error", err.Error())
		}

		if err = CleanStalePromscalesAfterCounterReset(t.engineCopy.conn); err != nil {
			log.Debug("msg", "error cleaning stale Promscale rows", "error", err.Error())
		}
	}
}

type Stats map[string]int64

func convertStatsToMetadata(s Stats) Metadata {
	m := make(Metadata, len(s))
	for k, v := range s {
		m[trim(k, "promscale_")] = strconv.FormatInt(v, 10) // trim 'promscale_' since that will be added by the extension.
	}
	return m
}

func trim(s, trimStr string) string {
	return strings.TrimPrefix(s, trimStr)
}

func getInstanceInformationStats(conn pgxconn.PgxConn) (Stats, error) {
	stats := make(Stats)
	var (
		samples        int64
		queriesExec    int64
		queriesTimeout int64
		queriesFailed  int64
		traceQueryReqs int64
		traceDepReqs   int64
	)
	if err := conn.QueryRow(context.Background(), `
	SELECT
		sum(promscale_ingested_samples_total),
		sum(promscale_metrics_queries_executed_total),
		sum(promscale_metrics_queries_timedout_total),
		sum(promscale_metrics_queries_failed_total),
		sum(promscale_trace_query_requests_executed_total),
		sum(promscale_trace_dependency_requests_executed_total)
	FROM _ps_catalog.promscale_instance_information`).Scan(
		&samples, &queriesExec, &queriesTimeout, &queriesFailed, &traceQueryReqs, &traceDepReqs,
	); err != nil {
		return nil, fmt.Errorf("querying values from information table: %w", err)
	}
	stats["promscale_ingested_samples_total"] = samples
	stats["promscale_metrics_queries_executed_total"] = queriesExec
	stats["promscale_metrics_queries_timedout_total"] = queriesTimeout
	stats["promscale_metrics_queries_failed_total"] = queriesFailed
	stats["promscale_trace_query_requests_executed_total"] = traceQueryReqs
	stats["promscale_trace_dependency_requests_executed_total"] = traceDepReqs
	return stats, nil
}

func CleanStalePromscalesAfterCounterReset(conn pgxconn.PgxConn) error {
	_, err := conn.Exec(context.Background(), "SELECT _ps_catalog.clean_stale_promscales_after_counter_reset()")
	return err
}
