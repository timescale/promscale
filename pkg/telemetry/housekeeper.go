// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package telemetry

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgxconn"
)

func (t *telemetryEngine) BecomeHousekeeper() (success bool, err error) {
	acquired, err := t.telemetryLock.GetAdvisoryLock()
	if err != nil {
		return false, fmt.Errorf("attemping telemetry pg-advisory-lock: %w", err)
	}
	if acquired {
		t.isHouseKeeper = true
	}
	return acquired, nil
}

// DoHouseKeepingAsync starts telemetry housekeeping activities async. It must be called after calling BecomeHousekeeper
// and only when the returned result is success.
func (t *telemetryEngine) DoHouseKeepingAsync(ctx context.Context) error {
	if !t.isHouseKeeper {
		return fmt.Errorf("cannot do house keeping as not a house-keeper")
	}
	go t.housekeeping(ctx)
	return nil
}

// housekeeping takes all the telemetry stats, evaluates and writes into the database.
func (t *telemetryEngine) housekeeping(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second * 12):
		}
		newStats := make(Stats)
		if err := t.telemetryVisitor(newStats); err != nil {
			log.Debug("msg", "error visiting telemetry", "err", err.Error())
		}
		if len(newStats) > 0 {
			if err := syncTimescaleMetadataTable(t.conn, newStats); err != nil {
				log.Debug("msg", "syncing new stats", "error", err.Error())
			}
		}
		if err := cleanStalePromscalesAfterCounterReset(t.conn); err != nil {
			log.Error("msg", "unable to clean stale Promscale instances. Please report to Promscale team as an issue at https://github.com/timescale/promscale/issues/new",
				"err", err.Error())
		}
	}
}

// telemetryVisitor visits all telemetries and fills the provided newStats after querying from the database.
func (t *telemetryEngine) telemetryVisitor(newStats Stats) (err error) {
	for _, stat := range telemetries {
		s, ok := stat.(telemetrySQL)
		if !ok {
			log.Debug("msg", "invalid telemetry type for housekeeper. Expected telemetrySQL or telemetryPromQL", "received", reflect.TypeOf(stat))
			continue
		}
		query := stat.Query().(string)
		switch s.ResultType() {
		case isInt:
			tmp := new(int64)
			if err := t.conn.QueryRow(context.Background(), query).Scan(&tmp); err != nil {
				return fmt.Errorf("int: scanning sql stats '%s' with query '%s': %w", stat.Name(), query, err)
			}
			if tmp == nil {
				continue
			}
			newStats[stat.Name()] = strconv.Itoa(int(*tmp))
		case isString:
			tmp := new(string)
			if err := t.conn.QueryRow(context.Background(), query).Scan(&tmp); err != nil {
				return fmt.Errorf("string: scanning sql stats '%s' with query '%s': %w", stat.Name(), query, err)
			}
			if tmp == nil {
				continue
			}
			newStats[stat.Name()] = *tmp
		}
	}
	return nil
}

func cleanStalePromscalesAfterCounterReset(conn pgxconn.PgxConn) error {
	_, err := conn.Exec(context.Background(), "SELECT _ps_catalog.clean_stale_promscales_after_counter_reset()")
	return err
}
