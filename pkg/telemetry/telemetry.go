// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package telemetry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgtype"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/model/pgutf8str"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/util"
)

// ExecPlatform To fill this variable in build time, use linker flags.
// Example: go build -ldflags="-X github.com/timescale/promscale/pkg/telemetry.ExecPlatform=<any_string>" ./cmd/promscale/
var ExecPlatform string

type telemetryEngine struct {
	uuid [16]byte
	conn pgxconn.PgxConn

	isHouseKeeper bool
	telemetryLock util.AdvisoryLock

	metricMux sync.RWMutex
	metrics   []telemetryMetric
}

type Telemetry interface {
	StartRoutineAsync(context.Context)
	// BecomeHousekeeper tries to become a telemetry housekeeper. If it succeeds,
	// the caller must call DoHouseKeepingAsync() otherwise the telemetry advisory
	// lock is unreleased.
	BecomeHousekeeper() (success bool, err error)
	DoHouseKeepingAsync(context.Context) error
	// RegisterMetric registers a Prometheus Gauge or Counter metric for telemetry visitor.
	// It must be called after creating the telemetry engine.
	RegisterMetric(statName string, gaugeOrCounterMetric prometheus.Metric) error
}

const telemetryLockId = 0x2D829A932AAFCEDE // Random.

func NewTelemetryEngine(conn pgxconn.PgxConn, uuid [16]byte, connStr string) (Telemetry, error) {
	// Warn the users about telemetry collection.
	log.Warn("msg", "Promscale collects anonymous usage telemetry data for project improvements while being GDPR compliant. "+
		"If you wish not to participate, please turn off sending telemetry in TimescaleDB. "+
		"Details about the process can be found at https://docs.timescale.com/timescaledb/latest/how-to-guides/configuration/telemetry/#disabling-telemetry")
	advisoryLock, err := util.NewPgAdvisoryLock(telemetryLockId, connStr)
	if err != nil {
		return nil, fmt.Errorf("error getting pg-advisory-lock: %w", err)
	}
	engine := &telemetryEngine{
		conn:          conn,
		uuid:          uuid,
		telemetryLock: advisoryLock,
	}
	if err = engine.writeMetadata(); err != nil {
		return nil, fmt.Errorf("writing metadata: %w", err)
	}
	return engine, nil
}

// RegisterMetric registers a counter or gauge metric for telemetry purpose.
func (t *telemetryEngine) RegisterMetric(statsName string, metric prometheus.Metric) error {
	ok, err := isCounterOrGauge(metric)
	if err != nil {
		return fmt.Errorf("is counter or gauge: %w", err)
	}
	if !ok {
		return fmt.Errorf("metric should only be of type Counter or Gauge")
	}
	t.metricMux.Lock()
	defer t.metricMux.Unlock()
	t.metrics = append(t.metrics, telemetryMetric{stat: statsName, metric: metric})
	return nil
}

func isCounterOrGauge(metric prometheus.Metric) (bool, error) {
	var temp io_prometheus_client.Metric
	if err := metric.Write(&temp); err != nil {
		return false, fmt.Errorf("writing metric: %w", err)
	}
	if temp.Gauge != nil {
		return true, nil
	} else if temp.Counter != nil {
		return true, nil
	}
	return false, nil
}

func (t *telemetryEngine) StartRoutineAsync(ctx context.Context) {
	go t.telemetrySync(ctx)
}

// telemetryStats are collected for telemetry information by all Promscale instances and sorted
// in the instance information table.
type telemetryStats struct {
	samplesIngested                float64
	promqlQueriesExecuted          float64
	promqlQueriesTimedout          float64
	promqlQueriesFailed            float64
	traceQueriesExecuted           float64
	traceDependencyQueriesExecuted float64
}

func (t *telemetryEngine) telemetrySync(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second * 10):
		}
		t.metricMux.RLock()
		if len(t.metrics) == 0 {
			t.metricMux.RUnlock()
			continue
		}
		newStats := telemetryStats{}
		if err := t.metricsVisitor(&newStats); err != nil {
			log.Debug("msg", "error extracting underlying value of a stats metric", "error", err.Error())
			continue
		}
		if err := syncInfoTable(t.conn, t.uuid, newStats); err != nil {
			log.Debug("msg", "syncing new stats", "error", err.Error())
		}

		if !t.isHouseKeeper {
			// Check if housekeeper has died. If yes, then become the housekeeper.
			success, err := t.BecomeHousekeeper()
			if err != nil {
				log.Info("msg", "cannot take the position of telemetry housekeeper", "error", err.Error())
				continue
			}
			if success {
				// Now we are the housekeeper. Use the context of telemetrySync for graceful shutdowns.
				if err = t.DoHouseKeepingAsync(ctx); err != nil {
					log.Error("msg", "unable to do telemetry housekeeping after taking its position", "err", err.Error())
					continue
				}
				log.Info("msg", "Now, I am the telemetry housekeeper")
			}
		}
	}
}

// metricsVisitor fills the provided newStats with most recent metric values.
func (t *telemetryEngine) metricsVisitor(newStats *telemetryStats) error {
	t.metricMux.RLock()
	defer t.metricMux.RUnlock()
	if len(t.metrics) == 0 {
		return nil
	}
	for _, stat := range t.metrics {
		underlyingValue, err := extractMetricValue(stat.metric)
		if err != nil {
			return fmt.Errorf("extracting metric value of stat '%s' with metric '%s': %w", stat.Name(), stat.metric, err)
		}
		switch stat.Name() {
		case "telemetry_metrics_ingested_samples":
			newStats.samplesIngested = underlyingValue
		case "telemetry_metrics_queries_failed":
			newStats.promqlQueriesFailed = underlyingValue
		case "telemetry_metrics_queries_executed":
			newStats.promqlQueriesExecuted = underlyingValue
		case "telemetry_metrics_queries_timed_out":
			newStats.promqlQueriesTimedout = underlyingValue
		case "telemetry_traces_queries_executed":
			newStats.traceQueriesExecuted = underlyingValue
		case "telemetry_traces_dependency_queries_executed":
			newStats.traceDependencyQueriesExecuted = underlyingValue
		default:
			log.Error("msg", fmt.Sprintf("unregistered stat name: %s", stat.Name()))
		}
	}
	return nil
}

func extractMetricValue(metric prometheus.Metric) (float64, error) {
	var internal io_prometheus_client.Metric
	if err := metric.Write(&internal); err != nil {
		return 0, fmt.Errorf("error writing metric: %w", err)
	}
	if internal.Gauge != nil {
		return internal.Gauge.GetValue(), nil
	} else if internal.Counter != nil {
		return internal.Counter.GetValue(), nil
	}
	return 0, fmt.Errorf("both Gauge and Counter are nil")
}

// writeMetadata writes Promscale and Tobs metadata. Must be written only by
func (t *telemetryEngine) writeMetadata() error {
	promscale, err := promscaleMetadata()
	if err != nil {
		return fmt.Errorf("promscale metadata: %w", err)
	}
	promscale["promscale_exec_platform"] = ExecPlatform
	if err := syncTimescaleMetadataTable(t.conn, Stats(promscale)); err != nil {
		return fmt.Errorf("writing metadata for promscale: %w", err)
	}

	tobs := tobsMetadata()
	if len(tobs) > 0 {
		if err := syncTimescaleMetadataTable(t.conn, Stats(tobs)); err != nil {
			return fmt.Errorf("writing metadata for tobs: %w", err)
		}
	}
	return nil
}

// syncTimescaleMetadataTable syncs the metadata/stats with telemetry metadata table and returns the last error if any.
func syncTimescaleMetadataTable(conn pgxconn.PgxConn, m Stats) error {
	batch := conn.NewBatch()
	for key, metadata := range m {
		safe := pgutf8str.Text{}
		if err := safe.Set(metadata); err != nil {
			return fmt.Errorf("setting in pgutf8 safe string: %w", err)
		}
		query := "INSERT INTO _timescaledb_catalog.metadata VALUES ( $1, $2, true ) ON CONFLICT (key) DO UPDATE SET value = $2, include_in_telemetry = true WHERE metadata.key = $1"
		batch.Queue(query, key, safe)
	}
	results, err := conn.SendBatch(context.Background(), batch)
	if err != nil {
		return fmt.Errorf("error sending batch: %w", err)
	}
	rows, err := results.Query()
	if err != nil {
		return fmt.Errorf("error querying results: %w", err)
	}
	for rows.Next() {
		if err != nil {
			err = rows.Err()
		}
		rows.Close()
	}
	return err
}

// syncInfoTable stats with promscale_instance_information table.
func syncInfoTable(conn pgxconn.PgxConn, uuid [16]byte, s telemetryStats) error {
	lastUpdated := time.Now()

	pgUUID := new(pgtype.UUID)
	if err := pgUUID.Set(uuid); err != nil {
		return fmt.Errorf("setting pg-uuid: %w", err)
	}

	query := `INSERT INTO _ps_catalog.promscale_instance_information
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	ON CONFlICT (uuid) DO
		UPDATE SET
			last_updated = $2,
			telemetry_metrics_ingested_samples = $3,
			telemetry_metrics_queries_executed = $4,
			telemetry_metrics_queries_timed_out = $5,
			telemetry_metrics_queries_failed = $6,
			telemetry_traces_queries_executed = $7,
			telemetry_traces_dependency_queries_executed = $8
`

	_, err := conn.Exec(context.Background(), query, pgUUID, lastUpdated,
		s.samplesIngested, s.promqlQueriesExecuted, s.promqlQueriesTimedout, s.promqlQueriesFailed,
		s.traceQueriesExecuted, s.traceDependencyQueriesExecuted)
	if err != nil {
		return fmt.Errorf("executing telemetry sync query: %w", err)
	}
	return nil
}

type noop struct{}

func NewNoopEngine() Telemetry                              { return noop{} }
func (noop) StartRoutineAsync(context.Context)              {}
func (noop) BecomeHousekeeper() (bool, error)               { return false, nil }
func (noop) DoHouseKeepingAsync(context.Context) error      { return nil }
func (noop) RegisterMetric(string, prometheus.Metric) error { return nil }
