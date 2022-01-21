// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package telemetry

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgtype"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/model/pgutf8str"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/promql"
)

// Engine for telemetry performs activities like inserting metadata of Promscale and Tobs (env vars with 'TOBS_TELEMETRY_')
// into the _timescaledb_catalog.metadata table. It allows the caller to register Prometheus Counter & Gauge metrics
// that will be monitored every hour and filled into _ps_catalog.promscale_instance_information table and then into
// the _timescaledb_catalog.metadata table.
type Engine interface {
	RegisterMetric(columnName string, gaugeOrCounterMetric prometheus.Metric) error
	Start()
	Stop()
}

type engineImpl struct {
	uuid [16]byte
	conn pgxconn.PgxConn

	stop chan struct{}

	promqlEngine    *promql.Engine
	promqlQueryable promql.Queryable

	metrics sync.Map
}

func NewEngine(conn pgxconn.PgxConn, uuid [16]byte, promqlQueryable promql.Queryable) (*engineImpl, error) {
	isTelemetryOff, err := isTelemetryOff(conn)
	if err != nil {
		log.Debug("msg", "unable to get TimescaleDB telemetry configuration. Maybe TimescaleDB is not installed", "err", err.Error())
	}
	if isTelemetryOff {
		return nil, nil
	} else {
		// Warn the users about telemetry collection only if telemetry collection is enabled.
		log.Warn("msg", "Promscale collects anonymous usage telemetry data to help the Promscale team better understand and assist users. "+
			"This can be disabled via the process described at https://docs.timescale.com/timescaledb/latest/how-to-guides/configuration/telemetry/#disabling-telemetry")
	}

	t := &engineImpl{
		conn: conn,
		uuid: uuid,
		promqlEngine: promql.NewEngine(promql.EngineOpts{
			// Similar to prometheus defaults, except the timeout.
			Logger:                   log.GetLogger(),
			Reg:                      prometheus.NewRegistry(),
			MaxSamples:               50000000,
			Timeout:                  promqlQueryTimeout,
			LookbackDelta:            5 * time.Minute,
			NoStepSubqueryIntervalFn: func(int64) int64 { return time.Minute.Milliseconds() },
		}),
		promqlQueryable: promqlQueryable,
	}

	if err := t.writeMetadata(); err != nil {
		return nil, fmt.Errorf("writing metadata: %w", err)
	}
	return t, nil
}

func isTelemetryOff(conn pgxconn.PgxConn) (bool, error) {
	var state string
	err := conn.QueryRow(context.Background(), "SHOW timescaledb.telemetry_level").Scan(&state)
	if err != nil {
		// Return true as telemetry is by default not collected when TimescaleDB is not installed.
		return true, fmt.Errorf("fetching timescaledb telemetry setting: %w", err)
	}
	if state == "off" {
		return true, nil
	}
	return false, nil
}

// writeMetadata writes Promscale and Tobs metadata.
func (t *engineImpl) writeMetadata() error {
	promscale := promscaleMetadata()
	t.writeToTimescaleMetadataTable(promscale)

	tobs := tobsMetadata()
	if len(tobs) > 0 {
		t.writeToTimescaleMetadataTable(tobs)
	}
	return nil
}

const (
	metadataUpdateWithExtension = "SELECT " + schema.Ext + ".update_tsprom_metadata($1, $2, $3)"
	metadataUpdateNoExtension   = "INSERT INTO _timescaledb_catalog.metadata(key, value, include_in_telemetry) VALUES ('promscale_' || $1, $2, $3) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, include_in_telemetry = EXCLUDED.include_in_telemetry"
)

// writeToTimescaleMetadataTable syncs the metadata/stats with telemetry metadata table.
func (t *engineImpl) writeToTimescaleMetadataTable(m Metadata) {
	// Try to update via Promscale extension.
	if err := t.syncWithMetadataTable(metadataUpdateWithExtension, m); err != nil {
		// Promscale extension not installed. Try to attempt to write directly as a rare attempt
		// in case we fix the _timescaledb_catalog.metadata permissions in the future.
		_ = t.syncWithMetadataTable(metadataUpdateNoExtension, m)
	}
}

func (t *engineImpl) syncWithMetadataTable(queryFormat string, m Metadata) error {
	batch := t.conn.NewBatch()
	for key, metadata := range m {
		safe := pgutf8str.Text{}
		if err := safe.Set(metadata); err != nil {
			return fmt.Errorf("setting in pgutf8 safe string: %w", err)
		}
		query := queryFormat
		batch.Queue(query, key, safe, true)
	}

	results, err := t.conn.SendBatch(context.Background(), batch)
	if err != nil {
		return fmt.Errorf("error sending batch: %w", err)
	}
	defer results.Close()

	for range m {
		_, err := results.Exec()
		if err != nil {
			// Metadata table not accessible. Try for func in Promscale extension.
			return fmt.Errorf("error querying results: %w", err)
		}
	}
	return err
}

func (t *engineImpl) Sync() error {
	if err := t.syncWithInfoTable(); err != nil {
		return err
	}
	t.housekeeping()
	return nil
}

func (t *engineImpl) Stop() {
	t.stop <- struct{}{}
	close(t.stop)
}

const telemetrySync = time.Hour

func (t *engineImpl) Start() {
	t.stop = make(chan struct{})
	go func() {
		collect := time.NewTicker(telemetrySync) // Collect telemetry info every hour.
		defer collect.Stop()

		for {
			select {
			case <-t.stop:
				return
			case <-collect.C:
			}
			if err := t.Sync(); err != nil {
				log.Debug("msg", "error syncing telemetry", "err", err.Error())
			}
		}
	}()
}

func (t *engineImpl) syncWithInfoTable() error {
	var (
		err             error
		underlyingValue float64

		newStats = make(map[string]float64)
	)
	t.metrics.Range(func(statName, metric interface{}) bool {
		columnName := statName.(string)
		promMetric := metric.(prometheus.Metric)
		underlyingValue, err = extractMetricValue(promMetric)
		if err != nil {
			err = fmt.Errorf("extracting metric value of stat '%s': %w", columnName, err)
			return false
		}

		newStats[columnName] = underlyingValue
		return true
	})
	if err != nil {
		return fmt.Errorf("extracting underlying value of a stats metric: %w", err)
	}
	if err := t.syncInfoTable(newStats); err != nil {
		return fmt.Errorf("sync new stats: %w", err)
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

// RegisterMetric registers a Prometheus Gauge or Counter metric for telemetry visitor.
// It must be called after creating the telemetry engine.
func (t *engineImpl) RegisterMetric(columnName string, gaugeOrCounterMetric prometheus.Metric) error {
	if !isCounterOrGauge(gaugeOrCounterMetric) {
		return fmt.Errorf("metric not a counter or gauge")
	}
	t.metrics.Store(columnName, gaugeOrCounterMetric)
	return nil
}

func isCounterOrGauge(metric prometheus.Metric) bool {
	switch metric.(type) {
	case prometheus.Counter, prometheus.Gauge:
		return true
	default:
		return false
	}
}

// syncInfoTable stats with promscale_instance_information table.
func (t *engineImpl) syncInfoTable(stats map[string]float64) error {
	lastUpdated := time.Now()

	pgUUID := new(pgtype.UUID)
	if err := pgUUID.Set(t.uuid); err != nil {
		return fmt.Errorf("setting pg-uuid: %w", err)
	}

	columnNames := []string{"uuid", "last_updated"}
	columnValues := []interface{}{pgUUID, lastUpdated}
	indexes := []string{"$1", "$2"}
	updateStatements := []string{"last_updated = $2"}

	index := 2 // Since 1 & 2 are uuid & last_updated.
	for k, v := range stats {
		index++
		indexes = append(indexes, fmt.Sprintf("$%d", index))
		columnNames = append(columnNames, k)
		columnValues = append(columnValues, v)
		updateStatements = append(updateStatements, fmt.Sprintf("%s = $%d", k, index))
	}

	// Sample query:
	// INSERT INTO _ps_catalog.promscale_instance_information
	//	VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	//		ON CONFLICT (uuid) DO
	//	UPDATE SET
	//		last_updated = $2,
	//		promscale_ingested_samples_total = $3,
	//		promscale_metrics_queries_executed_total = $4,
	//		promscale_metrics_queries_timedout_total = $5,
	//		promscale_metrics_queries_failed_total = $6,
	//		promscale_trace_query_requests_executed_total = $7,
	//		promscale_trace_dependency_requests_executed_total = $8

	query := fmt.Sprintf(`INSERT INTO %s.promscale_instance_information(%s) VALUES (%s)
	ON CONFLICT (uuid) DO UPDATE SET %s`,
		schema.PromscaleCatalog,
		strings.Join(columnNames, ", "),
		strings.Join(indexes, ", "),
		strings.Join(updateStatements, ", "),
	)
	_, err := t.conn.Exec(context.Background(), query, columnValues...)
	if err != nil {
		return fmt.Errorf("executing telemetry sync query: %w", err)
	}
	return nil
}

func (t *engineImpl) housekeeping() {
	var updateInformationStats bool
	err := t.conn.QueryRow(context.Background(), "SELECT _ps_catalog.promscale_telemetry_housekeeping($1)", telemetrySync).Scan(&updateInformationStats)
	if err != nil {
		log.Debug("msg", "error performing promscale telemetry housekeeping", "err", err.Error())
	}
	// Update information stats only when last run was beyond telemetrySync.
	if !updateInformationStats {
		return
	}

	stats, err := t.getInstanceInformationStats()
	if err != nil {
		log.Debug("msg", "error getting instance information stats", "err", err.Error())
		return
	}
	t.writeToTimescaleMetadataTable(stats)
	t.syncSQLStats()
	t.syncPromqlTelemetry()
}

func (t *engineImpl) syncSQLStats() {
	_, err := t.conn.Exec(context.Background(), "SELECT _ps_catalog.promscale_sql_telemetry()")
	if err != nil {
		log.Debug("msg", "error getting or setting sql based telemetry stats", "err", err.Error())
	}
}

func (t *engineImpl) getInstanceInformationStats() (Metadata, error) {
	stats := make(Metadata)
	var (
		samples        int64
		queriesExec    int64
		queriesTimeout int64
		queriesFailed  int64
		traceQueryReqs int64
		traceDepReqs   int64
	)
	if err := t.conn.QueryRow(context.Background(), `
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
	stats["ingested_samples_total"] = convertIntToString(samples)
	stats["metrics_queries_executed_total"] = convertIntToString(queriesExec)
	stats["metrics_queries_timedout_total"] = convertIntToString(queriesTimeout)
	stats["metrics_queries_failed_total"] = convertIntToString(queriesFailed)
	stats["trace_query_requests_executed_total"] = convertIntToString(traceQueryReqs)
	stats["trace_dependency_requests_executed_total"] = convertIntToString(traceDepReqs)
	return stats, nil
}

// Let's keep this high as large systems can take a good time evaluating histograms.
const promqlQueryTimeout = time.Minute * 10

func (t *engineImpl) syncPromqlTelemetry() {
	if t.promqlQueryable == nil {
		// When not testing PromQL stats, let's skip this function.
		return
	}
	stats := make(Metadata)
	start := time.Now()
	for _, query := range promqlStats {
		value, err := query.execute(t.promqlEngine, t.promqlQueryable)
		if err != nil {
			log.Debug("msg", "error executing promql expression", "err", err.Error())
			continue
		}
		stats[query.name] = fmt.Sprintf("%.4f", value)
	}

	evalDuration := time.Since(start)
	stats["promql_telemetry_evaluation_duration_seconds"] = fmt.Sprintf("%.3f", evalDuration.Seconds())

	t.writeToTimescaleMetadataTable(stats)
}

func convertIntToString(i int64) string {
	return strconv.FormatInt(i, 10)
}

type noop struct{}

func NewNoopEngine() Engine                                 { return noop{} }
func (noop) Start()                                         {}
func (noop) Stop()                                          {}
func (noop) RegisterMetric(string, prometheus.Metric) error { return nil }
