// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package telemetry

import (
	"context"
	"fmt"
	"github.com/timescale/promscale/pkg/pgmodel/model/pgutf8str"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgtype"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgxconn"
)

// BuildPlatform To fill this variable in build time, use linker flags.
// Example: go build -ldflags="-X github.com/timescale/promscale/pkg/telemetry.BuildPlatform=<any_string>" ./cmd/promscale/
var BuildPlatform string

type engine struct {
	uuid [16]byte
	conn pgxconn.PgxConn

	stop chan struct{}

	metrics sync.Map
}

type Telemetry interface {
	Start()
	// RegisterMetric registers a Prometheus Gauge or Counter metric for telemetry visitor.
	// It must be called after creating the telemetry engine.
	RegisterMetric(columnName string, gaugeOrCounterMetric prometheus.Metric) error
	Stop()
}

func NewTelemetryEngine(conn pgxconn.PgxConn, uuid [16]byte) (Telemetry, error) {
	// Warn the users about telemetry collection.
	log.Warn("msg", "Promscale collects anonymous usage telemetry data to help the Promscale team better understand and assist users. "+
		"This can be disabled via the process described at https://docs.timescale.com/timescaledb/latest/how-to-guides/configuration/telemetry/#disabling-telemetry")
	engine := &engine{
		conn: conn,
		uuid: uuid,
	}
	if err := engine.writeMetadata(); err != nil {
		return nil, fmt.Errorf("writing metadata: %w", err)
	}
	return engine, nil
}

// writeMetadata writes Promscale and Tobs metadata.
func (t *engine) writeMetadata() error {
	promscale, err := promscaleMetadata()
	if err != nil {
		log.Debug("msg", "error fetching complete Promscale metadata", "error", err.Error())
	}
	promscale["build_platform"] = BuildPlatform
	if err := writeToTimescaleMetadataTable(t.conn, Stats(promscale)); err != nil {
		return fmt.Errorf("writing metadata for promscale: %w", err)
	}

	tobs := tobsMetadata()
	if len(tobs) > 0 {
		if err := writeToTimescaleMetadataTable(t.conn, Stats(tobs)); err != nil {
			return fmt.Errorf("writing metadata for tobs: %w", err)
		}
	}
	return nil
}

const (
	metadataUpdateWithExtension = "SELECT update_tsprom_metadata($1, $2, $3)"
	metadataUpdateNoExtension   = "INSERT INTO _timescaledb_catalog.metadata(key, value, include_in_telemetry) VALUES ('promscale_' || $1, $2, $3) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, include_in_telemetry = EXCLUDED.include_in_telemetry"
)

// writeToTimescaleMetadataTable syncs the metadata/stats with telemetry metadata table and returns the last error if any.
func writeToTimescaleMetadataTable(conn pgxconn.PgxConn, m Stats) error {
	update := func(queryFormat string) error {
		batch := conn.NewBatch()
		for key, metadata := range m {
			safe := pgutf8str.Text{}
			if err := safe.Set(metadata); err != nil {
				return fmt.Errorf("setting in pgutf8 safe string: %w", err)
			}
			query := queryFormat
			batch.Queue(query, key, safe, true)
		}

		results, err := conn.SendBatch(context.Background(), batch)
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

	// Try to update via Promscale extension.
	if err := update(metadataUpdateWithExtension); err != nil {
		// Promscale extension not installed. Try to attempt to write directly as a rare attempt
		// in case we fix the _timescaledb_catalog.metadata permissions in the future.
		_ = update(metadataUpdateNoExtension)
	}

	return nil
}

func (t *engine) Stop() {
	t.stop <- struct{}{}
}

func (t *engine) Start() {
	t.stop = make(chan struct{})
	go func() {
		collect := time.NewTicker(time.Minute * 55) // Collect telemetry info every 55 minutes since housekeeper fills the _timescaledb_catalog.metadata table every 1 hour.
		defer collect.Stop()

		for {
			select {
			case <-t.stop:
				close(t.stop)
				return
			case <-collect.C:
			}
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
				log.Debug("msg", "error extracting underlying value of a stats metric", "error", err.Error())
				continue
			}
			if err := syncInfoTable(t.conn, t.uuid, newStats); err != nil {
				log.Debug("msg", "syncing new stats", "error", err.Error())
			}
		}
	}()
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

// RegisterMetric registers a counter or gauge metric for telemetry purpose.
func (t *engine) RegisterMetric(columnName string, metric prometheus.Metric) error {
	if !isCounterOrGauge(metric) {
		return fmt.Errorf("metric not a counter or gauge")
	}
	t.metrics.Store(columnName, metric)
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
func syncInfoTable(conn pgxconn.PgxConn, uuid [16]byte, stats map[string]float64) error {
	lastUpdated := time.Now()

	pgUUID := new(pgtype.UUID)
	if err := pgUUID.Set(uuid); err != nil {
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
	//		ON CONFlICT (uuid) DO
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
	_, err := conn.Exec(context.Background(), query, columnValues...)
	if err != nil {
		return fmt.Errorf("executing telemetry sync query: %w", err)
	}
	return nil
}

type noop struct{}

func NewNoopEngine() Telemetry                              { return noop{} }
func (noop) Start()                                         {}
func (noop) Stop()                                          {}
func (noop) RegisterMetric(string, prometheus.Metric) error { return nil }
