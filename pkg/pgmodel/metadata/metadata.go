// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package metadata

import (
	"context"
	"fmt"
	"strings"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgmodel/querier"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const (
	metricMetadata = "SELECT metric_family, type, unit, help from " + schema.Catalog + ".metadata"
	middle         = " ORDER BY last_seen"
)

func MetricMetadata(conn pgxconn.PgxConn, metric string, limit int) (map[string][]model.Metadata, error) {
	query := metricMetadata
	if metric != "" {
		query += fmt.Sprintf(" WHERE metric_family='%s'", metric)
	}
	query += middle
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("query metric metadata: %w", err)
	}
	defer rows.Close()
	metadata := make(map[string][]model.Metadata)
	for rows.Next() {
		var metricFamily, typ, unit, help string
		if err := rows.Scan(&metricFamily, &typ, &unit, &help); err != nil {
			return nil, fmt.Errorf("query result: %w", err)
		}
		metadata[metricFamily] = append(metadata[metricFamily], model.Metadata{
			Unit: unit,
			Type: typ,
			Help: help,
		})
	}
	return metadata, nil
}

type Target struct {
	Instance string `json:"instance"`
	Job      string `json:"job"`
}

type TargetMetadataType struct {
	Target Target `json:"target"`
	model.Metadata
}

func TargetMetadata(conn pgxconn.PgxConn, matchers []*labels.Matcher, metric string, targetLimit int) ([]TargetMetadataType, error) {
	var metadataSlice []TargetMetadataType
	metrics, seriesIds, err := querier.GetMetricNameSeriesIDFromMatchers(conn, matchers)
	if err != nil {
		return nil, fmt.Errorf("fetch metric-name series-ids: %w", err)
	}
	var data []model.Metadata
	if metric == "" {
		// Even if seriesLimit is applied, we still need to query all metric metadata since we never know how many unique
		// targets are there in 'metrics'.
		//
		// Example: Even if the limit is 2, we still have to query all metadata, since the first 'metrics' can contain 10 'seriesIds'
		// but they all may correspond to a single target only. Hence, if we go by estimation of targets using len of 'seriesIds',
		// the request of targetLimit 2 will be responded wrongly.
		rows, err := conn.Query(context.Background(), "SELECT metric_family, type, unit, help from "+schema.Catalog+".metadata group by metric_family, type, unit, help, last_seen order by last_seen desc")
		if err != nil {
			return nil, fmt.Errorf("fetching metadata: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var metricFamily, typ, unit, help string
			if err := rows.Scan(&metricFamily, &typ, &unit, &help); err != nil {
				return nil, fmt.Errorf("query result: %w", err)
			}
			data = append(data, model.Metadata{
				MetricFamily: metricFamily,
				Unit:         unit,
				Type:         typ,
				Help:         help,
			})
		}
		var targetInfo map[string]map[Target]struct{}
		targetInfo, err = getTargetInfo(conn, metrics, seriesIds)
		if err != nil {
			return nil, fmt.Errorf("get target info: %w", err)
		}
		for i := range data {
			meta := data[i]
			metricName := meta.MetricFamily
			targetInfos := targetInfo[metricName]
			for j := range targetInfos {
				if targetLimit != 0 && len(metadataSlice) >= targetLimit {
					break
				}
				metadataSlice = append(metadataSlice, TargetMetadataType{
					Target:   j,
					Metadata: meta,
				})
			}
		}
	} else {
		var metricFamily, typ, unit, help string
		row := conn.QueryRow(context.Background(), "SELECT metric_family, type, unit, help from "+schema.Catalog+".metadata WHERE metric_family=$1 order by last_seen desc limit 1", metric)
		if err := row.Scan(&metricFamily, &typ, &unit, &help); err != nil {
			return nil, fmt.Errorf("query result: %w", err)
		}
		meta := model.Metadata{
			// We do not have to send metric name, as its already asked in the params. This
			// is to keep the behaviour consistent with Prometheus.
			Unit: unit,
			Type: typ,
			Help: help,
		}
		seriesId := getSeriesIdsForMetric(metric, metrics, seriesIds)
		if len(seriesIds) == 0 {
			// No series exists, meaning we cannot send any target info.
			// Hence, just send the metadata about the metric.
			metadataSlice = append(metadataSlice, TargetMetadataType{
				Metadata: meta,
			})
			return metadataSlice, nil
		}
		targetInfo, err := getTargetInfo(conn, []string{metric}, [][]model.SeriesID{seriesId})
		if err != nil {
			return nil, fmt.Errorf("get target info: %w", err)
		}
		targetInfos := targetInfo[metricFamily]
		for j := range targetInfos {
			if targetLimit != 0 && len(metadataSlice) >= targetLimit {
				break
			}
			metadataSlice = append(metadataSlice, TargetMetadataType{
				Target:   j,
				Metadata: meta,
			})
		}
	}
	return metadataSlice, nil
}

func getSeriesIdsForMetric(metric string, metrics []string, seriesIds [][]model.SeriesID) []model.SeriesID {
	for i := range metrics {
		if metrics[i] == metric {
			return seriesIds[i]
		}
	}
	// Corresponding series id does not exists.
	return []model.SeriesID{}
}

func getTargetInfo(conn pgxconn.PgxConn, metrics []string, seriesIds [][]model.SeriesID) (map[string]map[Target]struct{}, error) {
	metricTargets := make(map[string]map[Target]struct{})
	batch := conn.NewBatch()
	for i := range metrics {
		batch.Queue("SELECT * from prom_api.get_targets($1, $2::BIGINT[])", metrics[i], convertSeriesIDsToInt64s(seriesIds[i]))
	}
	result, err := conn.SendBatch(context.Background(), batch)
	if err != nil {
		return nil, fmt.Errorf("batching target query: %w", err)
	}
	defer result.Close()
	for i := range metrics {
		rows, err := result.Query()
		if err != nil {
			if strings.Contains(err.Error(), "does not exist") {
				// The metric does not exists. This means the metadata was queried before inserting into the db. Hence, we skip this.
				continue
			}
			return nil, fmt.Errorf("querying target data: %w", err)
		}
		metricTargets[metrics[i]] = make(map[Target]struct{})
		for rows.Next() {
			var (
				seriesId     model.SeriesID
				targetKeys   []string
				targetValues []string
			)
			err := rows.Scan(&seriesId, &targetKeys, &targetValues)
			if err != nil {
				return nil, fmt.Errorf("scanning target data: %w", err)
			}
			var t Target
			for j, k := range targetKeys {
				switch k {
				case "job":
					t.Job = targetValues[j]
				case "instance":
					t.Instance = targetValues[j]
				}
			}
			metricTargets[metrics[i]][t] = struct{}{} // Maintain all unique targets of a metric.
		}
	}
	return metricTargets, nil
}

func convertSeriesIDsToInt64s(s []model.SeriesID) []int64 {
	temp := make([]int64, len(s))
	for i := range s {
		temp[i] = int64(s[i])
	}
	return temp
}
