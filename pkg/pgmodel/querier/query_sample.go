package querier

import (
	"context"
	"fmt"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
)

type querySamples struct {
	*pgxQuerier
	ctx context.Context
}

func newQuerySamples(ctx context.Context, qr *pgxQuerier) *querySamples {
	return &querySamples{qr, ctx}
}

// Select implements the SamplesQuerier interface. It is the entry point for our
// own version of the Prometheus engine.
func (q *querySamples) Select(mint, maxt int64, _ bool, hints *storage.SelectHints, qh *QueryHints, path []parser.Node, ms ...*labels.Matcher) (seriesSet SeriesSet, node parser.Node) {
	sampleRows, topNode, err := q.fetchSamplesRows(mint, maxt, hints, qh, path, ms)
	if err != nil {
		return errorSeriesSet{err: err}, nil
	}
	responseSeriesSet := buildSeriesSet(sampleRows, q.tools.labelsReader)
	return responseSeriesSet, topNode
}

func (q *querySamples) fetchSamplesRows(mint, maxt int64, hints *storage.SelectHints, qh *QueryHints, path []parser.Node, ms []*labels.Matcher) ([]sampleRow, parser.Node, error) {
	metadata, err := getEvaluationMetadata(q.tools, mint, maxt, GetPromQLMetadata(ms, hints, qh, path))
	if err != nil {
		return nil, nil, fmt.Errorf("get evaluation metadata: %w", err)
	}

	filter := metadata.timeFilter
	if metadata.isSingleMetric {
		// Single vector selector case.
		mInfo, err := q.tools.getMetricTableName(q.ctx, filter.schema, filter.metric, false)
		if err != nil {
			if err == errors.ErrMissingTableName {
				return nil, nil, nil
			}
			return nil, nil, fmt.Errorf("get metric table name: %w", err)
		}
		metadata.timeFilter.metric = mInfo.TableName
		metadata.timeFilter.schema = mInfo.TableSchema
		metadata.timeFilter.seriesTable = mInfo.SeriesTable

		sampleRows, topNode, err := fetchSingleMetricSamples(q.ctx, q.tools, metadata)
		if err != nil {
			return nil, nil, err
		}

		return sampleRows, topNode, nil
	}
	// Multiple vector selector case.
	sampleRows, err := fetchMultipleMetricsSamples(q.ctx, q.tools, metadata)
	if err != nil {
		return nil, nil, err
	}
	return sampleRows, nil, nil
}

// fetchSingleMetricSamples returns all the result rows for a single metric
// using the query metadata and the tools. It uses the hints and node path to
// try to push down query functions where possible. When a pushdown is
// successfully applied, the new top node is returned together with the metric
// rows. For more information about top nodes, see `engine.populateSeries`.
func fetchSingleMetricSamples(ctx context.Context, tools *queryTools, metadata *evalMetadata) ([]sampleRow, parser.Node, error) {
	sqlQuery, values, topNode, tsSeries, err := buildSingleMetricSamplesQuery(metadata)
	if err != nil {
		return nil, nil, err
	}

	rows, err := tools.conn.Query(ctx, sqlQuery, values...)
	if err != nil {
		if e, ok := err.(*pgconn.PgError); ok {
			switch e.Code {
			case pgerrcode.UndefinedTable:
				// If we are getting undefined table error, it means the metric we are trying to query
				// existed at some point but the underlying relation was removed from outside of the system.
				return nil, nil, fmt.Errorf(errors.ErrTmplMissingUnderlyingRelation, metadata.timeFilter.schema, metadata.timeFilter.metric)
			case pgerrcode.UndefinedColumn:
				// If we are getting undefined column error, it means the column we are trying to query
				// does not exist in the metric table so we return empty results.
				// Empty result is more consistent and in-line with PromQL assumption of a missing series based on matchers.
				return nil, nil, nil
			}
		}
		return nil, nil, err
	}
	defer rows.Close()

	updatedMetricName := ""
	// If the table name and series table name don't match, this is a custom metric view which
	// shares the series table with the raw metric, hence we have to update the metric name label.
	if metadata.timeFilter.metric != metadata.timeFilter.seriesTable {
		updatedMetricName = metadata.timeFilter.metric
	}

	filter := metadata.timeFilter
	samplesRows, err := appendSampleRows(make([]sampleRow, 0, 1), rows, tsSeries, updatedMetricName, filter.schema, filter.column)
	if err != nil {
		return nil, topNode, fmt.Errorf("appending sample rows: %w", err)
	}
	return samplesRows, topNode, nil
}

// fetchMultipleMetricsSamples returns all the result rows for across multiple
// metrics using the supplied query parameters.
func fetchMultipleMetricsSamples(ctx context.Context, tools *queryTools, metadata *evalMetadata) ([]sampleRow, error) {
	// First fetch series IDs per metric.
	metrics, schemas, series, err := GetMetricNameSeriesIds(ctx, tools.conn, metadata)
	if err != nil {
		return nil, err
	}

	// TODO this assume on average on row per-metric. Is this right?
	results := make([]sampleRow, 0, len(metrics))
	numQueries := 0
	batch := tools.conn.NewBatch()

	// Generate queries for each metric and send them in a single batch.
	for i := range metrics {
		//TODO batch getMetricTableName
		metricInfo, err := tools.getMetricTableName(ctx, schemas[i], metrics[i], false)
		if err != nil {
			// If the metric table is missing, there are no results for this query.
			if err == errors.ErrMissingTableName {
				continue
			}
			return nil, err
		}

		// We only support default data schema for multi-metric queries
		// NOTE: this needs to be updated once we add support for storing
		// non-view metrics into multiple schemas
		if metricInfo.TableSchema != schema.PromData {
			return nil, fmt.Errorf("found unsupported metric schema in multi-metric matching query")
		}

		filter := timeFilter{
			metric:      metricInfo.TableName,
			schema:      metricInfo.TableSchema,
			seriesTable: metricInfo.SeriesTable,
			start:       metadata.timeFilter.start,
			end:         metadata.timeFilter.end,
		}
		sqlQuery, err := buildMultipleMetricSamplesQuery(filter, series[i])
		if err != nil {
			return nil, fmt.Errorf("build timeseries by series-id: %w", err)
		}
		batch.Queue(sqlQuery)
		numQueries += 1
	}

	batchResults, err := tools.conn.SendBatch(ctx, batch)
	if err != nil {
		return nil, err
	}
	defer batchResults.Close()

	for i := 0; i < numQueries; i++ {
		rows, err := batchResults.Query()
		if err != nil {
			rows.Close()
			return nil, err
		}
		// Append all rows into results.
		results, err = appendSampleRows(results, rows, nil, "", "", "")
		rows.Close()
		if err != nil {
			rows.Close()
			return nil, err
		}
	}

	return results, nil
}
