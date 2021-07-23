package querier

import (
	"context"
	"fmt"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
)

type querySamples struct {
	*pgxQuerier
}

func newQuerySamples(qr *pgxQuerier) *querySamples {
	return &querySamples{qr}
}

// Select implements the Querier interface. It is the entry point for our
// own version of the Prometheus engine.
func (q *querySamples) Select(mint, maxt int64, _ bool, hints *storage.SelectHints, qh *QueryHints, path []parser.Node, ms ...*labels.Matcher) (seriesSet SeriesSet, node parser.Node) {
	sampleRows, topNode, err := q.fetchSamplesRows(mint, maxt, hints, qh, path, ms)
	if err != nil {
		return errorSeriesSet{err: fmt.Errorf("fetch samples rows: %w", err)}, nil
	}
	responseSeriesSet := buildSeriesSet(sampleRows, q.tools.labelsReader)
	return responseSeriesSet, topNode
}

func (q *querySamples) fetchSamplesRows(mint, maxt int64, hints *storage.SelectHints, qh *QueryHints, path []parser.Node, ms []*labels.Matcher) ([]sampleRow, parser.Node, error) {
	metadata, err := getEvaluationMetadata(q.tools, mint, maxt, GetPromQLMetadata(ms))
	if err != nil {
		return nil, nil, fmt.Errorf("get evaluation metadata: %w", err)
	}

	// Fill query metadata.
	metadata.promqlMetadata.path = path
	metadata.promqlMetadata.selectHints = hints
	metadata.promqlMetadata.queryHints = qh

	if metadata.isSingleMetric {
		// Single vector selector case.
		tableName, err := q.tools.getMetricTableName(metadata.metric, false)
		if err != nil {
			if err == errors.ErrMissingTableName {
				return nil, nil, nil
			}
			return nil, nil, fmt.Errorf("get metric table name: %w", err)
		}
		metadata.timeFilter.metric = tableName

		sampleRows, topNode, err := fetchSingleMetricSamples(q.tools, metadata)
		if err != nil {
			return nil, nil, fmt.Errorf("fetch single metric samples: %w", err)
		}

		return sampleRows, topNode, nil
	}
	// Multiple vector selector case.
	sampleRows, err := fetchMultipleMetricsSamples(q.tools, metadata)
	if err != nil {
		return nil, nil, fmt.Errorf("fetch multiple metrics samples: %w", err)
	}
	return sampleRows, nil, nil
}

// fetchSingleMetricSamples returns all the result rows for a single metric using the
// query metadata and the tools. It uses the hints and node path to try to push
// down query functions where possible.
func fetchSingleMetricSamples(tools *queryTools, metadata *evalMetadata) ([]sampleRow, parser.Node, error) {
	sqlQuery, values, topNode, tsSeries, err := buildSingleMetricSamplesQuery(metadata)
	if err != nil {
		return nil, nil, err
	}

	rows, err := tools.conn.Query(context.Background(), sqlQuery, values...)
	if err != nil {
		// If we are getting undefined table error, it means the query
		// is looking for a metric which doesn't exist in the system.
		if e, ok := err.(*pgconn.PgError); !ok || e.Code != pgerrcode.UndefinedTable {
			return nil, nil, fmt.Errorf("querying single metric samples: %w", err)
		}
	}
	defer rows.Close()

	samplesRows, err := appendSampleRows(make([]sampleRow, 0, 1), rows, tsSeries)
	if err != nil {
		return nil, topNode, fmt.Errorf("appending sample rows: %w", err)
	}
	return samplesRows, topNode, nil
}

// queryMultipleMetrics returns all the result rows for across multiple metrics
// using the supplied query parameters.
func fetchMultipleMetricsSamples(tools *queryTools, metadata *evalMetadata) ([]sampleRow, error) {
	// First fetch series IDs per metric.
	metrics, series, err := GetMetricNameSeriesIds(tools.conn, metadata)
	if err != nil {
		return nil, fmt.Errorf("get metric-name series-ids: %w", err)
	}

	// TODO this assume on average on row per-metric. Is this right?
	results := make([]sampleRow, 0, len(metrics))
	numQueries := 0
	batch := tools.conn.NewBatch()

	// Generate queries for each metric and send them in a single batch.
	for i := range metrics {
		//TODO batch getMetricTableName
		tableName, err := tools.getMetricTableName(metrics[i], false)
		if err != nil {
			// If the metric table is missing, there are no results for this query.
			if err == errors.ErrMissingTableName {
				continue
			}
			return nil, err
		}
		tFilter := timeFilter{metric: tableName, start: metadata.timeFilter.start, end: metadata.timeFilter.end}
		sqlQuery, err := buildMultipleMetricSamplesQuery(tFilter, series[i])
		if err != nil {
			return nil, fmt.Errorf("build timeseries by series-id: %w", err)
		}
		batch.Queue(sqlQuery)
		numQueries += 1
	}

	batchResults, err := tools.conn.SendBatch(context.Background(), batch)
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
		results, err = appendSampleRows(results, rows, nil)
		rows.Close()
		if err != nil {
			rows.Close()
			return nil, err
		}
	}

	return results, nil
}
