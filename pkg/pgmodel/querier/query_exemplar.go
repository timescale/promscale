package querier

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/model"
)

type queryExemplars struct {
	*pgxQuerier
}

func newQueryExemplars(qr *pgxQuerier) *queryExemplars {
	return &queryExemplars{qr}
}

func (q *queryExemplars) Select(start, end time.Time, matchersList ...[]*labels.Matcher) ([]model.ExemplarQueryResult, error) {
	results := make([]model.ExemplarQueryResult, 0, len(matchersList))
	evaluatedMatchers := make(map[string]struct{})
	for _, matchers := range matchersList {
		matcherStr := fmt.Sprintf("%v", matchers)
		if _, seenPreviously := evaluatedMatchers[matcherStr]; seenPreviously {
			continue
		}
		evaluatedMatchers[matcherStr] = struct{}{}
		metadata, err := getEvaluationMetadata(q.tools, timestamp.FromTime(start), timestamp.FromTime(end), GetPromQLMetadata(matchers, nil, nil, nil))
		if err != nil {
			return nil, fmt.Errorf("get evaluation metadata: %w", err)
		}

		metadata.isExemplarQuery = true

		if metadata.isSingleMetric {
			metricInfo, err := q.tools.getMetricTableName("", metadata.metric, true)
			if err != nil {
				if err == errors.ErrMissingTableName {
					// The received metric does not have exemplars. Skip the remaining part and continue with
					// the next matchers.
					continue
				}
				return nil, fmt.Errorf("get metric table name: %w", err)
			}
			metadata.timeFilter.metric = metricInfo.TableName

			exemplarRows, err := fetchSingleMetricExemplars(q.tools, metadata)
			if err != nil {
				return nil, fmt.Errorf("fetch single metric exemplars: %w", err)
			}

			for i := range exemplarRows {
				exemplars, err := prepareExemplarQueryResult(q.tools, exemplarRows[i])
				if err != nil {
					return nil, fmt.Errorf("prepare exemplar result: %w", err)
				}
				results = append(results, exemplars)
			}
			continue
		}
		// Multiple metric exemplar query.
		exemplarRows, err := fetchMultipleMetricsExemplars(q.tools, metadata)
		if err != nil {
			return nil, fmt.Errorf("fetch multiple metrics exemplars: %w", err)
		}
		for i := range exemplarRows {
			exemplars, err := prepareExemplarQueryResult(q.tools, exemplarRows[i])
			if err != nil {
				return nil, fmt.Errorf("prepare exemplar result: %w", err)
			}
			results = append(results, exemplars)
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return labels.Compare(results[i].SeriesLabels, results[j].SeriesLabels) < 0
	})
	return results, nil
}

// fetchSingleMetricSamples returns all the result rows for a single metric using the
// query metadata and the tools. It uses the hints and node path to try to push
// down query functions where possible.
func fetchSingleMetricExemplars(tools *queryTools, metadata *evalMetadata) ([]exemplarSeriesRow, error) {
	sqlQuery := buildSingleMetricExemplarsQuery(metadata)

	rows, err := tools.conn.Query(context.Background(), sqlQuery)
	if err != nil {
		// If we are getting undefined table error, it means the query
		// is looking for a metric which doesn't exist in the system.
		if e, ok := err.(*pgconn.PgError); !ok || e.Code != pgerrcode.UndefinedTable {
			return nil, fmt.Errorf("querying single metric exemplars: %w", err)
		}
	}
	defer rows.Close()

	exemplarSeriesRows, err := getExemplarSeriesRows(metadata.metric, rows)
	if err != nil {
		return nil, fmt.Errorf("appending exemplar rows: %w", err)
	}
	return exemplarSeriesRows, nil
}

// queryMultipleMetrics returns all the result rows for across multiple metrics
// using the supplied query parameters.
func fetchMultipleMetricsExemplars(tools *queryTools, metadata *evalMetadata) ([]exemplarSeriesRow, error) {
	// First fetch series IDs per metric.
	metrics, _, correspondingSeriesIds, err := GetMetricNameSeriesIds(tools.conn, metadata)
	if err != nil {
		return nil, fmt.Errorf("get metric-name series-ids: %w", err)
	}

	var (
		results          = make([]exemplarSeriesRow, 0, len(metrics))
		metricTableNames = make([]string, 0, len(metrics)) // This will be required to fill the `metricName` field of exemplarResult.
	)

	numQueries := 0
	batch := tools.conn.NewBatch()

	// Generate queries for each metric and send them in a single batch.
	for i := range metrics {
		//TODO batch getMetricTableName
		metricInfo, err := tools.getMetricTableName("", metrics[i], true)
		if err != nil {
			if err == errors.ErrMissingTableName {
				// If the metric table is missing, there are no results for this query.
				continue
			}
			return nil, err
		}
		tFilter := timeFilter{
			metric: metricInfo.TableName,
			start:  metadata.timeFilter.start,
			end:    metadata.timeFilter.end,
		}
		sqlQuery, err := buildMultipleMetricExemplarsQuery(tFilter, correspondingSeriesIds[i])
		if err != nil {
			return nil, fmt.Errorf("build timeseries by series-id: %w", err)
		}
		metricTableNames = append(metricTableNames, metricInfo.TableName)
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
		metricTable := metricTableNames[i]
		exemplarSeriesRows, err := getExemplarSeriesRows(metricTable, rows)
		if err != nil {
			return nil, fmt.Errorf("append exemplar rows: %w", err)
		}
		results = append(results, exemplarSeriesRows...)
		// Can't defer because we need to Close before the next loop iteration.
		rows.Close()
	}

	return results, nil
}
