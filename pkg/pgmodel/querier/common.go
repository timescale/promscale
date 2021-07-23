package querier

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
)

const (
	getSampleMetricTableSQL   = "SELECT table_name FROM " + schema.Catalog + ".get_metric_table_name_if_exists($1)"
	getExemplarMetricTableSQL = "SELECT COALESCE(table_name, '') FROM " + schema.Catalog + ".exemplar WHERE metric_name=$1"
)

// fromLabelMatchers parses protobuf label matchers to Prometheus label matchers.
func fromLabelMatchers(matchers []*prompb.LabelMatcher) ([]*labels.Matcher, error) {
	result := make([]*labels.Matcher, 0, len(matchers))
	for _, matcher := range matchers {
		var mtype labels.MatchType
		switch matcher.Type {
		case prompb.LabelMatcher_EQ:
			mtype = labels.MatchEqual
		case prompb.LabelMatcher_NEQ:
			mtype = labels.MatchNotEqual
		case prompb.LabelMatcher_RE:
			mtype = labels.MatchRegexp
		case prompb.LabelMatcher_NRE:
			mtype = labels.MatchNotRegexp
		default:
			return nil, fmt.Errorf("invalid matcher type")
		}
		matcher, err := labels.NewMatcher(mtype, matcher.Name, matcher.Value)
		if err != nil {
			return nil, err
		}
		result = append(result, matcher)
	}
	return result, nil
}

type QueryHints struct {
	StartTime   time.Time
	EndTime     time.Time
	CurrentNode parser.Node
	Lookback    time.Duration
}

func GetMetricNameSeriesIds(conn pgxconn.PgxConn, metadata *evalMetadata) (metrics []string, correspondingSeriesIDs [][]model.SeriesID, err error) {
	sqlQuery := buildMetricNameSeriesIDQuery(metadata.clauses)
	rows, err := conn.Query(context.Background(), sqlQuery, metadata.values...)
	if err != nil {
		return nil, nil, fmt.Errorf("querying metric-name series-ids: %w", err)
	}
	defer rows.Close()

	metrics, correspondingSeriesIDs, err = getSeriesPerMetric(rows)
	if err != nil {
		return nil, nil, fmt.Errorf("get series per metric: %w", err)
	}
	return
}

func getSeriesPerMetric(rows pgxconn.PgxRows) ([]string, [][]model.SeriesID, error) {
	metrics := make([]string, 0)
	series := make([][]model.SeriesID, 0)

	for rows.Next() {
		var (
			metricName string
			seriesIDs  []int64
		)
		if err := rows.Scan(&metricName, &seriesIDs); err != nil {
			return nil, nil, err
		}

		sIDs := make([]model.SeriesID, 0, len(seriesIDs))

		for _, v := range seriesIDs {
			sIDs = append(sIDs, model.SeriesID(v))
		}

		metrics = append(metrics, metricName)
		series = append(series, sIDs)
	}

	return metrics, series, nil
}
