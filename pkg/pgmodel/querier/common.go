package querier

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
)

const (
	getMetricTableSQL         = "SELECT id, table_schema, table_name, series_table FROM _prom_catalog.get_metric_table_name_if_exists($1, $2)"
	getExemplarMetricTableSQL = "SELECT COALESCE(table_name, '') FROM _prom_catalog.exemplar WHERE metric_name=$1"
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

func GetMetricNameSeriesIds(conn pgxconn.PgxConn, metadata *evalMetadata) (metrics, schemas []string, correspondingSeriesIDs [][]model.SeriesID, err error) {
	sqlQuery := buildMetricNameSeriesIDQuery(metadata.clauses)
	rows, err := conn.Query(context.Background(), sqlQuery, metadata.values...)
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()

	metrics, schemas, correspondingSeriesIDs, err = getSeriesPerMetric(rows)
	if err != nil {
		return nil, nil, nil, err
	}
	return
}

func getSeriesPerMetric(rows pgxconn.PgxRows) ([]string, []string, [][]model.SeriesID, error) {
	metrics := make([]string, 0)
	schemas := make([]string, 0)
	series := make([][]model.SeriesID, 0)

	for rows.Next() {
		var (
			metricName string
			schemaName string
			seriesIDs  []int64
		)
		if err := rows.Scan(&schemaName, &metricName, &seriesIDs); err != nil {
			return nil, nil, nil, err
		}

		sIDs := make([]model.SeriesID, 0, len(seriesIDs))

		for _, v := range seriesIDs {
			sIDs = append(sIDs, model.SeriesID(v))
		}

		metrics = append(metrics, metricName)
		schemas = append(schemas, schemaName)
		series = append(series, sIDs)
	}

	return metrics, schemas, series, nil
}

func toMilis(t time.Time) int64 {
	return t.UnixNano() / 1e6
}
