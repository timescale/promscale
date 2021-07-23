package querier

import (
	"context"
	"fmt"

	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/lreader"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/tenancy"
)

type queryTools struct {
	conn             pgxconn.PgxConn
	metricTableNames cache.MetricCache
	exemplarPosCache cache.PositionCache
	labelsReader     lreader.LabelsReader
	rAuth            tenancy.ReadAuthorizer
}

// getMetricTableName gets the table name for a specific metric from internal
// cache. If not found, fetches it from the database and updates the cache.
func (tools *queryTools) getMetricTableName(metricName string, isExemplarQuery bool) (string, error) {
	tableFetchQuery := getSampleMetricTableSQL
	if isExemplarQuery {
		// The incoming query is for exemplar data. Let's change our parameters
		// so that the operations with the database and cache is focused towards
		// exemplars.
		tableFetchQuery = getExemplarMetricTableSQL
	}

	tableName, err := tools.metricTableNames.Get(metricName, isExemplarQuery)
	if err == nil {
		return tableName, nil
	}
	if err != errors.ErrEntryNotFound {
		return "", err
	}

	tableName, err = queryMetricTableName(tools.conn, metricName, tableFetchQuery)
	if err != nil {
		return "", err
	}

	err = tools.metricTableNames.Set(metricName, tableName, isExemplarQuery)
	return tableName, err
}

// queryMetricTableName returns table name for the given metric by evaluating the query passed as param.
// If you want the table name to be for samples, pass getSampleMetricTableSQL as query.
// If you want the table name to be for exemplars, pass getExemplarMetricTableSQL as query.
func queryMetricTableName(conn pgxconn.PgxConn, metric, query string) (string, error) {
	res, err := conn.Query(context.Background(), query, metric)
	if err != nil {
		return "", err
	}

	var tableName string
	defer res.Close()

	if !res.Next() {
		return "", errors.ErrMissingTableName
	}

	if err := res.Scan(&tableName); err != nil {
		return "", fmt.Errorf("scanning fetched table name: %w", err)
	}

	return tableName, nil
}
