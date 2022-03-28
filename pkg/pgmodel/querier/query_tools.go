package querier

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	pgmodelcommon "github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/lreader"
	"github.com/timescale/promscale/pkg/pgmodel/model"
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
func (tools *queryTools) getMetricTableName(metricSchema, metricName string, isExemplarQuery bool) (model.MetricInfo, error) {
	metricInfo, err := tools.metricTableNames.Get(metricSchema, metricName, isExemplarQuery)
	if err == nil {
		return metricInfo, nil
	}
	if !errors.Is(err, pgmodelcommon.ErrEntryNotFound) {
		return model.MetricInfo{}, fmt.Errorf("fetching metric info from cache: %w", err)
	}

	if isExemplarQuery {
		// The incoming query is for exemplar data. Let's change our parameters
		// so that the operations with the database and cache is focused towards
		// exemplars.
		tableName, err := queryExemplarMetricTableName(tools.conn, metricName)
		if err != nil {
			return model.MetricInfo{}, err
		}
		metricInfo = model.MetricInfo{TableSchema: schema.PromDataExemplar, TableName: tableName}
	} else {
		metricInfo, err = querySampleMetricTableName(tools.conn, metricSchema, metricName)
		if err != nil {
			return model.MetricInfo{}, err
		}
	}

	err = tools.metricTableNames.Set(metricSchema, metricName, metricInfo, isExemplarQuery)
	return metricInfo, err
}

// queryExemplarMetricTableName returns table name for exemplars for the given metric.
func queryExemplarMetricTableName(conn pgxconn.PgxConn, metric string) (string, error) {
	res, err := conn.Query(context.Background(), getExemplarMetricTableSQL, metric)
	if err != nil {
		return "", err
	}

	var tableName string
	defer res.Close()

	if !res.Next() {
		return "", pgmodelcommon.ErrMissingTableName
	}

	if err := res.Scan(&tableName); err != nil {
		return "", fmt.Errorf("scanning fetched exemplars table name: %w", err)
	}

	return tableName, nil
}

func querySampleMetricTableName(conn pgxconn.PgxConn, schema, metric string) (mInfo model.MetricInfo, err error) {
	row := conn.QueryRow(
		context.Background(),
		getMetricTableSQL,
		schema,
		metric,
	)

	if err = row.Scan(&mInfo.MetricID, &mInfo.TableSchema, &mInfo.TableName, &mInfo.SeriesTable); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			err = pgmodelcommon.ErrMissingTableName
		}
		return mInfo, err
	}

	return mInfo, nil
}
