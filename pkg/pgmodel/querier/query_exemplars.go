package querier

import (
	"github.com/timescale/promscale/pkg/pgmodel/model"
)

func (q *pgxQuerier) getExemplars(mint, maxt int64, metrics []string, seriesIDs [][]model.SeriesID) {

}

//func getMetricsContainingExemplars(conn pgxconn.PgxConn, metrics []string) []string {
//	var metricsContainingExemplars []string
//
//}
