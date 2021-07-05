package querier

import (
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

type seriesRow struct {
	labelIds []int64
	times    pgtype.TimestamptzArray
	values   pgtype.Float8Array
	err      error
}

// appendTsRows adds new results rows to already existing result rows and
// returns the as a result.
func appendSampleRows(out []seriesRow, in pgx.Rows) ([]seriesRow, error) {
	if in.Err() != nil {
		return out, in.Err()
	}
	for in.Next() {
		var row seriesRow
		row.err = in.Scan(&row.labelIds, &row.times, &row.values)
		out = append(out, row)
		if row.err != nil {
			return out, row.err
		}
	}
	return out, in.Err()
}
