package querier

import (
	"sync"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
)

// QUESTION(adn) should we cache only the underlying array like it was before?
var fPool = sync.Pool{
	New: func() interface{} {
		return new(model.ReusableArray[pgtype.Float8])
	},
}

var tPool = sync.Pool{
	New: func() interface{} {
		return new(model.ReusableArray[pgtype.Timestamptz])
	},
}

type sampleRow struct {
	labelIds       []*int64
	times          TimestampSeries
	values         *model.ReusableArray[pgtype.Float8]
	err            error
	metricOverride string
	schema         string
	column         string

	//only used to hold ownership for releasing to pool
	timeArrayOwnership *model.ReusableArray[pgtype.Timestamptz]
}

func (r *sampleRow) Close() {
	if r.timeArrayOwnership != nil {
		tPool.Put(r.timeArrayOwnership)
	}
	fPool.Put(r.values)
}

func (r *sampleRow) GetAdditionalLabels() (ll labels.Labels) {
	if r.schema != "" && r.schema != schema.PromData {
		ll = append(ll, labels.Label{Name: model.SchemaNameLabelName, Value: r.schema})
	}
	if r.column != "" && r.column != defaultColumnName {
		ll = append(ll, labels.Label{Name: model.ColumnNameLabelName, Value: r.column})
	}
	return ll
}

// appendSampleRows adds new results rows to already existing result rows and
// returns the as a result.
func appendSampleRows(out []sampleRow, in pgxconn.PgxRows, tsSeries TimestampSeries, metric, schema, column string) ([]sampleRow, error) {
	if in.Err() != nil {
		return out, in.Err()
	}
	for in.Next() {
		var row sampleRow
		values := fPool.Get().(*model.ReusableArray[pgtype.Float8])
		if values.FlatArray != nil {
			values.FlatArray = values.FlatArray[:0]
		}

		var labelIds []*int64
		//if a timeseries isn't provided it will be fetched from the database
		if tsSeries == nil {
			times := tPool.Get().(*model.ReusableArray[pgtype.Timestamptz])
			if times.FlatArray != nil {
				times.FlatArray = times.FlatArray[:0]
			}
			row.err = in.Scan(&labelIds, times, values)
			row.timeArrayOwnership = times
			row.times = newRowTimestampSeries(times)
		} else {
			row.err = in.Scan(&labelIds, values)
			row.times = tsSeries
		}

		// TODO
		row.labelIds = labelIds
		row.values = values
		row.metricOverride = metric
		row.schema = schema
		row.column = column

		out = append(out, row)
		if row.err != nil {
			log.Error("err", row.err)
			return out, row.err
		}
	}
	return out, in.Err()
}
