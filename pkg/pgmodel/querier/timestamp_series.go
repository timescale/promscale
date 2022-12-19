package querier

import (
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	prommodel "github.com/prometheus/common/model"
	"github.com/timescale/promscale/pkg/pgmodel/model"
)

// TimestampSeries represent an array of timestamps (model.Time/int64) that is 0-indexed.
type TimestampSeries interface {
	//At returns the element at an index location, as well as a bool to indicate
	//whether the value is valid (or NULL for example)
	At(index int) (int64, bool)
	Len() int
}

// rowTimestampSeries is a TimestampSeries based on data fetched from a database row
type rowTimestampSeries struct {
	times *model.ReusableArray[pgtype.Timestamptz]
}

func newRowTimestampSeries(times *model.ReusableArray[pgtype.Timestamptz]) *rowTimestampSeries {
	return &rowTimestampSeries{times: times}
}

func (t *rowTimestampSeries) At(index int) (int64, bool) {
	return model.TimestamptzToMs(t.times.FlatArray[index]), t.times.FlatArray[index].Valid
}

func (t *rowTimestampSeries) Len() int {
	return len(t.times.FlatArray)
}

// regularTimestampSeries represents a time-series that is regular (e.g. each timestamp is step duration ahead of the previous one)
type regularTimestampSeries struct {
	start time.Time
	end   time.Time
	step  time.Duration
	len   int
}

func newRegularTimestampSeries(start time.Time, end time.Time, step time.Duration) *regularTimestampSeries {
	len := (end.Sub(start) / step) + 1
	return &regularTimestampSeries{
		start: start,
		end:   end,
		step:  step,
		len:   int(len),
	}
}

func (t *regularTimestampSeries) Len() int {
	return t.len
}

func (t *regularTimestampSeries) At(index int) (int64, bool) {
	time := t.start.Add(time.Duration(index) * t.step)
	return int64(prommodel.TimeFromUnixNano(time.UnixNano())), true
}
