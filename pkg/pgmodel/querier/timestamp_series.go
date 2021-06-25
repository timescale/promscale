package querier

import (
	"time"

	"github.com/jackc/pgtype"
	prommodel "github.com/prometheus/common/model"
	"github.com/timescale/promscale/pkg/pgmodel/model"
)

type TimestampSeries interface {
	At(index int) int64
	Len() int
}

type rowTimestampSeries struct {
	times *pgtype.TimestamptzArray
}

func NewRowTimestampSeries(times *pgtype.TimestamptzArray) *rowTimestampSeries {
	return &rowTimestampSeries{times: times}
}

func (t *rowTimestampSeries) At(index int) int64 {
	return model.TimestamptzToMs(t.times.Elements[index])
}

func (t *rowTimestampSeries) Len() int {
	return len(t.times.Elements)
}

type regularTimestampSeries struct {
	start time.Time
	end   time.Time
	step  time.Duration
	len   int
}

func NewRegularTimestampSeries(start time.Time, end time.Time, step time.Duration) *regularTimestampSeries {
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

func (t *regularTimestampSeries) At(index int) int64 {
	time := t.start.Add(time.Duration(index) * t.step)
	return int64(prommodel.TimeFromUnixNano(time.UnixNano()))
}
