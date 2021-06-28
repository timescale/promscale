package querier

import (
	"encoding/binary"
	"sync"

	"github.com/jackc/pgtype"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgxconn"
)

var fPool = sync.Pool{
	New: func() interface{} {
		return new(pgtype.Float8Array)
	},
}

var tPool = sync.Pool{
	New: func() interface{} {
		return new(pgtype.TimestamptzArray)
	},
}

//wrapper to allow DecodeBinary to reuse the existing array so that a pool is effective
type timestamptzArrayWrapper struct {
	*pgtype.TimestamptzArray
}

func (dstwrapper *timestamptzArrayWrapper) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	dst := dstwrapper.TimestamptzArray
	if src == nil {
		*dst = pgtype.TimestamptzArray{Status: pgtype.Null}
		return nil
	}

	var arrayHeader pgtype.ArrayHeader
	rp, err := arrayHeader.DecodeBinary(ci, src)
	if err != nil {
		return err
	}

	if len(arrayHeader.Dimensions) == 0 {
		*dst = pgtype.TimestamptzArray{Dimensions: arrayHeader.Dimensions, Status: pgtype.Present}
		return nil
	}

	elementCount := arrayHeader.Dimensions[0].Length
	for _, d := range arrayHeader.Dimensions[1:] {
		elementCount *= d.Length
	}

	//reuse logic
	elements := dst.Elements
	if cap(dst.Elements) < int(elementCount) {
		elements = make([]pgtype.Timestamptz, elementCount)
	} else {
		elements = elements[:elementCount]
	}

	for i := range elements {
		elemLen := int(int32(binary.BigEndian.Uint32(src[rp:])))
		rp += 4
		var elemSrc []byte
		if elemLen >= 0 {
			elemSrc = src[rp : rp+elemLen]
			rp += elemLen
		}
		err = elements[i].DecodeBinary(ci, elemSrc)
		if err != nil {
			return err
		}
	}

	*dst = pgtype.TimestamptzArray{Elements: elements, Dimensions: arrayHeader.Dimensions, Status: pgtype.Present}
	return nil
}

//wrapper to to allow DecodeBinary to reuse existing array so that a pool is effective
type float8ArrayWrapper struct {
	*pgtype.Float8Array
}

func (dstwrapper *float8ArrayWrapper) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	dst := dstwrapper.Float8Array
	if src == nil {
		*dst = pgtype.Float8Array{Status: pgtype.Null}
		return nil
	}

	var arrayHeader pgtype.ArrayHeader
	rp, err := arrayHeader.DecodeBinary(ci, src)
	if err != nil {
		return err
	}

	if len(arrayHeader.Dimensions) == 0 {
		*dst = pgtype.Float8Array{Dimensions: arrayHeader.Dimensions, Status: pgtype.Present}
		return nil
	}

	elementCount := arrayHeader.Dimensions[0].Length
	for _, d := range arrayHeader.Dimensions[1:] {
		elementCount *= d.Length
	}

	//reuse logic
	elements := dst.Elements
	if cap(dst.Elements) < int(elementCount) {
		elements = make([]pgtype.Float8, elementCount)
	} else {
		elements = elements[:elementCount]
	}

	for i := range elements {
		elemLen := int(int32(binary.BigEndian.Uint32(src[rp:])))
		rp += 4
		var elemSrc []byte
		if elemLen >= 0 {
			elemSrc = src[rp : rp+elemLen]
			rp += elemLen
		}
		err = elements[i].DecodeBinary(ci, elemSrc)
		if err != nil {
			return err
		}
	}

	*dst = pgtype.Float8Array{Elements: elements, Dimensions: arrayHeader.Dimensions, Status: pgtype.Present}
	return nil
}

type timescaleRow struct {
	labelIds []int64
	times    TimestampSeries
	values   *pgtype.Float8Array
	err      error
	metric   string
	schema   string

	//only used to hold ownership for releasing to pool
	timeArrayOwnership *pgtype.TimestamptzArray
}

func (r *timescaleRow) Close() {
	if r.timeArrayOwnership != nil {
		tPool.Put(r.timeArrayOwnership)
	}
	fPool.Put(r.values)
}

// appendTsRows adds new results rows to already existing result rows and
// returns the as a result.
func appendTsRows(out []timescaleRow, in pgxconn.PgxRows, tsSeries TimestampSeries, schema, metric string) ([]timescaleRow, error) {
	if in.Err() != nil {
		return out, in.Err()
	}
	for in.Next() {
		var row timescaleRow
		values := fPool.Get().(*pgtype.Float8Array)
		values.Elements = values.Elements[:0]
		valuesWrapper := float8ArrayWrapper{values}

		//if a timeseries isn't provided it will be fetched from the database
		if tsSeries == nil {
			times := tPool.Get().(*pgtype.TimestamptzArray)
			times.Elements = times.Elements[:0]
			timesWrapper := timestamptzArrayWrapper{times}
			row.err = in.Scan(&row.labelIds, &timesWrapper, &valuesWrapper)
			row.timeArrayOwnership = times
			row.times = newRowTimestampSeries(times)
		} else {
			row.err = in.Scan(&row.labelIds, &valuesWrapper)
			row.times = tsSeries
		}

		row.values = values
		row.schema = schema
		row.metric = metric

		out = append(out, row)
		if row.err != nil {
			log.Error("err", row.err)
			return out, row.err
		}
	}
	return out, in.Err()
}
