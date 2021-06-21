package pglabelvaluearr

import (
	"fmt"
	"github.com/jackc/pgtype"
	"reflect"
)

type PgLabelValueArray struct {
	a    pgtype.Status // NULL will cause decoding error
	data pgtype.TextArray
}

func (dst *PgLabelValueArray) Set(src interface{}) error {
	d, ok := src.(pgtype.TextArray)
	if !ok {
		return fmt.Errorf("unvalid type '%v', expected pgtype.TextArray", reflect.TypeOf(src))
	}
	*dst = PgLabelValueArray{a: pgtype.Present, data: d}
	return nil
}

func (dst *PgLabelValueArray) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		return fmt.Errorf("NULL values can't be decoded. Scan into a &*MyType to handle NULLs")
	}

	if err := (pgtype.CompositeFields{&dst.a, &dst.data}).DecodeBinary(ci, src); err != nil {
		return err
	}

	return nil
}

func (src PgLabelValueArray) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) (newBuf []byte, err error) {
	var b pgtype.TextArray
	if src.data.Status == pgtype.Null {
		b = pgtype.TextArray{Status: pgtype.Null}
	} else {
		b = src.data
	}

	return (pgtype.CompositeFields{&b}).EncodeBinary(ci, buf)
}
