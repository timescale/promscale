// This file was copied from the PGX repository and modified. PGX is
// licensed under MIT.
//
// https://github.com/jackc/pgx/blob/master/copy_from.go
//
// The MIT License
//
// Copyright (c) 2011 Dominic Tarr
//
// Permission is hereby granted, free of charge,
// to any person obtaining a copy of this software and
// associated documentation files (the "Software"), to
// deal in the Software without restriction, including
// without limitation the rights to use, copy, modify,
// merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom
// the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice
// shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
// IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR
// ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
// TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
// SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package pgxconn

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"

	"github.com/jackc/pgio"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var bufPool = sync.Pool{
	New: func() any {
		s := make([]byte, 0, 4096)
		return &s
	},
}

type copyFrom struct {
	conn          *pgx.Conn
	tableName     pgx.Identifier
	columnNames   []string
	rowSrc        pgx.CopyFromSource
	readerErrChan chan error
	oids          []uint32
	wbuf          []byte
}

func doCopyFrom(
	ctx context.Context,
	conn *pgx.Conn,
	tableName pgx.Identifier,
	columnNames []string,
	rowSrc pgx.CopyFromSource,
	oids []uint32,
) (int64, error) {
	buf := bufPool.Get().(*[]byte)
	ct := &copyFrom{
		conn:          conn,
		tableName:     tableName,
		columnNames:   columnNames,
		rowSrc:        rowSrc,
		readerErrChan: make(chan error),
		oids:          oids,
		wbuf:          *buf,
	}
	defer bufPool.Put(buf)
	return ct.run(ctx)
}

func (ct *copyFrom) run(ctx context.Context) (int64, error) {
	quotedTableName := ct.tableName.Sanitize()
	cbuf := &bytes.Buffer{}
	for i, cn := range ct.columnNames {
		if i != 0 {
			cbuf.WriteString(", ")
		}
		cbuf.WriteString(quoteIdentifier(cn))
	}
	quotedColumnNames := cbuf.String()

	r, w := io.Pipe()
	doneChan := make(chan struct{})

	go func() {
		defer close(doneChan)

		// Purposely NOT using defer w.Close(). See https://github.com/golang/go/issues/24283.
		buf := ct.wbuf

		buf = append(buf, "PGCOPY\n\377\r\n\000"...)
		buf = pgio.AppendInt32(buf, 0)
		buf = pgio.AppendInt32(buf, 0)

		moreRows := true
		for moreRows {
			var err error
			moreRows, buf, err = ct.buildCopyBuf(buf, ct.oids)
			if err != nil {
				w.CloseWithError(err)
				return
			}

			if ct.rowSrc.Err() != nil {
				w.CloseWithError(ct.rowSrc.Err())
				return
			}

			if len(buf) > 0 {
				_, err = w.Write(buf)
				if err != nil {
					_ = w.Close()
					return
				}
			}

			buf = buf[:0]
		}

		_ = w.Close()
	}()

	commandTag, err := ct.conn.PgConn().CopyFrom(ctx, r, fmt.Sprintf("copy %s ( %s ) from stdin binary;", quotedTableName, quotedColumnNames))

	_ = r.Close()
	<-doneChan

	return commandTag.RowsAffected(), err
}

func (ct *copyFrom) buildCopyBuf(buf []byte, oids []uint32) (bool, []byte, error) {

	if len(ct.columnNames) != len(oids) {
		return false, nil, fmt.Errorf("expected %d OIDs, got %d OIDs", len(ct.columnNames), len(oids))
	}

	for ct.rowSrc.Next() {
		values, err := ct.rowSrc.Values()
		if err != nil {
			return false, nil, err
		}

		if len(values) != len(ct.columnNames) {
			return false, nil, fmt.Errorf("expected %d values, got %d values", len(ct.columnNames), len(values))
		}

		buf = pgio.AppendInt16(buf, int16(len(ct.columnNames)))
		for i, val := range values {
			buf, err = encodeCopyValue(ct.conn.TypeMap(), buf, oids[i], val)
			if err != nil {
				return false, nil, err
			}
		}

		if len(buf) > 65536 {
			return true, buf, nil
		}
	}

	return false, buf, nil
}

func quoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func encodeCopyValue(m *pgtype.Map, buf []byte, oid uint32, arg any) ([]byte, error) {
	if isNil(arg) {
		return pgio.AppendInt32(buf, -1), nil
	}

	sp := len(buf)
	buf = pgio.AppendInt32(buf, -1)
	argBuf, err := m.Encode(oid, pgx.BinaryFormatCode, arg, buf)
	if err != nil {
		if argBuf2, err2 := tryScanStringCopyValueThenEncode(m, buf, oid, arg); err2 == nil {
			argBuf = argBuf2
		} else {
			return nil, err
		}
	}

	if argBuf != nil {
		buf = argBuf
		pgio.SetInt32(buf[sp:], int32(len(buf[sp:])-4))
	}
	return buf, nil
}

func tryScanStringCopyValueThenEncode(m *pgtype.Map, buf []byte, oid uint32, arg any) ([]byte, error) {
	s, ok := arg.(string)
	if !ok {
		return nil, errors.New("not a string")
	}

	var v any
	err := m.Scan(oid, pgx.TextFormatCode, []byte(s), &v)
	if err != nil {
		return nil, err
	}

	return m.Encode(oid, pgx.BinaryFormatCode, v, buf)
}

// Is returns true if value is any type of nil. e.g. nil or []byte(nil).
func isNil(value any) bool {
	if value == nil {
		return true
	}

	refVal := reflect.ValueOf(value)
	switch refVal.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.UnsafePointer, reflect.Interface, reflect.Slice:
		return refVal.IsNil()
	default:
		return false
	}
}
