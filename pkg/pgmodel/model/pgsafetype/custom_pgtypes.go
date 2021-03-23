// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgsafetype

import (
	"fmt"
	"strings"

	"github.com/jackc/pgtype"
)

const (
	NullChar         = '\x00'
	NullCharSanitize = '\ufffe'
)

// Text is a custom pgtype that wraps the pgtype.Text. It is safe from null characters.
type Text struct {
	pgtype.Text
}

func (t *Text) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	if err := t.Text.DecodeBinary(ci, src); err != nil {
		return fmt.Errorf("pgsafetype.Text.DecodeBinary: %w", err)
	}
	return nil
}

func (t *Text) Set(src interface{}) error {
	text, ok := src.(string)
	if !ok {
		return fmt.Errorf("pgsafetype.Text.Set(): src is not of 'string' type")
	}
	if err := t.Text.Set(sanitizeNullChars(text)); err != nil {
		return fmt.Errorf("safe text: %w", err)
	}
	return nil
}

func (t *Text) Get() interface{} {
	// Status is set to present in pgtype.Text when using pgtype.Text.Set(). However, when using a Scan(),
	// this does not happen. In order to make this work, we need to explicitly tell here that data is present, otherwise
	// data is not returned, rather a status is returned in pgtype.Text.Get().
	t.Text.Status = pgtype.Present
	s, ok := t.Text.Get().(string)
	if !ok {
		panic("'string' type not received from underlying 'pgtype.Text'")
	}
	return revertSanitization(s)
}

func (t *Text) AssignTo(_ interface{}) error {
	panic("pgsafetype.Text.AssignTo(): not implemented")
}

func (t *Text) Scan(_ interface{}) error {
	panic("pgsafetype.Text.Scan(): not implemented")
}

// TextArray is a custom pgtype that wraps the pgtype.TextArray. It is safe from null characters.
type TextArray struct {
	pgtype.TextArray
}

func (t *TextArray) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	if err := t.TextArray.DecodeBinary(ci, src); err != nil {
		return fmt.Errorf("pgsafetype.TextArray.DecodeBinary: %w", err)
	}
	return nil
}

func (t *TextArray) Set(src interface{}) error {
	textArray, ok := src.([]string)
	if !ok {
		return fmt.Errorf("pgsafetype.TextArray.Set(): src is not of '[]string' type")
	}
	buf := make([]pgtype.Text, len(textArray))
	for i := range textArray {
		var text pgtype.Text
		if err := text.Set(sanitizeNullChars(textArray[i])); err != nil {
			return fmt.Errorf("safe text-array: %w", err)
		}
		buf[i] = text
	}
	if err := t.TextArray.Set(buf); err != nil {
		return fmt.Errorf("safe textarray: %w", err)
	}
	return nil
}

func (t *TextArray) revertSanitization() {
	val := t.TextArray.Get()
	if val == nil {
		return
	}
	arr := val.(pgtype.TextArray)
	for i := range arr.Elements {
		tmp, ok := arr.Elements[i].Get().(string)
		if !ok {
			panic("'string' type not received from underlying 'pgtype.Text' in 'pgsafetype.TextArray'")
		}
		arr.Elements[i].String = revertSanitization(tmp)
	}
	t.Elements = arr.Elements
}

func (t *TextArray) Get() interface{} {
	t.revertSanitization()
	// General patterns of returns in Get() in pgtype package is a value and not a pointer. To keep things in line,
	// we should do the same as well.
	return *t
}

func (t *TextArray) AssignTo(_ interface{}) error {
	panic("pgsafetype.TextArray.AssignTo(): not implemented")
}

func (t *TextArray) Scan(_ interface{}) error {
	panic("pgsafetype.TextArray.Scan(): not implemented")
}

func replaceFunc(r rune) rune {
	if r == NullChar {
		return NullCharSanitize
	}
	return r
}

func revertFunc(r rune) rune {
	if r == NullCharSanitize {
		return NullChar
	}
	return r
}

func sanitizeNullChars(s string) string {
	if strings.ContainsRune(s, NullChar) {
		return strings.Map(replaceFunc, s)
	}
	return s
}

func revertSanitization(s string) string {
	if strings.ContainsRune(s, NullCharSanitize) {
		return strings.Map(revertFunc, s)
	}
	return s
}
