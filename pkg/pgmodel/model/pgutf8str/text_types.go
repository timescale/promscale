// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

// pgutf8str solves the null char issue in Postgres when using UTF-8 based encoding. By default, Postgres has troubles
// handling null chars. Types under this packages solves this by sanitizing illegal null chars and reverting back to
// original form on Get().

package pgutf8str

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
	pgtype.Text // Contains sanitized string.
}

func (t *Text) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	if err := t.Text.DecodeBinary(ci, src); err != nil {
		return fmt.Errorf("pgutf8str.Text.DecodeBinary: %w", err)
	}
	return nil
}

func (t *Text) Set(src interface{}) error {
	text, ok := src.(string)
	if !ok {
		return fmt.Errorf("pgutf8str.Text.Set(): src is not of 'string' type")
	}
	if err := t.Text.Set(sanitizeNullChars(text)); err != nil {
		return fmt.Errorf("safe text: %w", err)
	}
	return nil
}

func (t *Text) Get() interface{} {
	s, ok := t.Text.Get().(string)
	if !ok {
		panic("'string' type not received from underlying 'pgtype.Text'")
	}
	return revertSanitization(s)
}

func (t *Text) AssignTo(_ interface{}) error {
	panic("pgutf8str.Text.AssignTo(): not implemented")
}

func (t *Text) Scan(_ interface{}) error {
	panic("pgutf8str.Text.Scan(): not implemented")
}

// TextArray is a custom pgtype that wraps the pgtype.TextArray. It is safe from null characters.
type TextArray struct {
	pgtype.TextArray // Contains sanitized string.
}

func (t *TextArray) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	if err := t.TextArray.DecodeBinary(ci, src); err != nil {
		return fmt.Errorf("pgutf8str.TextArray.DecodeBinary: %w", err)
	}
	return nil
}

func (t *TextArray) Slice(low, high int) (*TextArray, error) {
	var new TextArray
	err := new.TextArray.Set(t.Elements[low:high])
	return &new, err
}

func (t *TextArray) Set(src interface{}) error {
	textArray, ok := src.([]string)
	if !ok {
		return fmt.Errorf("pgutf8str.TextArray.Set(): src is not of '[]string' type")
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

func (t *TextArray) revertSanitization() []string {
	val := t.TextArray.Get()
	if val == nil {
		return nil
	}
	arr, ok := val.(pgtype.TextArray)
	if !ok {
		panic("underlying data not in 'pgutf8str.TextArray' type")
	}
	originalElements := make([]string, len(arr.Elements))
	for i := range arr.Elements {
		tmp, ok := arr.Elements[i].Get().(string)
		if !ok {
			panic("'string' type not received from underlying 'pgtype.Text' in 'pgutf8str.TextArray'")
		}
		originalElements[i] = revertSanitization(tmp)
	}
	return originalElements
}

func (t *TextArray) Get() interface{} {
	return t.revertSanitization()
}

func (t *TextArray) AssignTo(_ interface{}) error {
	panic("pgutf8str.TextArray.AssignTo(): not implemented")
}

func (t *TextArray) Scan(_ interface{}) error {
	panic("pgutf8str.TextArray.Scan(): not implemented")
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
