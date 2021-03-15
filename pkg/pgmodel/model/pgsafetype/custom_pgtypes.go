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

func (t *Text) Set(src interface{}) error {
	text, ok := src.(string)
	if !ok {
		return fmt.Errorf("pgsafetype.Text Set(): src is not of 'string' type")
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

func (t *Text) AssignTo(dst interface{}) error {
	if err := t.Text.AssignTo(dst); err != nil {
		return fmt.Errorf("safe text: %w", err)
	}
	return nil
}

func (t *Text) Scan(src interface{}) error {
	if err := t.Text.Scan(src); err != nil {
		return fmt.Errorf("safe text: %w", err)
	}
	return nil
}

// TextArray is a custom pgtype that wraps the pgtype.TextArray. It is safe from null characters.
type TextArray struct {
	pgtype.TextArray
}

func (t *TextArray) Set(src interface{}) error {
	textArray, ok := src.([]string)
	if !ok {
		return fmt.Errorf("pgsafetype.TextArray Set(): src is not of '[]string' type")
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
		return fmt.Errorf("safe text: %w", err)
	}
	return nil
}

func (t *TextArray) Get() interface{} {
	var (
		arr = t.TextArray.Get().(pgtype.TextArray).Elements
		s   = make([]string, len(arr))
	)
	for i := range arr {
		tmp, ok := arr[i].Get().(string)
		if !ok {
			panic("'string' type not received from underlying 'pgtype.Text' in 'pgsafetype.TextArray'")
		}
		s[i] = revertSanitization(tmp)
	}
	return s
}

func (t *TextArray) AssignTo(dst interface{}) error {
	if err := t.TextArray.AssignTo(dst); err != nil {
		return fmt.Errorf("safe text: %w", err)
	}
	return nil
}

func (t *TextArray) Scan(src interface{}) error {
	if err := t.TextArray.Scan(src); err != nil {
		return fmt.Errorf("safe text: %w", err)
	}
	return nil
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
	return strings.Map(replaceFunc, s)
}

func revertSanitization(s string) string {
	return strings.Map(revertFunc, s)
}
