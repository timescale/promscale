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

	"github.com/jackc/pgx/v5/pgtype"
)

const (
	NullChar         = '\x00'
	NullCharSanitize = '\ufffe'
)

// Text is a custom pgtype that wraps the pgtype.Text. It is safe from null characters.
type Text struct {
	pgtype.Text // Contains sanitized string.
}

func (t *Text) ScanText(v pgtype.Text) error {
	if !v.Valid {
		t.Text = v
		return nil
	}
	t.Text = pgtype.Text{
		String: sanitizeNullChars(v.String),
		Valid:  true,
	}
	return nil
}

// Scan implements the database/sql Scanner interface.
func (dst *Text) Scan(src any) error {
	if src == nil {
		dst.Text = pgtype.Text{}
		return nil
	}

	switch src := src.(type) {
	case string:
		dst.Text = pgtype.Text{String: sanitizeNullChars(src), Valid: true}
		return nil
	case []byte:
		dst.Text = pgtype.Text{String: sanitizeNullChars(string(src)), Valid: true}
		return nil
	}

	return fmt.Errorf("cannot scan %T", src)
}

func TextArrayToSlice(src pgtype.FlatArray[Text]) []string {
	if src == nil {
		return nil
	}

	originalElements := make([]string, len(src))
	for i, v := range src {
		// TODO should we maybe return an error?
		if !v.Valid {
			panic("'string' type not received from underlying 'pgtype.Text' in 'pgutf8str.TextArray'")
		}
		originalElements[i] = revertSanitization(v.String)
	}
	return originalElements
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
