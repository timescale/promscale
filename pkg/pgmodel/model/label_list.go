// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/timescale/promscale/pkg/pgmodel/model/pgutf8str"
)

type LabelList struct {
	names  pgtype.FlatArray[pgutf8str.Text]
	values pgtype.FlatArray[pgutf8str.Text]
}

func NewLabelList(size int) *LabelList {
	return &LabelList{
		// We want to avoid runtime conversion of []string to pgutf8str.TextArray. The best way to do that is
		// to use directly the pgutf8str.TextArray under the hood.
		// The implementations done here are kept in line with what happens in
		// https://github.com/jackc/pgtype/blob/master/text_array.go
		names:  pgtype.FlatArray[pgutf8str.Text](make([]pgutf8str.Text, 0, size)),
		values: pgtype.FlatArray[pgutf8str.Text](make([]pgutf8str.Text, 0, size)),
	}
}

func (ls *LabelList) Add(name string, value string) error {
	var (
		nameT  pgutf8str.Text
		valueT pgutf8str.Text
	)
	if err := nameT.Scan(name); err != nil {
		return fmt.Errorf("setting pgutf8str.Text: %w", err)
	}
	if err := valueT.Scan(value); err != nil {
		return fmt.Errorf("setting pgutf8str.Text: %w", err)
	}
	ls.names = append(ls.names, nameT)
	ls.values = append(ls.values, valueT)
	return nil
}

// Get returns the addresses of names and values slice after updating the array dimensions.
func (ls *LabelList) Get() (pgtype.FlatArray[pgutf8str.Text], pgtype.FlatArray[pgutf8str.Text]) {
	return ls.names, ls.values
}

func (ls *LabelList) Len() int { return len(ls.names) }
func (ls *LabelList) Swap(i, j int) {
	ls.names[i], ls.names[j] = ls.names[j], ls.names[i]
	ls.values[i], ls.values[j] = ls.values[j], ls.values[i]
}

func (ls LabelList) Less(i, j int) bool {
	elemI := ls.names[i].String
	elemJ := ls.names[j].String
	return elemI < elemJ || (elemI == elemJ && elemI < elemJ)
}
