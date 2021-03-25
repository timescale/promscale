// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"fmt"

	"github.com/jackc/pgtype"
	"github.com/timescale/promscale/pkg/pgmodel/model/pgutf8str"
)

type LabelList struct {
	names  *pgutf8str.TextArray
	values *pgutf8str.TextArray
}

func NewLabelList(size int) *LabelList {
	nameElements := make([]pgtype.Text, 0, size)
	valueElements := make([]pgtype.Text, 0, size)
	return &LabelList{
		// We want to avoid runtime conversion of []string to pgutf8str.TextArray. The best way to do that is
		// to use directly the pgutf8str.TextArray under the hood.
		// The implementations done here are kept in line with what happens in
		// https://github.com/jackc/pgtype/blob/master/text_array.go
		names: &pgutf8str.TextArray{
			TextArray: pgtype.TextArray{
				Elements:   nameElements,
				Dimensions: []pgtype.ArrayDimension{{Length: int32(size), LowerBound: 1}},
				Status:     pgtype.Present,
			},
		},
		values: &pgutf8str.TextArray{
			TextArray: pgtype.TextArray{
				Elements:   valueElements,
				Dimensions: []pgtype.ArrayDimension{{Length: int32(size), LowerBound: 1}},
				Status:     pgtype.Present,
			},
		},
	}
}

func (ls *LabelList) Add(name string, value string) error {
	var (
		nameT  pgutf8str.Text
		valueT pgutf8str.Text
	)
	if err := nameT.Set(name); err != nil {
		return fmt.Errorf("setting pgtype.Text: %w", err)
	}
	if err := valueT.Set(value); err != nil {
		return fmt.Errorf("setting pgtype.Text: %w", err)
	}
	ls.names.Elements = append(ls.names.Elements, nameT.Text)
	ls.values.Elements = append(ls.values.Elements, valueT.Text)
	return nil
}

func (ls *LabelList) updateArrayDimensions() {
	l := int32(len(ls.names.Elements))
	ls.names.Dimensions[0].Length = l
	ls.values.Dimensions[0].Length = l
}

// Get returns the addresses of names and values slice after updating the array dimensions.
func (ls *LabelList) Get() (*pgutf8str.TextArray, *pgutf8str.TextArray) {
	ls.updateArrayDimensions()
	return ls.names, ls.values
}

func (ls *LabelList) Len() int { return len(ls.names.Elements) }
func (ls *LabelList) Swap(i, j int) {
	ls.names.Elements[i], ls.names.Elements[j] = ls.names.Elements[j], ls.names.Elements[i]
	ls.values.Elements[i], ls.values.Elements[j] = ls.values.Elements[j], ls.values.Elements[i]
}
func (ls LabelList) Less(i, j int) bool {
	elemI := ls.names.Elements[i].String
	elemJ := ls.names.Elements[j].String
	return elemI < elemJ || (elemI == elemJ && elemI < elemJ)
}
