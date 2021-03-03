// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

type LabelList struct {
	Names  []string
	Values []string
}

func NewLabelList(size int) *LabelList {
	return &LabelList{Names: make([]string, 0, size), Values: make([]string, 0, size)}
}
func (ls *LabelList) Add(name string, value string) {
	ls.Names = append(ls.Names, name)
	ls.Values = append(ls.Values, value)
}

func (ls *LabelList) Len() int { return len(ls.Names) }
func (ls *LabelList) Swap(i, j int) {
	ls.Names[i], ls.Names[j] = ls.Names[j], ls.Names[i]
	ls.Values[i], ls.Values[j] = ls.Values[j], ls.Values[i]
}
func (ls LabelList) Less(i, j int) bool {
	return ls.Names[i] < ls.Names[j] || (ls.Names[i] == ls.Names[j] && ls.Values[i] < ls.Values[j])
}
