// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package querier

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/blang/semver/v4"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/common/extension"
	"github.com/timescale/promscale/pkg/pgmodel/lreader"
	pgmodel "github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

const (
	subQueryEQ            = "labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value = $%d)"
	subQueryEQMatchEmpty  = "NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value != $%d)"
	subQueryNEQ           = "labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value != $%d)"
	subQueryNEQMatchEmpty = "NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value = $%d)"
	subQueryRE            = "labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value ~ $%d)"
	subQueryREMatchEmpty  = "NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value !~ $%d)"
	subQueryNRE           = "labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value !~ $%d)"
	subQueryNREMatchEmpty = "NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and l.value ~ $%d)"
)

var (
	minTime = timestamp.FromTime(time.Unix(math.MinInt64/1000+62135596801, 0).UTC())
	maxTime = timestamp.FromTime(time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC())
)

func BuildSubQueries(matchers []*labels.Matcher) (*clauseBuilder, error) {
	var err error
	cb := &clauseBuilder{}

	for _, m := range matchers {
		// From the PromQL docs: "Label matchers that match
		// empty label values also select all time series that
		// do not have the specific label set at all."
		matchesEmpty := m.Matches("")

		switch m.Type {
		case labels.MatchEqual:
			if m.Name == pgmodel.MetricNameLabelName {
				cb.SetMetricName(m.Value)
				continue
			}
			if m.Name == pgmodel.SchemaNameLabelName {
				cb.SetSchemaName(m.Value)
				continue
			}
			if m.Name == pgmodel.ColumnNameLabelName {
				cb.SetColumnName(m.Value)
				continue
			}
			sq := subQueryEQ
			if matchesEmpty {
				sq = subQueryEQMatchEmpty
			}
			err = cb.addClause(sq, m.Name, m.Value)
		case labels.MatchNotEqual:
			sq := subQueryNEQ
			if matchesEmpty {
				sq = subQueryNEQMatchEmpty
			}
			err = cb.addClause(sq, m.Name, m.Value)
		case labels.MatchRegexp:
			sq := subQueryRE
			if matchesEmpty {
				sq = subQueryREMatchEmpty
			}
			err = cb.addClause(sq, m.Name, anchorValue(m.Value))
		case labels.MatchNotRegexp:
			sq := subQueryNRE
			if matchesEmpty {
				sq = subQueryNREMatchEmpty
			}
			err = cb.addClause(sq, m.Name, anchorValue(m.Value))
		}

		if err != nil {
			return nil, err
		}
	}

	return cb, err
}

func initLabelIdIndexForSamples(index map[int64]labels.Label, rows []sampleRow) {
	for i := range rows {
		for _, id := range rows[i].labelIds {
			//id==0 means there is no label for the key, so nothing to look up
			if id == 0 {
				continue
			}
			index[id] = labels.Label{}
		}
	}
}

func buildTimeSeries(rows []sampleRow, lr lreader.LabelsReader) ([]*prompb.TimeSeries, error) {
	results := make([]*prompb.TimeSeries, 0, len(rows))
	labelIDMap := make(map[int64]labels.Label)
	initLabelIdIndexForSamples(labelIDMap, rows)

	err := lr.LabelsForIdMap(labelIDMap)
	if err != nil {
		return nil, fmt.Errorf("fetching labels to build timeseries: %w", err)
	}

	for _, row := range rows {
		if row.err != nil {
			return nil, row.err
		}

		if row.times.Len() != len(row.values.Elements) {
			return nil, errors.ErrQueryMismatchTimestampValue
		}

		promLabels := make([]prompb.Label, 0, len(row.labelIds))
		for _, id := range row.labelIds {
			if id == 0 {
				continue
			}
			label, ok := labelIDMap[id]
			if !ok {
				return nil, fmt.Errorf("missing label for id %v", id)
			}
			if label == (labels.Label{}) {
				return nil, fmt.Errorf("label not found for id %v", id)
			}
			promLabels = append(promLabels, prompb.Label{Name: label.Name, Value: label.Value})

		}
		if row.metricOverride != "" {
			for i := range promLabels {
				if promLabels[i].Name == pgmodel.MetricNameLabelName {
					promLabels[i].Value = row.metricOverride
					break
				}
			}
		}
		for _, v := range row.GetAdditionalLabels() {
			promLabels = append(promLabels, prompb.Label{Name: v.Name, Value: v.Value})
		}

		sort.Slice(promLabels, func(i, j int) bool {
			return promLabels[i].Name < promLabels[j].Name
		})

		result := &prompb.TimeSeries{
			Labels:  promLabels,
			Samples: make([]prompb.Sample, 0, row.times.Len()),
		}

		for i := 0; i < row.times.Len(); i++ {
			ts, ok := row.times.At(i)
			if !ok {
				return nil, fmt.Errorf("invalid timestamp found")
			}
			result.Samples = append(result.Samples, prompb.Sample{
				Timestamp: ts,
				Value:     row.values.Elements[i].Float,
			})
		}

		results = append(results, result)
	}

	return results, nil
}

func hasSubquery(path []parser.Node) bool {
	for _, node := range path {
		switch node.(type) {
		case *parser.SubqueryExpr:
			return true
		}
	}
	return false
}

/* vectorSelectors called by the timestamp function have special handling see engine.go */
func calledByTimestamp(path []parser.Node) bool {
	if len(path) > 0 {
		node := path[len(path)-1]
		call, ok := node.(*parser.Call)
		if ok && call.Func.Name == "timestamp" {
			return true
		}
	}
	return false
}

var (
	vectorSelectorExtensionRange = semver.MustParseRange(">= 0.2.0")
	rateIncreaseExtensionRange   = semver.MustParseRange(">= 0.2.0")
)

type aggregators struct {
	timeClause  string
	timeParams  []interface{}
	valueClause string
	valueParams []interface{}
	unOrdered   bool
	tsSeries    TimestampSeries //can be NULL and only present if timeClause == ""
}

/* The path is the list of ancestors (direct parent last) returned node is the most-ancestral node processed by the pushdown */
// todo: investigate if query hints can have only node and lookback
func getAggregators(md *promqlMetadata) (*aggregators, parser.Node, error) {
	ast := md.path // PromQL AST.
	queryHints := md.queryHints
	selectHints := md.selectHints
	if !extension.ExtensionIsInstalled || queryHints == nil || hasSubquery(ast) || selectHints == nil {
		return getDefaultAggregators(), nil, nil
	}

	//switch on the current node being processed
	vs, isVectorSelector := queryHints.CurrentNode.(*parser.VectorSelector)
	if isVectorSelector {
		/* Try to optimize the aggregation first since that would return less data than a plain vector selector */
		if len(ast) >= 2 {
			//switch on the 2nd-to-last last path node
			node := ast[len(ast)-2]
			callNode, isCall := node.(*parser.Call)
			if isCall {
				switch callNode.Func.Name {
				case "delta":
					agg, err := callAggregator(selectHints, callNode.Func.Name)
					return agg, node, err
				case "rate", "increase":
					if rateIncreaseExtensionRange(extension.PromscaleExtensionVersion) {
						agg, err := callAggregator(selectHints, callNode.Func.Name)
						return agg, node, err
					}
				}
			}
		}

		//TODO: handle the instant query (hints.Step==0) case too.
		/* vector selector pushdown improves performance by selecting from the database only the last point
		* in a vector selector window(step) this decreases the amount of samples transferred from the DB to Promscale
		* by orders of magnitude. A vector selector aggregate also does not require ordered inputs which saves
		* a sort and allows for parallel evaluation. */
		if selectHints.Step > 0 &&
			selectHints.Range == 0 && /* So this is not an aggregate. That's optimized above */
			!calledByTimestamp(ast) &&
			vs.OriginalOffset == time.Duration(0) &&
			vs.Offset == time.Duration(0) &&
			vectorSelectorExtensionRange(extension.PromscaleExtensionVersion) {
			qf := aggregators{
				valueClause: "vector_selector($%d, $%d,$%d, $%d, time, value)",
				valueParams: []interface{}{queryHints.StartTime, queryHints.EndTime, selectHints.Step, queryHints.Lookback.Milliseconds()},
				unOrdered:   true,
				tsSeries:    newRegularTimestampSeries(queryHints.StartTime, queryHints.EndTime, time.Duration(selectHints.Step)*time.Millisecond),
			}
			return &qf, queryHints.CurrentNode, nil
		}
	}

	return getDefaultAggregators(), nil, nil
}

func GetSeriesPerMetric(rows pgxconn.PgxRows) ([]string, []string, [][]pgmodel.SeriesID, error) {
	metrics := make([]string, 0)
	schemas := make([]string, 0)
	series := make([][]pgmodel.SeriesID, 0)

	for rows.Next() {
		var (
			metricName string
			schemaName string
			seriesIDs  []int64
		)
		if err := rows.Scan(&schemaName, &metricName, &seriesIDs); err != nil {
			return nil, nil, nil, err
		}
	}
}

func getDefaultAggregators() *aggregators {
	return &aggregators{
		timeClause:  "array_agg(time)",
		valueClause: "array_agg(value)",
		unOrdered:   false,
	}
}

func callAggregator(hints *storage.SelectHints, funcName string) (*aggregators, error) {
	queryStart := hints.Start + hints.Range
	queryEnd := hints.End
	stepDuration := time.Second
	rangeDuration := time.Duration(hints.Range) * time.Millisecond

	if hints.Step > 0 {
		stepDuration = time.Duration(hints.Step) * time.Millisecond
	} else {
		if queryStart != queryEnd {
			return nil, fmt.Errorf("query start should equal query end")
		}

		metrics = append(metrics, metricName)
		schemas = append(schemas, schemaName)
		series = append(series, sIDs)
	}

	return metrics, schemas, series, nil
}

// anchorValue adds anchors to values in regexps since PromQL docs
// states that "Regex-matches are fully anchored."
func anchorValue(str string) string {
	//Reference:  NewFastRegexMatcher in Prometheus source code
	return "^(?:" + str + ")$"
}

func toRFC3339Nano(milliseconds int64) string {
	if milliseconds == minTime {
		return "-Infinity"
	}
	if milliseconds == maxTime {
		return "Infinity"
	}
	sec := milliseconds / 1000
	nsec := (milliseconds - (sec * 1000)) * 1000000
	return time.Unix(sec, nsec).UTC().Format(time.RFC3339Nano)
}
