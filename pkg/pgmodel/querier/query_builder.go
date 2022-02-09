// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package querier

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/blang/semver/v4"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
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

// aggregators represent postgres functions which are used for the array
// aggregation of series values.
type aggregators struct {
	timeClause  string
	timeParams  []interface{}
	valueClause string
	valueParams []interface{}
	unOrdered   bool
	tsSeries    TimestampSeries //can be NULL and only present if timeClause == ""
}

// getAggregators returns the aggregator which should be used to fetch data for
// a single metric. It may apply pushdowns to functions.
func getAggregators(metadata *promqlMetadata) (*aggregators, parser.Node, error) {
	// todo: investigate if query hints can have only node and lookback

	agg, node, err := tryPushDown(metadata)
	if err == nil && agg != nil {
		return agg, node, nil
	}

	defaultAggregators := &aggregators{
		timeClause:  "array_agg(time)",
		valueClause: "array_agg(value)",
		unOrdered:   false,
	}

	return defaultAggregators, nil, nil
}

// tryPushDown inspects the AST above the current node to determine if it's
// possible to make use of a known pushdown function.
//
// We can push down some PromQL functions, as well as a VectorSelector. Refer
// to tryExtractPushdownableFunctionName to see which PromQL pushdowns are
// available.
//
// If pushdown is possible, tryPushDown returns the aggregator representing the
// pushed down function, as well as the new top node resulting from the
// pushdown. If no pushdown is possible, it returns nil.
// For more on top nodes, see `engine.populateSeries`
func tryPushDown(metadata *promqlMetadata) (*aggregators, parser.Node, error) {

	// A function call like `rate(metric[5m])` parses to this AST:
	//
	// 	Call -> MatrixSelector -> VectorSelector
	//
	// This forms the basis for how we determine if we can do a promQL
	// pushdown: If the current node is a vector selector, we look up the AST
	// to check if the "grandparent" node to the vector selector is a known
	// function which we can push down to the database.

	path := metadata.path
	queryHints := metadata.queryHints
	selectHints := metadata.selectHints

	switch {
	// We can't push down without hints.
	case queryHints == nil || selectHints == nil:
		return nil, nil, nil
	// We can't handle subqueries in pushdowns.
	case hasSubquery(path):
		return nil, nil, nil
	// We can't do pushdowns without the extension.
	case !extension.ExtensionIsInstalled:
		return nil, nil, nil
	}

	vs, isVectorSelector := queryHints.CurrentNode.(*parser.VectorSelector)

	switch {
	// We can't push down something that isn't a VectorSelector.
	case !isVectorSelector:
		return nil, nil, nil
	// We can't handle offsets in VectorSelector pushdowns.
	case vs.OriginalOffset != 0 || vs.Offset != 0:
		return nil, nil, nil
	}

	if len(path) >= 2 {
		grandparent := path[len(path)-2]
		funcName, canPushDown := tryExtractPushdownableFunctionName(grandparent)
		if canPushDown {
			agg, err := callAggregator(selectHints, funcName)
			return agg, grandparent, err
		}
	}

	//TODO: handle the instant query (hints.Step==0) case too.

	// vector selector pushdown improves performance by selecting from the
	// database only the last point in a vector selector window(step).
	// This decreases the number of samples transferred from the DB to
	// Promscale by orders of magnitude. A vector selector aggregate also
	// does not require ordered inputs which saves a sort and allows for
	// parallel evaluation.
	if selectHints.Step > 0 &&
		selectHints.Range == 0 && // So this is not an aggregate. That's optimized above
		!calledByTimestamp(path) &&
		vectorSelectorExtensionRange(extension.PromscaleExtensionVersion) {
		qf := aggregators{
			valueClause: "vector_selector($%d, $%d, $%d, $%d, time, value)",
			valueParams: []interface{}{queryHints.StartTime, queryHints.EndTime, selectHints.Step, queryHints.Lookback.Milliseconds()},
			unOrdered:   true,
			tsSeries:    newRegularTimestampSeries(queryHints.StartTime, queryHints.EndTime, time.Duration(selectHints.Step)*time.Millisecond),
		}
		return &qf, queryHints.CurrentNode, nil
	}
	return nil, nil, nil
}

func tryExtractPushdownableFunctionName(node parser.Node) (string, bool) {
	callNode, isCall := node.(*parser.Call)
	if isCall {
		switch callNode.Func.Name {
		case "delta":
			return callNode.Func.Name, true
		case "rate", "increase":
			if rateIncreaseExtensionRange(extension.PromscaleExtensionVersion) {
				return callNode.Func.Name, true
			}
		}
	}
	return "", false
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
	}
	qf := aggregators{
		valueClause: "prom_" + funcName + "($%d, $%d, $%d, $%d, time, value)",
		valueParams: []interface{}{model.Time(hints.Start).Time(), model.Time(queryEnd).Time(), stepDuration.Milliseconds(), rangeDuration.Milliseconds()},
		unOrdered:   false,
		tsSeries:    newRegularTimestampSeries(model.Time(queryStart).Time(), model.Time(queryEnd).Time(), stepDuration),
	}
	return &qf, nil
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
