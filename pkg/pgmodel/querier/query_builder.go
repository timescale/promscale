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
	"github.com/grafana/regexp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/promscale/pkg/log"
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

	// RE2 regex matching sub-queries using custom regex matching function.
	// TODO: we might want to reduce the complexity in the future by using re2_match function for all regex matching.
	subQueryRE2            = "labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and _prom_ext.re2_match(l.value, $%d))"
	subQueryRE2MatchEmpty  = "NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and not _prom_ext.re2_match(l.value, $%d))"
	subQueryNRE2           = "labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and not _prom_ext.re2_match(l.value, $%d))"
	subQueryNRE2MatchEmpty = "NOT labels && (SELECT COALESCE(array_agg(l.id), array[]::int[]) FROM _prom_catalog.label l WHERE l.key = $%d and _prom_ext.re2_match(l.value, $%d))"
)

var (
	minTime = timestamp.FromTime(time.Unix(math.MinInt64/1000+62135596801, 0).UTC())
	maxTime = timestamp.FromTime(time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC())

	// Regex used to try to detect any non-POSIX regex features that should
	// be treated as RE2 regexes.
	re2Regex = regexp.MustCompile(`\(\?`)
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
			switch m.Name {
			case pgmodel.MetricNameLabelName:
				cb.SetMetricName(m.Value)
			case pgmodel.SchemaNameLabelName:
				cb.SetSchemaName(m.Value)
			case pgmodel.ColumnNameLabelName:
				cb.SetColumnName(m.Value)
			default:
				sq := subQueryEQ
				if matchesEmpty {
					sq = subQueryEQMatchEmpty
				}
				err = cb.addClause(sq, m.Name, m.Value)
			}
		case labels.MatchNotEqual:
			sq := subQueryNEQ
			if matchesEmpty {
				sq = subQueryNEQMatchEmpty
			}
			err = cb.addClause(sq, m.Name, m.Value)
		case labels.MatchRegexp:
			re2 := re2Regex.MatchString(m.Value)
			sq := subQueryRE
			switch {
			case !re2 && !matchesEmpty:
				sq = subQueryRE
			case !re2 && matchesEmpty:
				sq = subQueryREMatchEmpty
			case re2 && matchesEmpty:
				sq = subQueryRE2MatchEmpty
			case re2 && !matchesEmpty:
				sq = subQueryRE2
			}
			err = cb.addClause(sq, m.Name, anchorValue(m.Value))
		case labels.MatchNotRegexp:
			re2 := re2Regex.MatchString(m.Value)
			sq := subQueryNRE
			switch {
			case !re2 && !matchesEmpty:
				sq = subQueryNRE
			case !re2 && matchesEmpty:
				sq = subQueryNREMatchEmpty
			case re2 && matchesEmpty:
				sq = subQueryNRE2MatchEmpty
			case re2 && !matchesEmpty:
				sq = subQueryNRE2
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
			if id == nil || *id == 0 {
				continue
			}
			index[*id] = labels.Label{}
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

		if row.times.Len() != len(row.values.FlatArray) {
			return nil, errors.ErrQueryMismatchTimestampValue
		}

		promLabels := make([]prompb.Label, 0, len(row.labelIds))
		for _, id := range row.labelIds {
			if id == nil || *id == 0 {
				continue
			}
			label, ok := labelIDMap[*id]
			if !ok {
				return nil, fmt.Errorf("missing label for id %v", *id)
			}
			if label == (labels.Label{}) {
				return nil, fmt.Errorf("label not found for id %v", *id)
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
				Value:     row.values.FlatArray[i].Float64,
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

// calledByTimestamp returns whether the immediate parent node is the
// `timestamp` function call.
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
func getAggregators(metadata *promqlMetadata) (*aggregators, parser.Node) {

	agg, node, err := tryPushDown(metadata)
	if err != nil {
		log.Info("msg", "error while trying to push down, will skip pushdown optimization", "error", err)
	} else if agg != nil {
		return agg, node
	}

	defaultAggregators := &aggregators{
		timeClause:  "array_agg(time)",
		valueClause: "array_agg(value)",
		unOrdered:   false,
	}

	return defaultAggregators, nil
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
			agg, err := buildPromQlFunctionCallAggregator(selectHints, funcName)
			return agg, grandparent, err
		}
	}

	lookback := queryHints.Lookback.Milliseconds()
	agg := buildVectorSelectorFunctionCallAggregator(lookback, selectHints, path)
	if agg != nil {
		return agg, queryHints.CurrentNode, nil
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

func buildPromQlFunctionCallAggregator(selectHints *storage.SelectHints, funcName string) (*aggregators, error) {
	// Note: selectHints.Start = results.start - lookback, i.e. it has been
	// adjusted to account for the time from which we start _scanning_ for
	// results. The time range that results will lie in is:
	// [resultStart, selectHints.end].
	resultStart := selectHints.Start + selectHints.Range

	stepDuration := time.Second
	rangeDuration := time.Duration(selectHints.Range) * time.Millisecond

	switch {
	case selectHints.Step > 0:
		stepDuration = time.Duration(selectHints.Step) * time.Millisecond
	case selectHints.Step == 0 && resultStart != selectHints.End:
		return nil, fmt.Errorf("query start should equal query end")
	}

	// On time ranges: given the parameters (scan_start, result_start,
	// result_end), as defined in buildSingleMetricSamplesQuery, the prom_*
	// call that we construct here has the rough structure of:
	// SELECT
	//   prom_*(scan_start, result_end, ...)
	// FROM ...
	// WHERE t >= scan_start AND t <= result_end
	//
	// Note: The actual WHERE clause parameters are set in
	// buildSingleMetricSamplesQuery

	qf := aggregators{
		valueClause: "_prom_ext.prom_" + funcName + "($%d, $%d, $%d, $%d, time, value)",
		valueParams: []interface{}{model.Time(selectHints.Start).Time(), model.Time(selectHints.End).Time(), stepDuration.Milliseconds(), rangeDuration.Milliseconds()},
		unOrdered:   false,
		tsSeries:    newRegularTimestampSeries(model.Time(resultStart).Time(), model.Time(selectHints.End).Time(), stepDuration),
	}
	return &qf, nil
}

func buildVectorSelectorFunctionCallAggregator(lookback int64, selectHints *storage.SelectHints, path []parser.Node) *aggregators {
	// vector selector pushdown improves performance by selecting from the
	// database only the last point in a vector selector window (step).
	// This decreases the number of samples transferred from the DB to
	// Promscale by orders of magnitude. A vector selector aggregate also
	// does not require ordered inputs which saves a sort and allows for
	// parallel evaluation. For more information, refer to the module doc of
	// the promscale extension.

	switch {
	// We can't handle a zero-sized step, so skip pushdown optimization.
	// TODO: handle the instant query (hints.Step==0) case too.
	case selectHints.Step == 0:
		return nil
	// The `vector_selector` can only be applied to non-aggregates (i.e. when range is zero).
	case selectHints.Range != 0:
		return nil
	// Vector selectors called by the timestamp function have special handling, see `eval` in engine.go.
	case calledByTimestamp(path):
		return nil
	// We need the extension version to support pushdowns.
	case !vectorSelectorExtensionRange(extension.PromscaleExtensionVersion):
		return nil
	}

	// On time ranges: given the parameters (scan_start, result_start,
	// result_end), as defined in buildSingleMetricSamplesQuery, the
	// vector_selector call that we construct here has the rough structure
	// of:
	// SELECT
	//   vector_selector(result_start, result_end, ...)
	// FROM ...
	// WHERE t >= scan_start AND t <= result_end
	//
	// Note: The actual WHERE clause parameters are set in
	// buildSingleMetricSamplesQuery

	resultStart := selectHints.Start + lookback
	resultEnd := selectHints.End
	qf := aggregators{
		valueClause: "_prom_ext.vector_selector($%d, $%d, $%d, $%d, time, value)",
		valueParams: []interface{}{model.Time(resultStart).Time(), model.Time(resultEnd).Time(), selectHints.Step, lookback},
		unOrdered:   true,
		tsSeries:    newRegularTimestampSeries(model.Time(resultStart).Time(), model.Time(resultEnd).Time(), time.Duration(selectHints.Step)*time.Millisecond),
	}
	return &qf
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
