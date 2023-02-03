package ilp

import (
	"compress/gzip"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/parsers/influx/influx_upstream"
	"github.com/influxdata/telegraf/plugins/serializers/prometheus"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/prompb"
)

var timeProvider = time.Now

var (
	ErrNoMetric = errors.New("no metric in line")
	ErrEOF      = errors.New("EOF")
)

// ParseRequest parses an incoming HTTP request as a Prometheus text format.
func ParseRequest(req *http.Request, wr *prompb.WriteRequest) error {
	// Check that the content length is not too large for us to handle.
	// if req.ContentLength > int64(h.MaxBodySize) {
	//   if err := tooLarge(res, int64(h.MaxBodySize)); err != nil {
	//     h.Log.Debugf("error in too-large: %v", err)
	//   }
	//   return
	// }

	// bucket := req.URL.Query().Get("bucket")

	body := req.Body
	// body = http.MaxBytesReader(res, body, int64(h.MaxBodySize))
	// Handle gzip request bodies
	if req.Header.Get("Content-Encoding") == "gzip" {
		var err error
		body, err = gzip.NewReader(body)
		if err != nil {
			// h.Log.Debugf("Error decompressing request body: %v", err.Error())
			// if err := badRequest(res, Invalid, err.Error()); err != nil {
			//   h.Log.Debugf("error in bad-request: %v", err)
			// }

			log.Error("msg", "Error decompressing request body", "err", err)
			return nil
		}
		defer body.Close()
	}

	var readErr error
	var bytes []byte
	//body = http.MaxBytesReader(res, req.Body, 1000000) //p.MaxBodySize.Size)
	bytes, readErr = io.ReadAll(body)
	if readErr != nil {
		// h.Log.Debugf("Error parsing the request body: %v", readErr.Error())
		// if err := badRequest(res, InternalError, readErr.Error()); err != nil {
		//   h.Log.Debugf("error in bad-request: %v", err)
		// }
		// return
		return readErr
	}

	precisionStr := req.URL.Query().Get("precision")

	var metrics []telegraf.Metric
	var err error
	// if h.ParserType == "upstream" {
	parser := influx_upstream.Parser{}
	err = parser.Init()
	if err != ErrEOF && err != nil {
		// h.Log.Debugf("Error initializing parser: %v", err.Error())
		return err
	}
	parser.SetTimeFunc(influx_upstream.TimeFunc(time.Now))

	if precisionStr != "" {
		precision := getPrecisionMultiplier(precisionStr)
		parser.SetTimePrecision(precision)
	}

	metrics, err = parser.Parse(bytes)
	// } else {
	//   parser := influx.Parser{}
	//   err = parser.Init()
	//   if err != ErrEOF && err != nil {
	//     h.Log.Debugf("Error initializing parser: %v", err.Error())
	//     return
	//   }
	//   parser.SetTimeFunc(h.timeFunc)

	//   if precisionStr != "" {
	//     precision := getPrecisionMultiplier(precisionStr)
	//     parser.SetTimePrecision(precision)
	//   }

	//   metrics, err = parser.Parse(bytes)
	// }

	if err != ErrEOF && err != nil {
		// h.Log.Debugf("Error parsing the request body: %v", err.Error())
		// if err := badRequest(res, Invalid, err.Error()); err != nil {
		//   h.Log.Debugf("error in bad-request: %v", err)
		// }
		log.Error("msg", "Error parsing the request body", "err", err)
		return err
	}

	// for _, m := range metrics {
	// Handle bucket_tag override
	// if h.BucketTag != "" && bucket != "" {
	//   m.AddTag(h.BucketTag, bucket)
	// }

	// h.acc.AddMetric(m)
	// }

	// http request success
	// res.WriteHeader(http.StatusNoContent)
	return SerializeBatch(metrics, wr)
}

func getPrecisionMultiplier(precision string) time.Duration {
	// Influxdb defaults silently to nanoseconds if precision isn't
	// one of the following:
	var d time.Duration
	switch precision {
	case "us":
		d = time.Microsecond
	case "ms":
		d = time.Millisecond
	case "s":
		d = time.Second
	default:
		d = time.Nanosecond
	}
	return d
}

func SerializeBatch(metrics []telegraf.Metric, wr *prompb.WriteRequest) error {
	var promTS = make([]prompb.TimeSeries, 0, len(metrics))
	for _, metric := range metrics {
		commonLabels := createLabels(metric)
		metricName := metric.Name()
		metricName, ok := SanitizeMetricName(metricName)
		if !ok {
			fmt.Println("unsantized metric_name")
			continue
		}
		_, ts := getPromTS(metricName, commonLabels, metric.Time(), metric.FieldList())
		promTS = append(promTS, ts)
	}

	// if s.config.MetricSortOrder == SortMetrics {
	sort.Slice(promTS, func(i, j int) bool {
		lhs := promTS[i].Labels
		rhs := promTS[j].Labels
		if len(lhs) != len(rhs) {
			return len(lhs) < len(rhs)
		}

		for index := range lhs {
			l := lhs[index]
			r := rhs[index]

			if l.Name != r.Name {
				return l.Name < r.Name
			}

			if l.Value != r.Value {
				return l.Value < r.Value
			}
		}

		return false
	})
	// }

	wr.Timeseries = promTS
	return nil
}

func createLabels(metric telegraf.Metric) []prompb.Label {
	labels := make([]prompb.Label, 0, len(metric.TagList()))
	for _, tag := range metric.TagList() {
		// Ignore special tags for histogram and summary types.
		switch metric.Type() {
		case telegraf.Histogram:
			if tag.Key == "le" {
				continue
			}
		case telegraf.Summary:
			if tag.Key == "quantile" {
				continue
			}
		}

		name, ok := prometheus.SanitizeLabelName(tag.Key)
		if !ok {
			continue
		}

		// remove tags with empty values
		if tag.Value == "" {
			continue
		}

		labels = append(labels, prompb.Label{Name: name, Value: tag.Value})
	}

	// if s.config.StringHandling != StringAsLabel {
	//   return labels
	// }

	addedFieldLabel := false
	for _, field := range metric.FieldList() {
		value, ok := field.Value.(string)
		if !ok {
			continue
		}

		name, ok := prometheus.SanitizeLabelName(field.Key)
		if !ok {
			continue
		}

		// If there is a tag with the same name as the string field, discard
		// the field and use the tag instead.
		if hasLabel(name, labels) {
			continue
		}

		labels = append(labels, prompb.Label{Name: name, Value: value})
		addedFieldLabel = true
	}

	if addedFieldLabel {
		sort.Slice(labels, func(i, j int) bool {
			return labels[i].Name < labels[j].Name
		})
	}

	return labels
}

func hasLabel(name string, labels []prompb.Label) bool {
	for _, label := range labels {
		if name == label.Name {
			return true
		}
	}
	return false
}

func getPromTS(name string, labels []prompb.Label, ts time.Time, fields []*telegraf.Field) (MetricKey, prompb.TimeSeries) {
	values := make(map[string]interface{}, len(fields))
	for _, field := range fields {
		val, ok := prometheus.SampleValue(field.Value)
		if !ok {
			log.Debug("msg", "couldn't parse sample value", "value", field.Value)
			continue
		}
		values[field.Key] = val
	}
	sample := []prompb.Sample{{
		// Timestamp is int milliseconds for remote write.
		Timestamp:  ts.UnixNano() / int64(time.Millisecond),
		MultiValue: values,
	}}
	labelscopy := make([]prompb.Label, len(labels), len(labels)+1)
	copy(labelscopy, labels)
	labels = append(labelscopy, prompb.Label{
		Name:  "__name__",
		Value: name,
	})

	// we sort the labels since Prometheus TSDB does not like out of order labels
	sort.Slice(labels, func(i, j int) bool {
		return labels[i].Name < labels[j].Name
	})

	return MakeMetricKey(labels), prompb.TimeSeries{Labels: labels, Samples: sample}
}

func MakeMetricKey(labels []prompb.Label) MetricKey {
	h := fnv.New64a()
	for _, label := range labels {
		h.Write([]byte(label.Name))  //nolint:revive // from hash.go: "It never returns an error"
		h.Write([]byte("\x00"))      //nolint:revive // from hash.go: "It never returns an error"
		h.Write([]byte(label.Value)) //nolint:revive // from hash.go: "It never returns an error"
		h.Write([]byte("\x00"))      //nolint:revive // from hash.go: "It never returns an error"
	}
	return MetricKey(h.Sum64())
}

type MetricKey uint64

// MetricName returns the Prometheus metric name.
func MetricName(measurement, fieldKey string, valueType telegraf.ValueType) string {
	switch valueType {
	case telegraf.Histogram, telegraf.Summary:
		switch {
		case strings.HasSuffix(fieldKey, "_bucket"):
			fieldKey = strings.TrimSuffix(fieldKey, "_bucket")
		case strings.HasSuffix(fieldKey, "_sum"):
			fieldKey = strings.TrimSuffix(fieldKey, "_sum")
		case strings.HasSuffix(fieldKey, "_count"):
			fieldKey = strings.TrimSuffix(fieldKey, "_count")
		}
	}

	if measurement == "prometheus" {
		return fieldKey
	}
	return measurement + "_" + fieldKey
}

// SanitizeMetricName checks if the name is a valid Prometheus metric name.  If
// not, it attempts to replaces invalid runes with an underscore to create a
// valid name.
func SanitizeMetricName(name string) (string, bool) {
	return sanitize(name, MetricNameTable)
}

// SanitizeLabelName checks if the name is a valid Prometheus label name.  If
// not, it attempts to replaces invalid runes with an underscore to create a
// valid name.
func SanitizeLabelName(name string) (string, bool) {
	return sanitize(name, LabelNameTable)
}

// Sanitize checks if the name is valid according to the table.  If not, it
// attempts to replaces invalid runes with an underscore to create a valid
// name.
func sanitize(name string, table Table) (string, bool) {
	if isValid(name, table) {
		return name, true
	}

	var b strings.Builder

	for i, r := range name {
		switch {
		case i == 0:
			if unicode.In(r, table.First) {
				b.WriteRune(r) //nolint:revive // from builder.go: "It returns the length of r and a nil error."
			}
		default:
			if unicode.In(r, table.Rest) {
				b.WriteRune(r) //nolint:revive // from builder.go: "It returns the length of r and a nil error."
			} else {
				b.WriteString("_") //nolint:revive // from builder.go: "It returns the length of s and a nil error."
			}
		}
	}

	name = strings.Trim(b.String(), "_")
	if name == "" {
		return "", false
	}

	return name, true
}

func isValid(name string, table Table) bool {
	if name == "" {
		return false
	}

	for i, r := range name {
		switch {
		case i == 0:
			if !unicode.In(r, table.First) {
				return false
			}
		default:
			if !unicode.In(r, table.Rest) {
				return false
			}
		}
	}

	return true
}

type Table struct {
	First *unicode.RangeTable
	Rest  *unicode.RangeTable
}

var MetricNameTable = Table{
	First: &unicode.RangeTable{
		R16: []unicode.Range16{
			{0x003A, 0x003A, 1}, // :
			{0x0041, 0x005A, 1}, // A-Z
			{0x005F, 0x005F, 1}, // _
			{0x0061, 0x007A, 1}, // a-z
		},
		LatinOffset: 4,
	},
	Rest: &unicode.RangeTable{
		R16: []unicode.Range16{
			{0x0030, 0x003A, 1}, // 0-:
			{0x0041, 0x005A, 1}, // A-Z
			{0x005F, 0x005F, 1}, // _
			{0x0061, 0x007A, 1}, // a-z
		},
		LatinOffset: 4,
	},
}

var LabelNameTable = Table{
	First: &unicode.RangeTable{
		R16: []unicode.Range16{
			{0x0041, 0x005A, 1}, // A-Z
			{0x005F, 0x005F, 1}, // _
			{0x0061, 0x007A, 1}, // a-z
		},
		LatinOffset: 3,
	},
	Rest: &unicode.RangeTable{
		R16: []unicode.Range16{
			{0x0030, 0x0039, 1}, // 0-9
			{0x0041, 0x005A, 1}, // A-Z
			{0x005F, 0x005F, 1}, // _
			{0x0061, 0x007A, 1}, // a-z
		},
		LatinOffset: 4,
	},
}

// SampleValue converts a field value into a value suitable for a simple sample value.
func SampleValue(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case int64:
		return float64(v), true
	case uint64:
		return float64(v), true
	case bool:
		if v {
			return 1.0, true
		}
		return 0.0, true
	default:
		return 0, false
	}
}
