package api

import (
	"io"
	"math"
	"strconv"
	"unicode/utf8"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/timescale/promscale/pkg/promql"
)

func marshalVectorResponse(writer io.Writer, data promql.Vector, warnings []string) error {
	out := &errorWrapper{writer: writer}
	marshalCommonHeader(out)
	marshalVectorData(out, data)
	marshalCommonFooter(out, warnings)
	return out.err
}

func marshalCommonHeader(out *errorWrapper) {
	out.WriteStrings(`{"status":"success","data":`)
}

func marshalCommonFooter(out *errorWrapper, warnings []string) {
	{
		if len(warnings) != 0 {
			out.WriteStrings(`,"warnings":[`)
			for i, warning := range warnings {
				open := `,"`
				if i == 0 {
					open = open[1:]
				}
				out.WriteStrings(open)
				out.WriteEscapedString(warning, true)
				out.WriteStrings(`"`)
			}
			out.WriteStrings(`]`)
		}
	}
	out.WriteStrings("}\n")
}

func marshalMatrixData(out *errorWrapper, data promql.Matrix) {
	out.WriteStrings(`{"resultType":"`, string(parser.ValueTypeMatrix), `","result":[`)
	{
		for i, data := range data {
			open := `,{"metric":{`
			if i == 0 {
				open = open[1:]
			}
			out.WriteStrings(open)
			marshalLabels(out, data.Metric)
			out.WriteStrings(`},"values":[`)
			for i, point := range data.Points {
				open = ",["
				if i == 0 {
					open = open[1:]
				}
				out.WriteStrings(open)
				out.writeJsonFloat(float64(point.T) / 1000)
				out.WriteStrings(`,"`)
				out.writeFloat(point.V)
				out.WriteStrings(`"]`)
			}
			out.WriteStrings(`]}`)
		}
	}
	out.WriteStrings(`]}`)
}

func marshalVectorData(out *errorWrapper, data promql.Vector) {
	out.WriteStrings(`{"resultType":"`, string(parser.ValueTypeVector), `","result":[`)
	{
		floatLen := 0
		for i, data := range data {
			open := `,{"metric":{`
			if i == 0 {
				open = open[1:]
			}
			out.WriteStrings(open)
			marshalLabels(out, data.Metric)
			out.WriteStrings(`},"value":[`)
			{
				if floatLen == 0 {
					floatLen = out.writeJsonFloat(float64(data.Point.T) / 1000)
				} else {
					out.writeCachedJsonFloat(floatLen)
				}
				out.WriteStrings(`,"`)
				out.writeFloat(data.Point.V)
				out.WriteStrings(`"`)
			}
			out.WriteStrings(`]}`)
		}
	}
	out.WriteStrings(`]}`)
}

func marshalLabels(out *errorWrapper, labels labels.Labels) {
	for i, label := range labels {
		open := `,"`
		if i == 0 {
			open = `"`
		}
		out.WriteStrings(open)
		out.WriteEscapedString(label.Name, true)
		out.WriteStrings(`":"`)
		out.WriteEscapedString(label.Value, true)
		out.WriteStrings(`"`)
	}
}

type errorWrapper struct {
	err          error
	writer       io.Writer
	jsonScratch  [64]byte
	floatScratch []byte
}

func (self *errorWrapper) WriteStrings(strings ...string) {
	if self.err != nil {
		return
	}
	for _, s := range strings {
		_, err := io.WriteString(self.writer, s)
		if err != nil {
			self.err = err
			return
		}
	}
}

func (self *errorWrapper) WriteBytes(b ...byte) {
	if self.err != nil {
		return
	}
	_, err := self.writer.Write(b)
	if err != nil {
		self.err = err
		return
	}
}

func (self *errorWrapper) writeFloat(f float64) {
	self.floatScratch = self.floatScratch[:0]
	self.floatScratch = strconv.AppendFloat(self.floatScratch, f, 'f', -1, 64)
	self.WriteBytes(self.floatScratch...)
}

func (self *errorWrapper) writeCachedJsonFloat(length int) {
	self.WriteBytes(self.jsonScratch[:length]...)
}

// from floatEncoder
func (self *errorWrapper) writeJsonFloat(f float64) int {
	// Convert as if by ES6 number to string conversion.
	// This matches most other JSON generators.
	// See golang.org/issue/6384 and golang.org/issue/14135.
	// Like fmt %g, but the exponent cutoffs are different
	// and exponents themselves are not padded to two digits.
	b := self.jsonScratch[:0]
	abs := math.Abs(f)
	fmt := byte('f')
	// only supports float64
	if abs != 0 {
		if abs < 1e-6 || abs >= 1e21 {
			fmt = 'e'
		}
	}
	b = strconv.AppendFloat(b, f, fmt, -1, 64)
	if fmt == 'e' {
		// clean up e-09 to e-9
		n := len(b)
		if n >= 4 && b[n-4] == 'e' && b[n-3] == '-' && b[n-2] == '0' {
			b[n-2] = b[n-1]
			b = b[:n-1]
		}
	}
	self.WriteBytes(b...)
	return len(b)
}

var hex = "0123456789abcdef"

func (self *errorWrapper) WriteEscapedString(s string, escapeHTML bool) {
	if self.err != nil {
		return
	}
	//from json/encode.go
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if htmlSafeSet[b] || (!escapeHTML && safeSet[b]) {
				i++
				continue
			}
			if start < i {
				self.WriteStrings(s[start:i])
			}

			switch b {
			case '\\':
				self.WriteStrings(`\\`)
			case '"':
				self.WriteStrings(`\"`)
			case '\n':
				self.WriteStrings(`\n`)
			case '\r':
				self.WriteStrings(`\r`)
			case '\t':
				self.WriteStrings(`\t`)
			default:
				// This encodes bytes < 0x20 except for \t, \n and \r.
				// If escapeHTML is set, it also escapes <, >, and &
				// because they can lead to security holes when
				// user-controlled strings are rendered into JSON
				// and served to some browsers.
				self.WriteStrings(`\u00`)
				self.WriteBytes(hex[b>>4], hex[b&0xF])
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRuneInString(s[i:])
		if c == utf8.RuneError && size == 1 {
			if start < i {
				self.WriteStrings(s[start:i])
			}
			self.WriteStrings(`\ufffd`)
			i += size
			start = i
			continue
		}
		// U+2028 is LINE SEPARATOR.
		// U+2029 is PARAGRAPH SEPARATOR.
		// They are both technically valid characters in JSON strings,
		// but don't work in JSONP, which has to be evaluated as JavaScript,
		// and can lead to security holes there. It is valid JSON to
		// escape them, so we do so unconditionally.
		// See http://timelessrepo.com/json-isnt-a-javascript-subset for discussion.
		if c == '\u2028' || c == '\u2029' {
			if start < i {
				self.WriteStrings(s[start:i])
			}
			self.WriteStrings(`\u202`)
			self.WriteBytes(hex[c&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	if start < len(s) {
		self.WriteStrings(s[start:])
	}
}

// safeSet holds the value true if the ASCII character with the given array
// position can be represented inside a JSON string without any further
// escaping.
//
// All values are true except for the ASCII control characters (0-31), the
// double quote ("), and the backslash character ("\").
var safeSet = [utf8.RuneSelf]bool{
	' ':      true,
	'!':      true,
	'"':      false,
	'#':      true,
	'$':      true,
	'%':      true,
	'&':      true,
	'\'':     true,
	'(':      true,
	')':      true,
	'*':      true,
	'+':      true,
	',':      true,
	'-':      true,
	'.':      true,
	'/':      true,
	'0':      true,
	'1':      true,
	'2':      true,
	'3':      true,
	'4':      true,
	'5':      true,
	'6':      true,
	'7':      true,
	'8':      true,
	'9':      true,
	':':      true,
	';':      true,
	'<':      true,
	'=':      true,
	'>':      true,
	'?':      true,
	'@':      true,
	'A':      true,
	'B':      true,
	'C':      true,
	'D':      true,
	'E':      true,
	'F':      true,
	'G':      true,
	'H':      true,
	'I':      true,
	'J':      true,
	'K':      true,
	'L':      true,
	'M':      true,
	'N':      true,
	'O':      true,
	'P':      true,
	'Q':      true,
	'R':      true,
	'S':      true,
	'T':      true,
	'U':      true,
	'V':      true,
	'W':      true,
	'X':      true,
	'Y':      true,
	'Z':      true,
	'[':      true,
	'\\':     false,
	']':      true,
	'^':      true,
	'_':      true,
	'`':      true,
	'a':      true,
	'b':      true,
	'c':      true,
	'd':      true,
	'e':      true,
	'f':      true,
	'g':      true,
	'h':      true,
	'i':      true,
	'j':      true,
	'k':      true,
	'l':      true,
	'm':      true,
	'n':      true,
	'o':      true,
	'p':      true,
	'q':      true,
	'r':      true,
	's':      true,
	't':      true,
	'u':      true,
	'v':      true,
	'w':      true,
	'x':      true,
	'y':      true,
	'z':      true,
	'{':      true,
	'|':      true,
	'}':      true,
	'~':      true,
	'\u007f': true,
}

// htmlSafeSet holds the value true if the ASCII character with the given
// array position can be safely represented inside a JSON string, embedded
// inside of HTML <script> tags, without any additional escaping.
//
// All values are true except for the ASCII control characters (0-31), the
// double quote ("), the backslash character ("\"), HTML opening and closing
// tags ("<" and ">"), and the ampersand ("&").
var htmlSafeSet = [utf8.RuneSelf]bool{
	' ':      true,
	'!':      true,
	'"':      false,
	'#':      true,
	'$':      true,
	'%':      true,
	'&':      false,
	'\'':     true,
	'(':      true,
	')':      true,
	'*':      true,
	'+':      true,
	',':      true,
	'-':      true,
	'.':      true,
	'/':      true,
	'0':      true,
	'1':      true,
	'2':      true,
	'3':      true,
	'4':      true,
	'5':      true,
	'6':      true,
	'7':      true,
	'8':      true,
	'9':      true,
	':':      true,
	';':      true,
	'<':      false,
	'=':      true,
	'>':      false,
	'?':      true,
	'@':      true,
	'A':      true,
	'B':      true,
	'C':      true,
	'D':      true,
	'E':      true,
	'F':      true,
	'G':      true,
	'H':      true,
	'I':      true,
	'J':      true,
	'K':      true,
	'L':      true,
	'M':      true,
	'N':      true,
	'O':      true,
	'P':      true,
	'Q':      true,
	'R':      true,
	'S':      true,
	'T':      true,
	'U':      true,
	'V':      true,
	'W':      true,
	'X':      true,
	'Y':      true,
	'Z':      true,
	'[':      true,
	'\\':     false,
	']':      true,
	'^':      true,
	'_':      true,
	'`':      true,
	'a':      true,
	'b':      true,
	'c':      true,
	'd':      true,
	'e':      true,
	'f':      true,
	'g':      true,
	'h':      true,
	'i':      true,
	'j':      true,
	'k':      true,
	'l':      true,
	'm':      true,
	'n':      true,
	'o':      true,
	'p':      true,
	'q':      true,
	'r':      true,
	's':      true,
	't':      true,
	'u':      true,
	'v':      true,
	'w':      true,
	'x':      true,
	'y':      true,
	'z':      true,
	'{':      true,
	'|':      true,
	'}':      true,
	'~':      true,
	'\u007f': true,
}
