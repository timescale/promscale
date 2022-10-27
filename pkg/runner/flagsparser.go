// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
//
// https://github.com/peterbourgon/ff/blob/7a9748fa77b6d2664e5553540a9cb69d31978815/ffyaml/ffyaml.go
package runner

import (
	"fmt"
	"io"
	"strconv"

	"github.com/peterbourgon/ff/v3"
	"gopkg.in/yaml.v2"
)

// Modified version of
// https://github.com/peterbourgon/ff/blob/7a9748fa77b6d2664e5553540a9cb69d31978815/ffyaml/ffyaml.go
// that for the key `startup.dataset.config` supports two types of values:
//
// - A yaml string (notice the `|` after the `:`):
//
//   startup.dataset.config: |
//     metrics:
//        ...
//     traces:
//        ...
//
// - A yaml map:
//
//   startup.dataset.config:
//     metrics:
//        ...
//     traces:
//        ...
//
// When the value is defined as a yaml map it will be marshaled into a string.
//
// Parser is a parser for YAML file format. Flags and their values are read
// from the key/value pairs defined in the config file.
func Parser(r io.Reader, set func(name, value string) error) error {
	var m map[string]interface{}
	d := yaml.NewDecoder(r)
	if err := d.Decode(&m); err != nil && err != io.EOF {
		return ParseError{err}
	}
	for key, val := range m {
		var values []string
		var err error
		if key == "startup.dataset.config" {
			values, err = datasetConfigToStr(val)
		} else {
			values, err = valsToStrs(val)
		}
		if err != nil {
			return ParseError{err}
		}
		for _, value := range values {
			if err := set(key, value); err != nil {
				return err
			}
		}
	}
	return nil
}

func valsToStrs(val interface{}) ([]string, error) {
	if vals, ok := val.([]interface{}); ok {
		ss := make([]string, len(vals))
		for i := range vals {
			s, err := valToStr(vals[i])
			if err != nil {
				return nil, err
			}
			ss[i] = s
		}
		return ss, nil
	}
	s, err := valToStr(val)
	if err != nil {
		return nil, err
	}
	return []string{s}, nil

}

func valToStr(val interface{}) (string, error) {
	switch v := val.(type) {
	case byte:
		return string([]byte{v}), nil
	case string:
		return v, nil
	case bool:
		return strconv.FormatBool(v), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case int:
		return strconv.Itoa(v), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64), nil
	case nil:
		return "", nil
	default:
		return "", ff.StringConversionError{Value: val}
	}
}

func datasetConfigToStr(val interface{}) ([]string, error) {
	switch v := val.(type) {
	case string:
		return []string{v}, nil
	case nil:
		return []string{""}, nil
	case interface{}:
		s, err := yaml.Marshal(val)
		if err != nil {
			return nil, err
		}
		return []string{string(s)}, err
	default:
		return nil, ff.StringConversionError{Value: val}
	}
}

// ParseError wraps all errors originating from the YAML parser.
type ParseError struct {
	Inner error
}

// Error implenents the error interface.
func (e ParseError) Error() string {
	return fmt.Sprintf("error parsing YAML config: %v", e.Inner)
}

// Unwrap implements the errors.Wrapper interface, allowing errors.Is and
// errors.As to work with ParseErrors.
func (e ParseError) Unwrap() error {
	return e.Inner
}
