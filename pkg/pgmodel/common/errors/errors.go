// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package errors

import "fmt"

var (
	ErrNoMetricName                = fmt.Errorf("metric name missing")
	ErrNoClausesGen                = fmt.Errorf("no clauses generated")
	ErrEntryNotFound               = fmt.Errorf("entry not found")
	ErrInvalidCacheEntryType       = fmt.Errorf("invalid cache entry type stored")
	ErrInvalidRowData              = fmt.Errorf("invalid row data, length of arrays does not match")
	ErrExtUnavailable              = fmt.Errorf("the extension is not available")
	ErrMissingTableName            = fmt.Errorf("missing metric table name")
	ErrTimeBasedDeletion           = fmt.Errorf("time based series deletion is unsupported")
	ErrInvalidSemverFormat         = fmt.Errorf("app version is not semver format, aborting migration")
	ErrQueryMismatchTimestampValue = fmt.Errorf("query returned a mismatch in timestamps and values")

	ErrTmplMissingUnderlyingRelation = `the underlying table ("%s"."%s") which is used to store the metric` +
		"values has been moved/removed thus the data cannot be retrieved"
)
