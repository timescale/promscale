// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package read

import (
	"github.com/prometheus/prometheus/pkg/labels"
)

const regexOR = "|"

// Authorizer tells if a read request is allowed to query via Promscale.
type Authorizer interface {
	// ApplySafetyMatcher applies a safety matcher to incoming query matchers. This safety matcher is responsible
	// from prevent unauthorized query reads from tenants that the incoming query is not supposed to read.
	ApplySafetyMatcher(ms []*labels.Matcher) []*labels.Matcher
}
