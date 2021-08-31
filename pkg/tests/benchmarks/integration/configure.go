// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package integration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/promql"
	"github.com/timescale/promscale/pkg/query"
)

func getQueryEngine(t testing.TB) *promql.Engine {
	queryEngine, err := query.NewEngine(log.GetLogger(), 2*time.Minute, 5*time.Minute, 1*time.Minute, 50000000, []string{})
	require.NoError(t, err)

	return queryEngine
}
