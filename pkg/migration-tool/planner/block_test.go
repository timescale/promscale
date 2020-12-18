// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package planner

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateBlock(t *testing.T) {
	cases := []struct {
		name       string
		mint       int64
		maxt       int64
		fails      bool
		errMessage string
	}{
		{
			name: "mint_maxt_same",
			mint: 100 * minute,
			maxt: 100 * minute,
		},
		{
			name: "normal",
			mint: 100 * minute,
			maxt: 100*minute + 1,
		},
		{
			name:  "mint_less_than_maxt",
			mint:  100 * minute,
			maxt:  100*minute - 1,
			fails: true,
		},
		{
			name:  "mint_less_than_global_mint",
			mint:  100*minute - 1,
			maxt:  101 * minute,
			fails: true,
		},
		{
			name:  "maxt_greater_than_global_maxt",
			mint:  100 * minute,
			maxt:  110*minute + 1,
			fails: true,
		},
	}

	mint := 100 * minute
	maxt := 110 * minute
	plan := &Plan{
		config: &Config{
			Mint: mint,
			Maxt: maxt,
		},
		pbarMux: new(sync.Mutex),
		Quiet:   true,
	}

	for _, c := range cases {
		_, err := plan.createBlock(c.mint, c.maxt)
		if c.fails {
			assert.Error(t, err)
			continue
		}
		assert.NoError(t, err)
	}
}
