// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package planner

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateSlab(t *testing.T) {
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
			Mint:      mint,
			Maxt:      maxt,
			NumStores: 1,
		},
		pbarMux: new(sync.Mutex),
		Quiet:   true,
	}

	for _, c := range cases {
		_, err := plan.createSlab(c.mint, c.maxt)
		if c.fails {
			assert.Error(t, err)
			continue
		}
		assert.NoError(t, err)
	}
}

func TestStoreTimeRanges(t *testing.T) {
	cases := []struct {
		name      string
		numStores int
		mint      int64
		maxt      int64
	}{
		{
			name:      "test-1",
			mint:      1 * minute,
			maxt:      100 * minute,
			numStores: 1,
		},
		{
			name:      "test-2",
			mint:      1 * minute,
			maxt:      100 * minute,
			numStores: 2,
		},
		{
			name:      "test-3",
			mint:      1 * minute,
			maxt:      100 * minute,
			numStores: 3,
		},
		{
			name:      "test-4",
			mint:      1 * minute,
			maxt:      100 * minute,
			numStores: 4,
		},
		{
			name:      "test-5",
			mint:      1 * minute,
			maxt:      2 * minute,
			numStores: 10,
		},
		{
			name:      "verify-integral-division-1",
			mint:      1 * minute,
			maxt:      2 * minute,
			numStores: 11,
		},
		{
			name:      "verify-integral-division-2",
			mint:      1 * minute,
			maxt:      2 * minute,
			numStores: 17,
		},
		{
			name:      "verify-integral-division-2",
			mint:      1 * minute,
			maxt:      2 * minute,
			numStores: 29,
		},
	}

	for _, c := range cases {
		config := &Config{
			Mint:      c.mint,
			Maxt:      c.maxt,
			NumStores: c.numStores,
		}
		plan, _, err := Init(config)
		assert.NoError(t, err)
		blockRef, err := plan.createSlab(c.mint, c.maxt)
		assert.NoError(t, err)
		// Verify time-ranges.
		var netTime int64
		for i := range blockRef.stores {
			r := blockRef.stores[i].maxt - blockRef.stores[i].mint
			netTime += r
		}
		assert.Equal(t, c.maxt-c.mint, netTime, c.name)
	}
}
