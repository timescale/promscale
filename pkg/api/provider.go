// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"sync"

	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/rules"
)

// Provider provides the API, access to the application level implementations.
// It must be created with its constructor so that none of its fields are nil.
type provider struct {
	client   *pgclient.Client
	rulesMux sync.RWMutex
	rules    rules.Manager
}

func NewProviderWith(client *pgclient.Client, manager rules.Manager) *provider {
	return &provider{
		client: client,
		rules:  manager,
	}
}

func (p *provider) UpdateRulesManager(m rules.Manager) {
	p.rulesMux.Lock()
	defer p.rulesMux.Unlock()
	p.rules = m
}

func (p *provider) RulesManager() rules.Manager {
	p.rulesMux.RLock()
	defer p.rulesMux.RUnlock()
	return p.rules
}
