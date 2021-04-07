// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package multi_tenancy

import (
	"fmt"

	"github.com/timescale/promscale/pkg/multi-tenancy/config"
	"github.com/timescale/promscale/pkg/multi-tenancy/read"
	"github.com/timescale/promscale/pkg/multi-tenancy/write"
)

var ErrUnauthorized = fmt.Errorf("invalid token or tenant: ")

type MultiTenancy interface {
	// ReadAuthorizer returns a authorizer that authorizes read operations.
	ReadAuthorizer() read.Authorizer
	// WriteAuthorizer returns a authorizer that authorizes write operations.
	WriteAuthorizer() write.Authorizer
}

// multiTenancy type implements the multi-tenancy concept in Promscale.
type multiTenancy struct {
	write write.Authorizer
	read  read.Authorizer
}

// NewMultiTenancy returns a new MultiTenancy type.
func NewMultiTenancy(c config.Config) (MultiTenancy, error) {
	readAuthr, err := read.NewAuthorizer(c)
	if err != nil {
		return nil, fmt.Errorf("creating multi-tenancy: %w", err)
	}
	writeAuthr := write.NewAuthorizer(c)
	if err != nil {
		return nil, fmt.Errorf("creating multi-tenancy: %w", err)
	}
	return &multiTenancy{
		read:  readAuthr,
		write: writeAuthr,
	}, nil
}

func (mt *multiTenancy) ReadAuthorizer() read.Authorizer {
	return mt.read
}

func (mt *multiTenancy) WriteAuthorizer() write.Authorizer {
	return mt.write
}

type noopMultiTenancy struct{}

// NewNoopMultiTenancy returns a No-op multi-tenancy that is used to initialize multi-tenancy types for no operations.
func NewNoopMultiTenancy() MultiTenancy {
	return &noopMultiTenancy{}
}

func (np *noopMultiTenancy) ReadAuthorizer() read.Authorizer {
	return nil
}

func (np *noopMultiTenancy) WriteAuthorizer() write.Authorizer {
	return nil
}
