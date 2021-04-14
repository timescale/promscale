// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package tenancy

import "fmt"

// Authorizer authorizes the read/write operations in multi-tenancy.
type Authorizer interface {
	// ReadAuthorizer returns a authorizer that authorizes read operations.
	ReadAuthorizer() ReadAuthorizer
	// WriteAuthorizer returns a authorizer that authorizes write operations.
	WriteAuthorizer() WriteAuthorizer
}

// multiTenancy type implements the tenancy concept in Promscale.
type genericAuthorizer struct {
	write WriteAuthorizer
	read  ReadAuthorizer
}

// NewAuthorizer returns a new MultiTenancy type.
func NewAuthorizer(c AuthConfig) (Authorizer, error) {
	readAuthr, err := NewReadAuthorizer(c)
	if err != nil {
		return nil, fmt.Errorf("creating tenancy: %w", err)
	}
	writeAuthr := NewWriteAuthorizer(c)
	return &genericAuthorizer{
		read:  readAuthr,
		write: writeAuthr,
	}, nil
}

func (mt *genericAuthorizer) ReadAuthorizer() ReadAuthorizer {
	return mt.read
}

func (mt *genericAuthorizer) WriteAuthorizer() WriteAuthorizer {
	return mt.write
}

type noopAuthorizer struct{}

// NewNoopAuthorizer returns a No-op tenancy that is used to initialize tenancy types for no operations.
func NewNoopAuthorizer() Authorizer {
	return &noopAuthorizer{}
}

func (np *noopAuthorizer) ReadAuthorizer() ReadAuthorizer {
	return nil
}

func (np *noopAuthorizer) WriteAuthorizer() WriteAuthorizer {
	return nil
}
