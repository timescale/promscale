// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgmodel

// DBCreator is used for initial setup of the database by preparing the correct
// series schema.
type DBCreator interface {
	// Init should set up any connection or other setup for talking to the DB,
	// but should NOT create any databases.
	Init()

	// DBExists checks if a database with the given name currently exists.
	DBExists(dbName string) bool

	// CreateDB creates a database with the given name.
	CreateDB(dbName string) error

	// RemoveOldDB removes an existing database with the given name.
	RemoveOldDB(dbName string) error

	// CreateSchema does further initialization after the database is created.
	CreateSchema(dbName string) error
}
