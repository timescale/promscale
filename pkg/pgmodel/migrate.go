// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgmodel

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/httpfs"
	"github.com/timescale/timescale-prometheus/pkg/pgmodel/migrations"
)

type mySrc struct {
	source.Driver
}

func (t *mySrc) replaceSchemaNames(r io.ReadCloser) (io.ReadCloser, error) {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(r)
	if err != nil {
		return r, err
	}
	err = r.Close()
	if err != nil {
		return r, err
	}
	s := buf.String()
	s = strings.ReplaceAll(s, "SCHEMA_CATALOG", catalogSchema)
	s = strings.ReplaceAll(s, "SCHEMA_PROM", promSchema)
	s = strings.ReplaceAll(s, "SCHEMA_SERIES", seriesViewSchema)
	s = strings.ReplaceAll(s, "SCHEMA_METRIC", metricViewSchema)
	r = ioutil.NopCloser(strings.NewReader(s))
	return r, err
}

func (t *mySrc) ReadUp(version uint) (r io.ReadCloser, identifier string, err error) {
	r, identifier, err = t.Driver.ReadUp(version)
	if err != nil {
		return
	}
	r, err = t.replaceSchemaNames(r)
	return
}

func (t *mySrc) ReadDown(version uint) (r io.ReadCloser, identifier string, err error) {
	r, identifier, err = t.Driver.ReadDown(version)
	if err != nil {
		return
	}
	r, err = t.replaceSchemaNames(r)
	return
}

// Migrate performs a database migration to the latest version
func Migrate(db *sql.DB) error {
	// The migration table will be put in the public schema not in any of our schema because we never want to drop it and
	// our scripts and our last down script drops our shemas
	driver, err := postgres.WithInstance(db, &postgres.Config{MigrationsTable: fmt.Sprintf("%s_schema_migrations", promSchema)})
	if err != nil {
		return err
	}

	src, err := httpfs.New(migrations.SqlFiles, "/")
	if err != nil {
		return err
	}
	src = &mySrc{src}

	m, err := migrate.NewWithInstance("SqlFiles", src, "Postgresql", driver)
	if err != nil {
		return err
	}

	err = m.Up()
	sErr, dErr := m.Close()
	if sErr != nil {
		return sErr
	}
	if dErr != nil {
		return dErr
	}
	return err
}
