package pgmodel

import (
	"database/sql"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/httpfs"
	"github.com/timescale/prometheus-postgresql-adapter/pkg/pgmodel/migrations"
)

// Migrate performs a database migration to the latest version
func Migrate(db *sql.DB) error {
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return err
	}

	src, err := httpfs.New(migrations.SqlFiles, "/")
	if err != nil {
		return err
	}

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
