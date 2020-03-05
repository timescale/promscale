package pgmodel

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
)

const (
	numPartitions = 1
	chunkTime     = 12 * time.Hour

	checkDBSQL  = "SELECT 1 from pg_database WHERE datname = $1"
	removeDBSQL = "DROP DATABASE IF EXISTS %s"
	createDBSQL = "CREATE DATABASE %s"

	removeSchemaSQL = `
		DROP TABLE IF EXISTS data; 
		DROP TABLE IF EXISTS labels; 
		DROP TABLE IF EXISTS series; 
		DROP VIEW IF EXISTS series_min_max CASCADE;`

	createSchemaSQL = `CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
		CREATE TABLE IF NOT EXISTS labels(
			name TEXT, 
			value TEXT, 
			constraint uniq2 unique(name, value)); 
		CREATE TABLE IF NOT EXISTS series(
			id SERIAL PRIMARY KEY, 
			fingerprint NUMERIC(20,0),  
			labels JSONB, constraint uniq1 unique(labels));
		CREATE INDEX idxginp 
			ON series 
			USING gin (labels jsonb_path_ops);
		CREATE TABLE IF NOT EXISTS data (
			time timestamptz, 
			value DOUBLE PRECISION, 
			series_id INTEGER)`

	createCaggSQL = `CREATE VIEW series_min_max
		WITH (timescaledb.continuous)
		AS
		SELECT time_bucket('12 hours', time) as bucket, series_id, min(time) as min_time, max(time) as max_time
		FROM data
    	GROUP BY bucket, series_id`

	getMatHypertableNameSQL = "SELECT materialization_hypertable FROM timescaledb_information.continuous_aggregates WHERE view_name = 'series_min_max'::regclass;"
)

var (
	// ErrDBNotCreated is returned when schema could not be created.
	ErrDBNotCreated = fmt.Errorf("unable to create schema, database not created")
	// ErrNoMatHyperName is returned when materialization hypertable name cannot be found.
	ErrNoMatHyperName = fmt.Errorf("no materialization hypertable name for created continuous agg series_min_max")
)

// pgxCreator implements the DBCreator interface used for creating the database and schema.
type pgxCreator struct {
	conn          pgxConn
	NumPartitions int
	ChunkTime     time.Duration

	created string
}

func NewPgxCreator(c *pgx.ConnConfig) *pgxCreator {
	p := &pgxCreator{
		conn: &PgxConnImpl{
			Config: c,
		},
	}

	p.Init()

	return p
}

// Init initializes the connection.
func (p *pgxCreator) Init() {
	// Set the defaults.
	if p.NumPartitions == 0 {
		p.NumPartitions = numPartitions
	}

	if p.ChunkTime == 0 {
		p.ChunkTime = chunkTime
	}
}

// DBExists checks if a database with the specified name already exists.
func (p *pgxCreator) DBExists(dbName string) (bool, error) {

	res, err := p.conn.Query(context.Background(), checkDBSQL, dbName)

	if err != nil {
		return false, err
	}
	defer res.Close()
	return res.Next(), nil
}

// RemoveOldDB removes the database with the specified name.
func (p *pgxCreator) RemoveOldDB(dbName string) error {
	_, err := p.conn.Exec(context.Background(), fmt.Sprintf(removeDBSQL, dbName))
	return err
}

// CreateDB creates the database with the specified name.
func (p *pgxCreator) CreateDB(dbName string) error {
	_, err := p.conn.Exec(context.Background(), fmt.Sprintf(createDBSQL, dbName))

	if err == nil {
		p.created = dbName
	}
	return err
}

func (p *pgxCreator) RemoveSchema(dbName string) error {
	p.conn.UseDatabase(dbName)

	_, err := p.conn.Exec(
		context.Background(),
		removeSchemaSQL)
	return err
}

func (p *pgxCreator) createHypertableSQL() string {
	return fmt.Sprintf(
		`SELECT create_hypertable( 			
		    'data'::regclass, 
			'time'::name, 
			number_partitions => %v::smallint, 
			chunk_time_interval => %d, 
			create_default_indexes=>FALSE)`,
		p.NumPartitions,
		p.ChunkTime.Nanoseconds()/1000)
}

func (p *pgxCreator) createCaggIndexSQL(hypertableName string) string {
	return fmt.Sprintf(
		`CREATE INDEX idxminmax 
		ON %s (series_id, agg_3_3, agg_4_4)`,
		hypertableName)
}

// CreateSchema creates the series schema used to store the metrics.
func (p *pgxCreator) CreateSchema(dbName string) error {
	if p.created != dbName {
		return ErrDBNotCreated
	}
	p.conn.UseDatabase(dbName)

	if _, err := p.conn.Exec(
		context.Background(),
		createSchemaSQL); err != nil {
		return err
	}

	if _, err := p.conn.Exec(context.Background(), p.createHypertableSQL()); err != nil {
		return err
	}

	if _, err := p.conn.Exec(
		context.Background(),
		createCaggSQL); err != nil {
		return err
	}

	r, err := p.conn.Query(
		context.Background(),
		getMatHypertableNameSQL)

	if err != nil {
		return err
	}

	defer r.Close()

	if !r.Next() {
		return ErrNoMatHyperName
	}

	var name string
	r.Scan(&name)

	if _, err := p.conn.Exec(
		context.Background(),
		p.createCaggIndexSQL(name)); err != nil {
		return err
	}
	return nil
}
