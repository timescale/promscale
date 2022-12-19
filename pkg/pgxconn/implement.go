package pgxconn

import (
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// rowWithTelemetry wraps the row returned by QueryRow() for metrics telemetry.
type rowWithTelemetry struct {
	row pgx.Row
}

func (w rowWithTelemetry) Scan(dest ...interface{}) error {
	err := w.row.Scan(dest...)
	if err != nil && err != pgx.ErrNoRows {
		errorsTotal.With(promMethodLabel("query_row")).Inc()
	}
	return err
}

// rowsWithDuration wraps the Query() function with duration metric taken to complete the entire execution.
type rowsWithDuration struct {
	rows  pgx.Rows
	start time.Time
}

func newRowsWithDuration(rows pgx.Rows, start time.Time) rowsWithDuration {
	return rowsWithDuration{rows, start}
}

func (r rowsWithDuration) Next() bool {
	return r.rows.Next()
}

func (r rowsWithDuration) Scan(dest ...interface{}) error {
	return r.rows.Scan(dest...)
}

func (r rowsWithDuration) Err() error {
	return r.rows.Err()
}

func (r rowsWithDuration) Close() {
	defer func() { duration.With(promMethodLabel("query")).Observe(time.Since(r.start).Seconds()) }()
	r.rows.Close()
}

func (r rowsWithDuration) CommandTag() pgconn.CommandTag {
	return r.rows.CommandTag()
}

func (r rowsWithDuration) FieldDescriptions() []pgconn.FieldDescription {
	return r.rows.FieldDescriptions()
}

func (r rowsWithDuration) Values() ([]interface{}, error) {
	return r.rows.Values()
}

func (r rowsWithDuration) RawValues() [][]byte {
	return r.rows.RawValues()
}

// batchResultsWithDuration wraps the SendBatch and records the duration of the batch till it closed.
type batchResultsWithDuration struct {
	batch pgx.BatchResults
	start time.Time
}

func newBatchResultsWithDuration(b pgx.BatchResults, start time.Time) batchResultsWithDuration {
	return batchResultsWithDuration{
		batch: b,
		start: start,
	}
}

func (w batchResultsWithDuration) Close() error {
	defer func() { duration.With(promMethodLabel("send_batch")).Observe(time.Since(w.start).Seconds()) }()
	return w.batch.Close()
}

func (w batchResultsWithDuration) Exec() (pgconn.CommandTag, error) {
	return w.batch.Exec()
}

func (w batchResultsWithDuration) Query() (pgx.Rows, error) {
	return w.batch.Query()
}

func (w batchResultsWithDuration) QueryRow() pgx.Row {
	return rowWithTelemetry{w.batch.QueryRow()}
}
