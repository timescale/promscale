package series

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

const (
	someDB = "test"
)

var (
	errReturn = fmt.Errorf("some error")
)

type modifiedMockConnection struct {
	// Return error after this many calls.
	errorOcc int
	execOcc  int
	mockPGXConn
}

func (m *modifiedMockConnection) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	m.execOcc++
	m.ExecSQLs = append(m.ExecSQLs, sql)
	m.ExecArgs = append(m.ExecArgs, arguments)

	var err error
	if m.execOcc == m.errorOcc {
		err = m.ExecErr
	}

	return pgconn.CommandTag([]byte{}), err
}

func TestPGXCreatorInit(t *testing.T) {
	creator := pgxCreator{}

	creator.Init()

	if creator.NumPartitions != numPartitions {
		t.Errorf("wrong default set for number of partitions: got %d wanted %d", creator.NumPartitions, numPartitions)
	}

	if creator.ChunkTime != chunkTime {
		t.Errorf("wrong default set for chunk time: got %s wanted %s", creator.ChunkTime.String(), chunkTime.String())
	}

	partitions := 5
	chTime := 5 * time.Second

	creator = pgxCreator{
		NumPartitions: partitions,
		ChunkTime:     chTime,
	}

	creator.Init()

	if creator.NumPartitions != partitions {
		t.Errorf("wrong default set for number of partitions: got %d wanted %d", creator.NumPartitions, partitions)
	}

	if creator.ChunkTime != chTime {
		t.Errorf("wrong default set for chunk time: got %s wanted %s", creator.ChunkTime.String(), chTime.String())
	}

}

func TestPGXCreatorDBExists(t *testing.T) {
	mock := &mockPGXConn{
		QueryResults: []interface{}{1},
	}
	creator := pgxCreator{
		conn: mock,
	}

	exists, err := creator.DBExists(someDB)

	if err != nil {
		t.Errorf("unexpected error:\ngot\n%s\nwanted nil", err)
	}

	if !exists {
		t.Errorf("expected the database to exist, it does not")
	}

	if mock.QuerySQLs[0] != checkDBSQL {
		t.Errorf("unexpected SQL query:\ngot\n%s\nwanted\n%s", mock.QuerySQLs[0], checkDBSQL)
	}

	if mock.QueryArgs[0][0] != someDB {
		t.Errorf("unexpected args for query:\ngot\n%s\nwanted\n%s", mock.QueryArgs[0][0], someDB)
	}

	mock = &mockPGXConn{
		QueryErr: errReturn,
	}
	creator = pgxCreator{
		conn: mock,
	}

	exists, err = creator.DBExists(someDB)

	if err == nil {
		t.Errorf("expected error:\ngot\n%s\nwanted\n%s", err, errReturn)
	}

	if exists {
		t.Errorf("expected the database to not exist, it does exist")
	}

	if mock.QuerySQLs[0] != checkDBSQL {
		t.Errorf("unexpected SQL query:\ngot\n%s\nwanted\n%s", mock.QuerySQLs[0], checkDBSQL)
	}

	if mock.QueryArgs[0][0] != someDB {
		t.Errorf("unexpected args for query:\ngot\n%s\nwanted\n%s", mock.QueryArgs[0][0], someDB)
	}

}

func TestPGXCreatorRemoveOldDB(t *testing.T) {
	mock := &mockPGXConn{}
	creator := pgxCreator{
		conn: mock,
	}

	err := creator.RemoveOldDB(someDB)

	if err != nil {
		t.Errorf("unexpected error:\ngot\n%s\nwanted\nnil", err)
	}

	sql := fmt.Sprintf(removeDBSQL, someDB)

	if mock.ExecSQLs[0] != sql {
		t.Errorf("unexpected SQL query:\ngot\n%s\nwanted\n%s", mock.ExecSQLs[0], sql)
	}

	mock = &mockPGXConn{
		ExecErr: errReturn,
	}
	creator = pgxCreator{
		conn: mock,
	}

	err = creator.RemoveOldDB(someDB)

	if err == nil {
		t.Errorf("expected error:\ngot\n%s\nwanted\n%s", err, errReturn)
	}

	if mock.ExecSQLs[0] != sql {
		t.Errorf("unexpected SQL query:\ngot\n%s\nwanted\n%s", mock.ExecSQLs[0], sql)
	}

}

func TestPGXCreatorCreateDB(t *testing.T) {
	mock := &mockPGXConn{}
	creator := pgxCreator{
		conn: mock,
	}
	sql := fmt.Sprintf(createDBSQL, someDB)

	err := creator.CreateDB(someDB)

	if err != nil {
		t.Errorf("unexpected error:\ngot\n%s\nwanted\nnil", err)
	}

	if mock.ExecSQLs[0] != sql {
		t.Errorf("unexpected SQL query:\ngot\n%s\nwanted\n%s", mock.ExecSQLs[0], sql)
	}

	mock = &mockPGXConn{
		ExecErr: errReturn,
	}
	creator = pgxCreator{
		conn: mock,
	}

	err = creator.CreateDB(someDB)

	if err == nil {
		t.Errorf("expected error:\ngot\n%s\nwanted\n%s", err, errReturn)
	}

	if mock.ExecSQLs[0] != sql {
		t.Errorf("unexpected SQL query:\ngot\n%s\nwanted\n%s", mock.ExecSQLs[0], sql)
	}
}
func TestPGXCreatorRemoveSchema(t *testing.T) {
	mock := &mockPGXConn{}
	creator := pgxCreator{
		conn: mock,
	}

	err := creator.RemoveSchema(someDB)

	if err != nil {
		t.Errorf("unexpected error while removing schema:\ngot\n%s\nwanted\nnil", err)
	}

	if mock.ExecSQLs[0] != removeSchemaSQL {
		t.Errorf("unexpected remove schema sql\ngot\n%s\nwanted\n%s", mock.ExecSQLs[1], removeSchemaSQL)
	}

	mock = &mockPGXConn{
		ExecErr: errReturn,
	}
	creator = pgxCreator{
		conn: mock,
	}

	err = creator.RemoveSchema(someDB)

	if err == nil || err != errReturn {
		t.Errorf("unexpected or missing error while removing schema:\ngot\n%s\nwanted\n%s", err, errReturn)
	}
}

func TestPGXCreatorCreateSchema(t *testing.T) {
	mock := &mockPGXConn{}
	creator := pgxCreator{
		conn: mock,
	}

	err := creator.CreateSchema(someDB)

	if err == nil || err != ErrDBNotCreated {
		t.Errorf("expected error when no DB was created:\ngot\n%s\nwanted\n%s", err, ErrDBNotCreated)
	}

	mock = &mockPGXConn{}
	creator = pgxCreator{
		conn: mock,
	}
	creator.CreateDB(someDB)

	err = creator.CreateSchema(someDB)

	if err == nil || err != ErrNoMatHyperName {
		t.Errorf("expected error finding materialization hypertable name:\ngot\n%s\nwanted\n%s", err, ErrNoMatHyperName)
	}

	mock = &mockPGXConn{
		QueryResults: []interface{}{"hyperName"},
	}
	creator = pgxCreator{
		conn: mock,
	}

	creator.CreateDB(someDB)

	err = creator.CreateSchema(someDB)

	if err != nil {
		t.Errorf("unexpected error for schema creation:\ngot\n%s\nwanted\nnil", err)
	}

	mock = &mockPGXConn{
		QueryErr: errReturn,
	}
	creator = pgxCreator{
		conn: mock,
	}

	creator.CreateDB(someDB)

	err = creator.CreateSchema(someDB)

	if err == nil || err != errReturn {
		t.Errorf("unexpected error for schema creation:\ngot\n%s\nwanted\n%s", err, errReturn)
	}

	modMock := &modifiedMockConnection{
		mockPGXConn: mockPGXConn{
			QueryResults: []interface{}{"hyperName"},
			ExecErr:      errReturn,
		},
		errorOcc: 1,
	}
	creator = pgxCreator{
		conn: modMock,
	}

	creator.CreateDB(someDB)

	err = creator.CreateSchema(someDB)

	if err == nil || err != ErrDBNotCreated {
		t.Errorf("unexpected error for schema creation:\ngot\n%s\nwanted\n%s", err, ErrDBNotCreated)
	}

	modMock = &modifiedMockConnection{
		mockPGXConn: mockPGXConn{
			QueryResults: []interface{}{"hyperName"},
			ExecErr:      errReturn,
		},
		errorOcc: 2,
	}
	creator = pgxCreator{
		conn: modMock,
	}

	creator.CreateDB(someDB)

	err = creator.CreateSchema(someDB)

	if err == nil || err != errReturn {
		t.Errorf("unexpected error for schema creation:\ngot\n%s\nwanted\n%s", err, errReturn)
	}

	modMock = &modifiedMockConnection{
		mockPGXConn: mockPGXConn{
			QueryResults: []interface{}{"hyperName"},
			ExecErr:      errReturn,
		},
		errorOcc: 3,
	}
	creator = pgxCreator{
		conn: modMock,
	}

	creator.CreateDB(someDB)

	err = creator.CreateSchema(someDB)

	if err == nil || err != errReturn {
		t.Errorf("unexpected error for schema creation:\ngot\n%s\nwanted\n%s", err, errReturn)
	}

	modMock = &modifiedMockConnection{
		mockPGXConn: mockPGXConn{
			QueryResults: []interface{}{"hyperName"},
			ExecErr:      errReturn,
		},
		errorOcc: 4,
	}
	creator = pgxCreator{
		conn: modMock,
	}

	creator.CreateDB(someDB)

	err = creator.CreateSchema(someDB)

	if err == nil || err != errReturn {
		t.Errorf("unexpected error for schema creation:\ngot\n%s\nwanted\n%s", err, errReturn)
	}

	modMock = &modifiedMockConnection{
		mockPGXConn: mockPGXConn{
			QueryResults: []interface{}{"hyperName"},
			ExecErr:      errReturn,
		},
		errorOcc: 5,
	}
	creator = pgxCreator{
		conn: modMock,
	}

	creator.CreateDB(someDB)

	err = creator.CreateSchema(someDB)

	if err == nil || err != errReturn {
		t.Errorf("unexpected error for schema creation:\ngot\n%s\nwanted\n%s", err, errReturn)
	}
}

func TestNewPgxCreator(t *testing.T) {
	config, err := pgx.ParseConfig("")
	if err != nil {
		t.Fatal("unable to create config", err)
	}

	c := NewPgxCreator(config)

	if c.NumPartitions != numPartitions {
		t.Errorf("incorrect number of partitions set, not default:\ngot\n%d\nwanted\n%d", c.NumPartitions, numPartitions)
	}

	if c.ChunkTime != chunkTime {
		t.Errorf("incorrect chunk time set, not default:\ngot\n%s\nwanted\n%s", c.ChunkTime.String(), chunkTime.String())
	}
}
