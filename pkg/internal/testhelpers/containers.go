// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package testhelpers

import (
	"context"
	"fmt"
	"io/ioutil"
	"runtime"

	"os"
	"path/filepath"
	"testing"

	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	defaultDB       = "postgres"
	connectTemplate = "postgres://%s:password@%s:%d/%s"

	postgresUser = "postgres"
	promUser     = "prom"

	Superuser   = true
	NoSuperuser = false
)

var (
	PromHost          = "localhost"
	PromPort nat.Port = "9090/tcp"

	pgHost          = "localhost"
	pgPort nat.Port = "5432/tcp"
)

type SuperuserStatus = bool

func pgConnectURL(dbName string, superuser SuperuserStatus) string {
	user := postgresUser
	if !superuser {
		user = promUser
	}
	return fmt.Sprintf(connectTemplate, user, pgHost, pgPort.Int(), dbName)
}

// WithDB establishes a database for testing and calls the callback
func WithDB(t testing.TB, DBName string, superuser SuperuserStatus, f func(db *pgxpool.Pool, t testing.TB, connectString string)) {
	db, err := dbSetup(DBName, superuser)
	if err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		db.Close()
	}()
	f(db, t, pgConnectURL(DBName, superuser))
}

func GetReadOnlyConnection(t testing.TB, DBName string) *pgxpool.Pool {
	dbPool, err := pgxpool.Connect(context.Background(), pgConnectURL(DBName, NoSuperuser))
	if err != nil {
		t.Fatal(err)
	}

	_, err = dbPool.Exec(context.Background(), "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
	if err != nil {
		t.Fatal(err)
	}

	return dbPool
}

func dbSetup(DBName string, superuser SuperuserStatus) (*pgxpool.Pool, error) {
	db, err := pgx.Connect(context.Background(), pgConnectURL(defaultDB, Superuser))
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", DBName))
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE %s OWNER %s", DBName, promUser))
	if err != nil {
		return nil, err
	}

	err = db.Close(context.Background())
	if err != nil {
		return nil, err
	}

	dbPool, err := pgxpool.Connect(context.Background(), pgConnectURL(DBName, superuser))
	if err != nil {
		return nil, err
	}
	return dbPool, nil
}

type stdoutLogConsumer struct {
}

func (s stdoutLogConsumer) Accept(l testcontainers.Log) {
	if l.LogType == testcontainers.StderrLog {
		fmt.Print(l.LogType, " ", string(l.Content))
	} else {
		fmt.Print(string(l.Content))
	}

}

// StartPGContainer starts a postgreSQL container for use in testing
func StartPGContainer(ctx context.Context, withExtension bool, testDataDir string, printLogs bool) (testcontainers.Container, error) {
	containerPort := nat.Port("5432/tcp")
	var image string
	if withExtension {
		image = "timescaledev/timescale_prometheus_extra:latest-pg12"
	} else {
		image = "timescale/timescaledb:latest-pg12"
	}
	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{string(containerPort)},
		WaitingFor: wait.ForSQL(containerPort, "pgx", func(port nat.Port) string {
			return "dbname=postgres password=password user=postgres host=127.0.0.1 port=" + port.Port()
		}),
		Env: map[string]string{
			"POSTGRES_PASSWORD": "password",
		},
		SkipReaper: false, /* switch to true not to kill docker container */
	}

	if testDataDir != "" {
		req.BindMounts = map[string]string{
			testDataDir: "/testdata",
		}
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          false,
	})
	if err != nil {
		return nil, err
	}

	if printLogs {
		container.FollowOutput(stdoutLogConsumer{})
	}

	err = container.Start(context.Background())
	if err != nil {
		return nil, err
	}
	if printLogs {
		err = container.StartLogProducer(context.Background())
		if err != nil {
			fmt.Println("Error setting up logger", err)
			os.Exit(1)
		}
	}

	pgHost, err = container.Host(ctx)
	if err != nil {
		return nil, err
	}

	pgPort, err = container.MappedPort(ctx, containerPort)
	if err != nil {
		return nil, err
	}

	db, err := pgx.Connect(context.Background(), pgConnectURL(defaultDB, Superuser))
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(context.Background(), fmt.Sprintf("CREATE USER %s WITH NOSUPERUSER CREATEROLE PASSWORD 'password'", promUser))
	if err != nil {
		return nil, err
	}

	err = db.Close(context.Background())
	if err != nil {
		return nil, err
	}

	return container, nil
}

// StartPromContainer starts a Prometheus container for use in testing
func StartPromContainer(storagePath string, ctx context.Context) (testcontainers.Container, error) {
	// Set the storage directories permissions so Prometheus can write to them.
	err := os.Chmod(storagePath, 0777)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(filepath.Join(storagePath, "wal"), 0777); err != nil {
		return nil, err
	}

	prometheusPort := nat.Port("9090/tcp")

	req := testcontainers.ContainerRequest{
		Image:        "prom/prometheus",
		ExposedPorts: []string{string(prometheusPort)},
		WaitingFor:   wait.ForListeningPort(prometheusPort),
		BindMounts: map[string]string{
			storagePath: "/prometheus",
		},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	PromHost, err = container.Host(ctx)
	if err != nil {
		return nil, err
	}

	PromPort, err = container.MappedPort(ctx, prometheusPort)
	if err != nil {
		return nil, err
	}

	return container, nil
}

// TempDir returns a temp directory for tests
func TempDir(name string) (string, error) {
	tmpDir := ""

	if runtime.GOOS == "darwin" {
		// Docker on Mac lacks access to default os tmp dir - "/var/folders/random_number"
		// so switch to cross-user tmp dir
		tmpDir = "/tmp"
	}
	return ioutil.TempDir(tmpDir, name)
}
