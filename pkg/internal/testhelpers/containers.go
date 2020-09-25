// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package testhelpers

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	defaultDB       = "postgres"
	connectTemplate = "postgres://%s:password@%s:%d/%s"

	postgresUser    = "postgres"
	promUser        = "prom"
	emptyPromConfig = "global:\n  scrape_interval: 10s"

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

func PgConnectURL(dbName string, superuser SuperuserStatus) string {
	user := postgresUser
	if !superuser {
		user = promUser
	}
	return fmt.Sprintf(connectTemplate, user, pgHost, pgPort.Int(), dbName)
}

// WithDB establishes a database for testing and calls the callback
func WithDB(t testing.TB, DBName string, superuser SuperuserStatus, f func(db *pgxpool.Pool, t testing.TB, connectString string)) {
	db, err := DbSetup(DBName, superuser)
	if err != nil {
		t.Fatal(err)
		return
	}
	defer db.Close()
	f(db, t, PgConnectURL(DBName, superuser))
}

func GetReadOnlyConnection(t testing.TB, DBName string) *pgxpool.Pool {
	dbPool, err := pgxpool.Connect(context.Background(), PgConnectURL(DBName, NoSuperuser))
	if err != nil {
		t.Fatal(err)
	}

	_, err = dbPool.Exec(context.Background(), "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
	if err != nil {
		t.Fatal(err)
	}

	return dbPool
}

func DbSetup(DBName string, superuser SuperuserStatus) (*pgxpool.Pool, error) {
	db, err := pgx.Connect(context.Background(), PgConnectURL(defaultDB, Superuser))
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

	dbPool, err := pgxpool.Connect(context.Background(), PgConnectURL(DBName, superuser))
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
	var image string
	if withExtension {
		image = "timescaledev/promscale-extension:latest-pg12"
	} else {
		image = "timescale/timescaledb:latest-pg12"
	}
	return StartPGContainerWithImage(ctx, image, testDataDir, "", printLogs, withExtension)
}

func StartPGContainerWithImage(ctx context.Context, image string, testDataDir string, dataDir string, printLogs bool, withExtension bool) (testcontainers.Container, error) {
	containerPort := nat.Port("5432/tcp")

	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{string(containerPort)},
		WaitingFor: wait.ForSQL(containerPort, "pgx", func(port nat.Port) string {
			return "dbname=postgres password=password user=postgres host=127.0.0.1 port=" + port.Port()
		}).Timeout(60 * time.Second),
		Env: map[string]string{
			"POSTGRES_PASSWORD": "password",
		},
		SkipReaper: false, /* switch to true not to kill docker container */
	}

	if withExtension {
		req.Cmd = []string{
			"-c", "shared_preload_libraries=timescaledb",
			"-c", "local_preload_libraries=pgextwlist",
			//timescale_prometheus_extra included for upgrade tests with old extension name
			"-c", "extwlist.extensions=promscale,timescaledb,timescale_prometheus_extra",
		}
	}

	req.BindMounts = make(map[string]string)
	if testDataDir != "" {
		req.BindMounts[testDataDir] = "/testdata"
	}
	if dataDir != "" {
		req.BindMounts[dataDir] = "/var/lib/postgresql/data"
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

	if printLogs {
		err = container.StartLogProducer(context.Background())
		if err != nil {
			fmt.Println("Error setting up logger", err)
			os.Exit(1)
		}
	}

	err = container.Start(context.Background())
	if err != nil {
		return nil, err
	}

	pgHost, err = container.Host(ctx)
	if err != nil {
		return nil, err
	}

	pgPort, err = container.MappedPort(ctx, containerPort)
	if err != nil {
		return nil, err
	}

	db, err := pgx.Connect(context.Background(), PgConnectURL(defaultDB, Superuser))
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(context.Background(), fmt.Sprintf("CREATE USER %s WITH NOSUPERUSER CREATEROLE PASSWORD 'password'", promUser))
	if err != nil {
		//ignore duplicate errors
		pgErr, ok := err.(*pgconn.PgError)
		if !ok || pgErr.Code != "42710" {
			return nil, err
		}
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

	promConfigFile := filepath.Join(storagePath, "prometheus.yml")
	err = ioutil.WriteFile(promConfigFile, []byte(emptyPromConfig), 0777)
	if err != nil {
		return nil, err
	}
	prometheusPort := nat.Port("9090/tcp")
	req := testcontainers.ContainerRequest{
		Image:        "prom/prometheus",
		ExposedPorts: []string{string(prometheusPort)},
		WaitingFor:   wait.ForListeningPort(prometheusPort),
		BindMounts: map[string]string{
			storagePath:    "/prometheus",
			promConfigFile: "/etc/prometheus/prometheus.yml",
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

var ConnectorPort = nat.Port("9201/tcp")

func StartConnectorWithImage(ctx context.Context, image string, printLogs bool, dbname string) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{string(ConnectorPort)},
		WaitingFor:   wait.ForHTTP("/write").WithPort(ConnectorPort).WithAllowInsecure(true),
		SkipReaper:   false, /* switch to true not to kill docker container */
		Cmd: []string{
			"-db-host", "172.17.0.1", // IP refering to the docker's host network
			"-db-port", pgPort.Port(),
			"-db-user", promUser,
			"-db-password", "password",
			"-db-name", dbname,
			"-db-ssl-mode", "prefer",
			"-web-listen-address", "0.0.0.0:" + ConnectorPort.Port(),
		},
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

	if printLogs {
		err = container.StartLogProducer(context.Background())
		if err != nil {
			fmt.Println("Error setting up logger", err)
			os.Exit(1)
		}
	}

	err = container.Start(context.Background())
	if err != nil {
		return nil, err
	}

	return container, nil
}

func StopContainer(ctx context.Context, container testcontainers.Container, printLogs bool) {
	if printLogs {
		_ = container.StopLogProducer()
	}

	_ = container.Terminate(ctx)
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
