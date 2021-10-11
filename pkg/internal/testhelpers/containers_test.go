// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package testhelpers

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"google.golang.org/grpc"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jaegertracing/jaeger/proto-gen/api_v3"
	"github.com/stretchr/testify/require"
)

var (
	useDocker    = flag.Bool("use-docker", true, "start database using a docker container")
	testDatabase = flag.String("database", "tmp_db_timescale_migrate_test", "database to run integration tests on")
)

const extensionState = timescaleBit | promscaleBit | postgres12Bit

func TestPGConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	db, err := pgx.Connect(context.Background(), PgConnectURL(defaultDB, Superuser))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(context.Background())
	var res int
	err = db.QueryRow(context.Background(), "SELECT 1").Scan(&res)
	if err != nil {
		t.Fatal(err)
	}
	if res != 1 {
		t.Errorf("Res is not 1 but %d", res)
	}
}

func TestOtelCollectorConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping checking Jaeger connection")
	}
	jContainer, jaegerHost, jaegerContainerIP, jaegerReceivingPort, grpcQueryPort, _, err := StartJaegerContainer(true)
	require.NoError(t, err)
	defer jContainer.Terminate(context.Background())

	// Check if Jaeger is up.
	opts := []grpc.DialOption{grpc.WithBlock(), grpc.WithInsecure()}
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", jaegerHost, grpcQueryPort.Port()), opts...)
	require.NoError(t, err)

	c := api_v3.NewQueryServiceClient(conn)
	services, err := c.GetServices(context.Background(), &api_v3.GetServicesRequest{})
	require.NoError(t, err)
	require.Equal(t, 0, len(services.Services)) // As the database is empty.

	// Start Otel collector.
	otelContainer, _, _, err := StartOtelCollectorContainer(fmt.Sprintf("%s:%s", jaegerContainerIP, jaegerReceivingPort.Port()), true)
	require.NoError(t, err)
	defer otelContainer.Terminate(context.Background())
}

func TestJaegerConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping checking Jaeger connection")
	}
	container, host, _, _, grpcQueryPort, uiPort, err := StartJaegerContainer(true)
	require.NoError(t, err)
	defer container.Terminate(context.Background())

	const servicesEndpoint = "api/services"

	resp, err := http.Get(fmt.Sprintf("http://%s:%s/%s", host, uiPort, servicesEndpoint))
	require.NoError(t, err)
	require.True(t, resp.StatusCode == http.StatusOK)

	bSlice, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.True(t, len(bSlice) > 0)

	opts := []grpc.DialOption{grpc.WithBlock(), grpc.WithInsecure()}
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", host, grpcQueryPort.Port()), opts...)
	require.NoError(t, err)

	c := api_v3.NewQueryServiceClient(conn)
	services, err := c.GetServices(context.Background(), &api_v3.GetServicesRequest{})
	require.NoError(t, err)
	require.Equal(t, 0, len(services.Services)) // As the database is empty.
}

func TestWithDB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	WithDB(t, *testDatabase, Superuser, false, extensionState, func(db *pgxpool.Pool, t testing.TB, connectURL string) {
		var res int
		err := db.QueryRow(context.Background(), "SELECT 1").Scan(&res)
		if err != nil {
			t.Fatal(err)
		}
		if res != 1 {
			t.Errorf("Res is not 1 but %d", res)
		}
	})
}

func runMain(m *testing.M) int {
	flag.Parse()
	ctx := context.Background()
	if !testing.Short() && *useDocker {
		_, closer, err := StartPGContainer(ctx, extensionState, "", false)
		if err != nil {
			fmt.Println("Error setting up container", err)
			os.Exit(1)
		}

		tmpDir := ""
		if runtime.GOOS == "darwin" {
			// Docker on Mac lacks access to default os tmp dir - "/var/folders/random_number"
			// so switch to cross-user tmp dir
			tmpDir = "/tmp"
		}
		path, err := ioutil.TempDir(tmpDir, "prom_test")
		if err != nil {
			fmt.Println("Error getting temp dir for Prometheus storage", err)
			os.Exit(1)
		}
		err = os.Mkdir(filepath.Join(path, "wal"), 0777)
		if err != nil {
			fmt.Println("Error getting temp dir for Prometheus storage", err)
			os.Exit(1)
		}
		promContainer, _, _, err := StartPromContainer(path, ctx)
		if err != nil {
			fmt.Println("Error setting up container", err)
			os.Exit(1)
		}
		defer func() {
			_ = closer.Close()
			err = promContainer.Terminate(ctx)
			if err != nil {
				panic(err)
			}
		}()
	}
	return m.Run()
}
func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}
