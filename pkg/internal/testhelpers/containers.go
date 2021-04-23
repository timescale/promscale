// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package testhelpers

import (
	"context"
	"fmt"
	"io"
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
	testcontainers "github.com/testcontainers/testcontainers-go"
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

type ExtensionState int

const (
	timescaleBit  = 1 << iota
	promscaleBit  = 1 << iota
	timescale2Bit = 1 << iota
	multinodeBit  = 1 << iota
	postgres12Bit = 1 << iota
)

const (
	VanillaPostgres        ExtensionState = 0
	Timescale1             ExtensionState = timescaleBit
	Timescale1AndPromscale ExtensionState = timescaleBit | promscaleBit
	Timescale2             ExtensionState = timescaleBit | timescale2Bit
	Timescale2AndPromscale ExtensionState = timescaleBit | timescale2Bit | promscaleBit
	Multinode              ExtensionState = timescaleBit | timescale2Bit | multinodeBit
	MultinodeAndPromscale  ExtensionState = timescaleBit | timescale2Bit | multinodeBit | promscaleBit
)

func (e *ExtensionState) UseTimescaleDB() {
	*e |= timescaleBit
}

func (e *ExtensionState) UsePromscale() {
	*e |= timescaleBit | promscaleBit
}

func (e *ExtensionState) UseTimescale2() {
	*e |= timescaleBit | timescale2Bit
}

func (e *ExtensionState) UseMultinode() {
	*e |= timescaleBit | timescale2Bit | multinodeBit
}

func (e *ExtensionState) UsePG12() {
	*e |= postgres12Bit
}

func (e ExtensionState) UsesTimescaleDB() bool {
	return (e & timescaleBit) != 0
}

func (e ExtensionState) UsesTimescale2() bool {
	return (e & timescale2Bit) != 0
}

func (e ExtensionState) UsesMultinode() bool {
	return (e & multinodeBit) != 0
}

func (e ExtensionState) usesPromscale() bool {
	return (e & promscaleBit) != 0
}

func (e ExtensionState) UsesPG12() bool {
	return (e & postgres12Bit) != 0
}

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
func WithDB(t testing.TB, DBName string, superuser SuperuserStatus, deferNode2Setup bool, extensionState ExtensionState, f func(db *pgxpool.Pool, t testing.TB, connectString string)) {
	db, err := DbSetup(DBName, superuser, deferNode2Setup, extensionState)
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

func DbSetup(DBName string, superuser SuperuserStatus, deferNode2Setup bool, extensionState ExtensionState) (*pgxpool.Pool, error) {
	defaultDb, err := pgx.Connect(context.Background(), PgConnectURL(defaultDB, Superuser))
	if err != nil {
		return nil, err
	}
	err = func() error {
		_, err = defaultDb.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", DBName))
		if err != nil {
			return err
		}

		if extensionState.UsesMultinode() {
			dropDistributed := "CALL distributed_exec($$ DROP DATABASE IF EXISTS %s $$, transactional => false)"
			_, err = defaultDb.Exec(context.Background(), fmt.Sprintf(dropDistributed, DBName))
			if err != nil {
				return err
			}
		}

		_, err = defaultDb.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE %s OWNER %s", DBName, promUser))
		if err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		_ = defaultDb.Close(context.Background())
		return nil, err
	}

	err = defaultDb.Close(context.Background())
	if err != nil {
		return nil, err
	}

	ourDb, err := pgx.Connect(context.Background(), PgConnectURL(DBName, Superuser))
	if err != nil {
		return nil, err
	}

	if extensionState.UsesMultinode() {
		// Multinode requires the administrator to set up data nodes, so in
		// that case timescaledb should always be installed by the time the
		// connector runs. We just set up the data nodes here.
		err = AddDataNode1(ourDb, DBName)
		if err == nil && !deferNode2Setup {
			err = AddDataNode2(ourDb, DBName)
		}
		if err != nil {
			_ = ourDb.Close(context.Background())
			return nil, err
		}
	} else {
		// some docker images may have timescaledb installed in a template,
		// but we want to test our own timescale installation.
		// (Multinode requires the administrator to set up data nodes, so in
		// that case timescaledb should always be installed by the time the
		// connector runs)
		_, err = ourDb.Exec(context.Background(), "DROP EXTENSION IF EXISTS timescaledb")
		if err != nil {
			_ = ourDb.Close(context.Background())
			return nil, err
		}
	}

	if err = ourDb.Close(context.Background()); err != nil {
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
func StartPGContainer(
	ctx context.Context,
	extensionState ExtensionState,
	testDataDir string,
	printLogs bool,
) (testcontainers.Container, io.Closer, error) {
	var image string
	PGMajor := "13"
	if extensionState.UsesPG12() {
		PGMajor = "12"
	}
	PGTag := "pg" + PGMajor

	switch extensionState &^ postgres12Bit {
	case MultinodeAndPromscale:
		image = "timescaledev/promscale-extension:latest-ts2-" + PGTag
	case Multinode:
		image = "timescale/timescaledb:latest-" + PGTag
	case Timescale2AndPromscale:
		image = "timescaledev/promscale-extension:latest-ts2-" + PGTag
	case Timescale2:
		image = "timescale/timescaledb:latest-" + PGTag
	case Timescale1AndPromscale:
		if PGMajor != "12" {
			return nil, nil, fmt.Errorf("Timescaledb 1.x requires pg12")
		}
		image = "timescaledev/promscale-extension:latest-ts1-pg12"
	case Timescale1:
		if PGMajor != "12" {
			return nil, nil, fmt.Errorf("Timescaledb 1.x requires pg12")
		}
		image = "timescale/timescaledb:1.7.4-pg12"
	case VanillaPostgres:
		image = "postgres:" + PGMajor
	}

	return StartDatabaseImage(ctx, image, testDataDir, "", printLogs, extensionState)
}

func StartDatabaseImage(ctx context.Context,
	image string,
	testDataDir string,
	dataDir string,
	printLogs bool,
	extensionState ExtensionState,
) (testcontainers.Container, io.Closer, error) {
	c := CloseAll{}
	var networks []string
	if extensionState.UsesMultinode() {
		networkName, closer, err := createMultinodeNetwork()
		if err != nil {
			return nil, c, err
		}
		c.Append(closer)
		networks = []string{networkName}
	}

	startContainer := func(containerPort nat.Port) (testcontainers.Container, error) {
		container, closer, err := startPGInstance(
			ctx,
			image,
			testDataDir,
			dataDir,
			extensionState,
			printLogs,
			networks,
			containerPort,
		)
		if closer != nil {
			c.Append(closer)
		}
		return container, err
	}

	if extensionState.UsesMultinode() {
		containerPort := nat.Port("5433/tcp")
		_, err := startContainer(containerPort)
		if err != nil {
			_ = c.Close()
			return nil, nil, err
		}
		containerPort = nat.Port("5434/tcp")
		_, err = startContainer(containerPort)
		if err != nil {
			_ = c.Close()
			return nil, nil, err
		}
	}

	containerPort := nat.Port("5432/tcp")
	container, err := startContainer(containerPort)
	if err != nil {
		_ = c.Close()
		return nil, nil, err
	}
	if extensionState.UsesMultinode() {
		/* Setting up the cluster on the default db allows us to run distributed_exec commands on all nodes.
		* Currently, it's used for dropping databases, although may be used for other things in future.  */
		db, err := pgx.Connect(context.Background(), PgConnectURL(defaultDB, Superuser))
		if err != nil {
			_ = c.Close()
			return nil, nil, err
		}

		err = AddDataNode1(db, defaultDB)
		if err == nil {
			err = AddDataNode2(db, defaultDB)
		}
		if err != nil {
			_ = c.Close()
			return nil, nil, fmt.Errorf("Error adding nodes: %w", err)
		}

		err = db.Close(context.Background())
		if err != nil {
			_ = c.Close()
			return nil, nil, err
		}
	}

	return container, c, nil
}

func createMultinodeNetwork() (networkName string, closer func(), err error) {
	networkName = "promscale-network"
	networkRequest := testcontainers.GenericNetworkRequest{
		NetworkRequest: testcontainers.NetworkRequest{
			Driver:     "bridge",
			Name:       networkName,
			Attachable: true,
		},
	}
	network, err := testcontainers.GenericNetwork(context.Background(), networkRequest)
	if err != nil {
		return "", nil, err
	}
	closer = func() { _ = network.Remove(context.Background()) }
	return networkName, closer, nil
}

func startPGInstance(
	ctx context.Context,
	image string,
	testDataDir string,
	dataDir string,
	extensionState ExtensionState,
	printLogs bool,
	networks []string,
	containerPort nat.Port,
) (testcontainers.Container, func(), error) {
	// we map the access node to port 5432 and the others to other ports
	isDataNode := containerPort.Port() != "5432"
	nodeType := "AN"
	if isDataNode {
		nodeType = "DN" + containerPort.Port()
	}

	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{string(containerPort)},
		WaitingFor: wait.ForSQL(containerPort, "pgx", func(port nat.Port) string {
			return "dbname=postgres password=password user=postgres host=127.0.0.1 port=" + port.Port()
		}).Timeout(120 * time.Second),
		Env: map[string]string{
			"POSTGRES_PASSWORD": "password",
		},
		Networks:       networks,
		NetworkAliases: map[string][]string{"promscale-network": {"db" + containerPort.Port()}},
		SkipReaper:     false, /* switch to true not to kill docker container */
	}
	req.Cmd = []string{
		"-c", "max_connections=100",
		"-c", "port=" + containerPort.Port(),
		"-c", "max_prepared_transactions=150",
		"-c", "log_line_prefix=" + nodeType + " %m [%d]",
		"-i",
	}

	if extensionState.UsesTimescaleDB() {
		req.Cmd = append(req.Cmd,
			"-c", "shared_preload_libraries=timescaledb",
			"-c", "timescaledb.max_background_workers=0",
		)
	}

	if extensionState.usesPromscale() {
		req.Cmd = append(req.Cmd,
			"-c", "local_preload_libraries=pgextwlist",
			//timescale_prometheus_extra included for upgrade tests with old extension name
			"-c", "extwlist.extensions=promscale,timescaledb,timescale_prometheus_extra",
		)
	}

	req.BindMounts = make(map[string]string)
	if testDataDir != "" {
		req.BindMounts[testDataDir] = "/testdata"
	}
	if dataDir != "" {
		var bindDir string
		if isDataNode {
			bindDir = dataDir + "/dn" + containerPort.Port()
		} else {
			bindDir = dataDir + "/an"
		}
		if err := os.Mkdir(bindDir, 0700); err != nil && !os.IsExist(err) {
			return nil, nil, err
		}
		req.BindMounts[bindDir] = "/var/lib/postgresql/data"
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          false,
	})
	if err != nil {
		return nil, nil, err
	}

	if printLogs {
		container.FollowOutput(stdoutLogConsumer{})
	}

	err = container.Start(context.Background())
	if err != nil {
		return nil, nil, err
	}

	if printLogs {
		err = container.StartLogProducer(context.Background())
		if err != nil {
			fmt.Println("Error setting up logger", err)
			os.Exit(1)
		}
	}

	closer := func() {
		StopContainer(ctx, container, printLogs)
	}

	if isDataNode {
		err = setDataNodePermissions(container)
		if err != nil {
			return nil, closer, err
		}
	}

	pgHost, err = container.Host(ctx)
	if err != nil {
		return nil, closer, err
	}

	pgPort, err = container.MappedPort(ctx, containerPort)
	if err != nil {
		return nil, closer, err
	}

	db, err := pgx.Connect(context.Background(), PgConnectURL(defaultDB, Superuser))
	if err != nil {
		return nil, closer, err
	}

	_, err = db.Exec(context.Background(), fmt.Sprintf("CREATE USER %s WITH NOSUPERUSER CREATEROLE PASSWORD 'password'", promUser))
	if err != nil {
		//ignore duplicate errors
		pgErr, ok := err.(*pgconn.PgError)
		if !ok || pgErr.Code != "42710" {
			return nil, closer, err
		}
	}

	if isDataNode {
		// reload the pg_hba.conf settings we changed in setDataNodePermissions
		_, err = db.Exec(context.Background(), "SELECT pg_reload_conf()")
		if err != nil {
			return nil, closer, err
		}
	}

	err = db.Close(context.Background())
	if err != nil {
		return nil, closer, err
	}

	return container, closer, nil
}

func AddDataNode1(db *pgx.Conn, database string) error {
	return addDataNode(db, database, "dn0", "5433")
}

func AddDataNode2(db *pgx.Conn, database string) error {
	return addDataNode(db, database, "dn1", "5434")
}

func addDataNode(db *pgx.Conn, database string, nodeName string, nodePort string) error {
	_, err := db.Exec(context.Background(), "SELECT add_data_node('"+nodeName+"', host => 'db"+nodePort+"', port => "+nodePort+", if_not_exists=>true);")
	if err != nil {
		return err
	}

	_, err = db.Exec(context.Background(), "GRANT USAGE ON FOREIGN SERVER "+nodeName+" TO "+promUser)
	if err != nil {
		return err
	}

	grantDistributed := "CALL distributed_exec($$ GRANT ALL ON DATABASE %s TO %s $$, transactional => false)"
	_, err = db.Exec(context.Background(), fmt.Sprintf(grantDistributed, database, promUser))
	if err != nil {
		return err
	}
	return nil
}

func setDataNodePermissions(container testcontainers.Container) error {
	// trust all incoming connections on the datanodes so we can add them to
	// the cluster without too much hassle. We'll reload the pg_hba.conf later
	// NOTE: the setting must be prepended to pg_hba.conf so it overrides
	//       the latter, stricter, settings
	code, err := container.Exec(context.Background(), []string{
		"bash",
		"-c",
		"echo -e \"host all all 0.0.0.0/0 trust\n$(cat /var/lib/postgresql/data/pg_hba.conf)\"" +
			"> /var/lib/postgresql/data/pg_hba.conf"})
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("docker exec error. exit code: %d", code)
	}
	return nil
}

type CloseAll struct {
	toClose []func()
}

func (c *CloseAll) Append(closer func()) {
	c.toClose = append(c.toClose, closer)
}

func (c CloseAll) Close() error {
	for i := len(c.toClose) - 1; i >= 0; i-- {
		c.toClose[i]()
	}
	return nil
}

// StartPromContainer starts a Prometheus container for use in testing
// #nosec
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
		Image:        "prom/prometheus:main",
		ExposedPorts: []string{string(prometheusPort)},
		WaitingFor:   wait.ForListeningPort(prometheusPort),
		BindMounts: map[string]string{
			storagePath:    "/prometheus",
			promConfigFile: "/etc/prometheus/prometheus.yml",
		},
		Cmd: []string{
			// Default configuration.
			"--config.file=/etc/prometheus/prometheus.yml",
			"--storage.tsdb.path=/prometheus",
			"--web.console.libraries=/usr/share/prometheus/console_libraries",
			"--web.console.templates=/usr/share/prometheus/consoles",

			// Enable features.
			"--enable-feature=promql-at-modifier,promql-negative-offset",

			// This is to stop Prometheus from messing with the data.
			"--storage.tsdb.retention.time=30y",
			"--storage.tsdb.min-block-duration=30y",
			"--storage.tsdb.max-block-duration=30y",
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

func StartConnectorWithImage(ctx context.Context, dbContainer testcontainers.Container, image string, printLogs bool, cmds []string, dbname string) (testcontainers.Container, error) {
	dbUser := promUser
	if dbname == "postgres" {
		dbUser = "postgres"
	}
	dbHost := "172.17.0.1"
	if runtime.GOOS == "darwin" {
		dbHost = "host.docker.internal"
	}

	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{string(ConnectorPort)},
		WaitingFor:   wait.ForHTTP("/metrics").WithPort(ConnectorPort).WithAllowInsecure(true),
		SkipReaper:   false, /* switch to true not to kill docker container */
		Cmd: []string{
			"-db-host", dbHost,
			"-db-port", pgPort.Port(),
			"-db-user", dbUser,
			"-db-password", "password",
			"-db-name", dbname,
			"-db-ssl-mode", "prefer",
		},
	}

	req.Cmd = append(req.Cmd, cmds...)

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

	return container, nil
}

func StopContainer(ctx context.Context, container testcontainers.Container, printLogs bool) {
	if printLogs {
		err := container.StopLogProducer()
		if err != nil {
			fmt.Fprintln(os.Stderr, "couldn't stop log producer", err)
		}
	}

	err := container.Terminate(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "couldn't terminate container", err)
	}
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
