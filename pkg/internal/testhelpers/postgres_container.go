// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package testhelpers

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/timescale/promscale/pkg/pgmodel/model"

	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	defaultDB       = "postgres"
	connectTemplate = "postgres://%s:password@%s:%d/%s"

	postgresUser    = "postgres"
	promUser        = "prom"
	emptyPromConfig = "global:\n  scrape_interval: 10s\nstorage:\n  exemplars:\n    max_exemplars: 100000"

	Superuser   = true
	NoSuperuser = false
)

type Result interface {
	Failed() bool
}

var (
	users = []string{
		promUser,
		"prom_reader_user",
		"prom_writer_user",
		"prom_modifier_user",
		"prom_admin_user",
		"prom_maintenance_user",
	}
)

type OptionState int

type TestOptions struct {
	options              OptionState
	timescaleDockerImage string
}

func NewTestOptions(options OptionState, timescaleDockerImage string) TestOptions {
	return TestOptions{
		options,
		timescaleDockerImage,
	}
}

const (
	timescaleBit        = 1 << iota
	multinodeBit        = 1 << iota
	timescaleNightlyBit = 1 << iota
)

const (
	Timescale OptionState = timescaleBit
	Multinode OptionState = timescaleBit | multinodeBit

	TimescaleNightly          OptionState = timescaleBit | timescaleNightlyBit
	TimescaleNightlyMultinode OptionState = timescaleBit | multinodeBit | timescaleNightlyBit
)

func (e *TestOptions) UseTimescaleDB() {
	e.options |= timescaleBit
}

func (e *TestOptions) UseTimescaleNightly() {
	e.options |= timescaleBit | timescaleNightlyBit
}

func (e *TestOptions) UseTimescaleNightlyMultinode() {
	e.options |= timescaleBit | multinodeBit | timescaleNightlyBit
}

func (e *TestOptions) UseMultinode() {
	e.options |= timescaleBit | multinodeBit
}

func (e *TestOptions) SetTimescaleDockerImage(timescaleDockerImage string) {
	e.timescaleDockerImage = timescaleDockerImage
}

func (e TestOptions) UsesTimescaleDB() bool {
	return (e.options & timescaleBit) != 0
}

func (e TestOptions) UsesTimescaleNightly() bool {
	return (e.options & timescaleNightlyBit) != 0
}

func (e TestOptions) UsesMultinode() bool {
	return (e.options & multinodeBit) != 0
}

func (e TestOptions) GetDockerImageName() string {
	return e.timescaleDockerImage
}

func (e TestOptions) TryGetDockerImagePgVersion() (string, error) {
	r, err := regexp.Compile(`pg(\d+)`)
	if err != nil {
		return "", err
	}
	m := r.FindStringSubmatch(e.timescaleDockerImage)
	if len(m) < 2 {
		return "", fmt.Errorf("unable to determine postgres version")
	}
	return m[1], nil
}

var (
	pgHost          = "localhost"
	pgPort nat.Port = "5432/tcp"
)

type SuperuserStatus = bool

func PgConnectURL(dbName string, superuser SuperuserStatus) string {
	user := postgresUser
	if !superuser {
		user = promUser
	}
	return PgConnectURLUser(dbName, user)
}

func PgConnectURLUser(dbName string, user string) string {
	return fmt.Sprintf(connectTemplate, user, pgHost, pgPort.Int(), dbName)
}

func getRoleUser(role string) string {
	return role + "_user"
}

func setupRole(t testing.TB, dbName string, role string) {
	user := getRoleUser(role)
	dbOwner, err := pgx.Connect(context.Background(), PgConnectURL(dbName, Superuser))
	assert.NoError(t, err)
	defer dbOwner.Close(context.Background())

	_, err = dbOwner.Exec(context.Background(), fmt.Sprintf("CALL _prom_catalog.execute_everywhere(NULL, $$ GRANT %s TO %s $$);", role, user))
	assert.NoError(t, err)
}

func MakePromUserPromAdmin(t testing.TB, dbName string) {
	db, err := pgx.Connect(context.Background(), PgConnectURL(dbName, Superuser))
	assert.NoError(t, err)
	defer db.Close(context.Background())

	_, err = db.Exec(context.Background(), fmt.Sprintf("CALL _prom_catalog.execute_everywhere('distributed_prom_user', $$ GRANT prom_admin TO %s $$);", promUser))
	assert.NoError(t, err)
}

func PgxPoolWithRole(t testing.TB, dbName string, role string) *pgxpool.Pool {
	user := getRoleUser(role)
	setupRole(t, dbName, role)
	pool, err := PgxPoolWithRegisteredTypes(PgConnectURLUser(dbName, user))
	require.NoError(t, err)
	return pool
}

func PgxPoolWithRegisteredTypes(connectURL string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(connectURL)
	if err != nil {
		return nil, err
	}
	config.AfterConnect = model.RegisterCustomPgTypes
	return pgxpool.NewWithConfig(context.Background(), config)
}

// WithDB establishes a database for testing and calls the callback
func WithDB(t testing.TB, DBName string, superuser SuperuserStatus, deferNode2Setup bool, extensionState TestOptions, f func(db *pgxpool.Pool, t testing.TB, connectString string)) {
	db, err := DbSetup(DBName, superuser, deferNode2Setup, extensionState)
	defer model.UnRegisterCustomPgTypes(db.Config().ConnConfig.Config)
	if err != nil {
		t.Fatal(err)
		return
	}
	defer db.Close()
	f(db, t, PgConnectURL(DBName, superuser))
}

func GetReadOnlyConnection(t testing.TB, DBName string) *pgxpool.Pool {
	role := "prom_reader"
	user := getRoleUser(role)
	setupRole(t, DBName, role)

	pgConfig, err := pgxpool.ParseConfig(PgConnectURLUser(DBName, user))
	assert.NoError(t, err)

	pgConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
		if err != nil {
			return err
		}
		return model.RegisterCustomPgTypes(ctx, conn)
	}

	dbPool, err := pgxpool.NewWithConfig(context.Background(), pgConfig)
	if err != nil {
		t.Fatal(err)
	}

	return dbPool
}

func DbSetup(DBName string, superuser SuperuserStatus, deferNode2Setup bool, extensionState TestOptions) (*pgxpool.Pool, error) {
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
			dropDistributed := "CALL public.distributed_exec($$ DROP DATABASE IF EXISTS %s $$, transactional => false)"
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
	model.UnRegisterCustomPgTypes(ourDb.Config().Config)

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
		// Some docker images may also have timescaledb_toolkit installed in
		// a template, but this messes with our upgrade tests because toolkit
		// was not always present. It's easier to remove it here and install it
		// in the specific tests which require toolkit.
		_, err = ourDb.Exec(context.Background(), "DROP EXTENSION IF EXISTS timescaledb_toolkit")
		if err != nil {
			_ = ourDb.Close(context.Background())
			return nil, err
		}
	}

	if err = ourDb.Close(context.Background()); err != nil {
		return nil, err
	}

	dbPool, err := pgxpool.New(context.Background(), PgConnectURL(DBName, superuser))
	if err != nil {
		return nil, err
	}
	return dbPool, nil
}

// StartPGContainer starts a postgreSQL container for use in testing
func StartPGContainer(
	ctx context.Context,
	t Result,
	extensionState TestOptions,
	testDataDir string,
	printLogs bool,
) (testcontainers.Container, io.Closer, error) {
	image := extensionState.GetDockerImageName()

	return StartDatabaseImage(ctx, t, image, testDataDir, "", printLogs, extensionState)
}

func StartDatabaseImage(ctx context.Context,
	t Result,
	image string,
	testDataDir string,
	dataDir string,
	printLogs bool,
	extensionState TestOptions,
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
			t,
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
			return nil, nil, fmt.Errorf("error adding nodes: %w", err)
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

func dropTimescaleAndToolkitExtensionsInDB(dbname string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	db, err := pgx.Connect(ctx, PgConnectURL(dbname, Superuser))
	if err != nil {
		return fmt.Errorf("could not connect to %s db: %w", dbname, err)
	}

	_, err = db.Exec(context.Background(), "DROP EXTENSION timescaledb")
	if err != nil {
		return fmt.Errorf("could not uninstall timescaledb: %w", err)
	}
	_, err = db.Exec(context.Background(), "DROP EXTENSION IF EXISTS timescaledb_toolkit")
	if err != nil {
		return fmt.Errorf("could not uninstall timescaledb: %w", err)
	}
	err = db.Close(context.Background())
	if err != nil {
		return err
	}
	return nil
}

// updateTimescaleDBInDatabase updates the timescaledb version in a named database
func updateTimescaleDBInDatabase(database string) error {
	ctx := context.Background()
	db, err := pgx.Connect(ctx, PgConnectURL(database, Superuser))
	if err != nil {
		return fmt.Errorf("could not connect to %s db: %w", database, err)
	}
	defer db.Close(ctx)

	_, err = db.Exec(context.Background(), "ALTER EXTENSION timescaledb UPDATE")

	if err != nil {
		return fmt.Errorf("unable to update timescaledb: %w", err)
	}

	return nil
}

func removeTimescaleAndToolkitExtensions(container testcontainers.Container) error {
	err := dropTimescaleAndToolkitExtensionsInDB("template1")
	if err != nil {
		return fmt.Errorf("error dropping timescale extension in template1 db: %w", err)
	}

	err = dropTimescaleAndToolkitExtensionsInDB("postgres")
	if err != nil {
		return fmt.Errorf("error dropping timescale extension in postgres db: %w", err)
	}

	code, err := container.Exec(context.Background(), []string{
		"bash",
		"-c",
		"rm `pg_config --sharedir`/extension/timescaledb* && rm `pg_config --pkglibdir`/timescaledb-*"})
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("docker exec error. exit code: %d", code)
	}

	return nil
}

// updateTimescaleDB updates the version of TimescaleDB installed in all databases in the target container
func updateTimescaleDB() error {
	// We don't know which databases have TimescaleDB installed. In order to
	// determine that, we need to connect to a database. The timescale/timescaledb-ha
	// docker image installs timescaledb into the template1 database, so that's
	// a good place to start. We update timescaledb in that database.
	err := updateTimescaleDBInDatabase("template1")
	if err != nil {
		return fmt.Errorf("error updating timescale extension in template1 db: %w", err)
	}

	ctx := context.Background()
	db, err := pgx.Connect(ctx, PgConnectURL("template1", Superuser))
	if err != nil {
		return fmt.Errorf("could not connect to template1 db: %w", err)
	}
	defer db.Close(ctx)

	// Note: We don't actually know which databases have timescaledb installed
	// in them. Instead of spending time trying to find out, we assume that
	// they all do. This is probably mostly correct.
	rows, err := db.Query(ctx, "SELECT datname as name FROM pg_database;")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var database string
		err = rows.Scan(&database)
		if err != nil {
			return err
		}
		if database == "template0" || database == "template1" {
			continue
		}
		err := updateTimescaleDBInDatabase(database)
		if err != nil {
			return fmt.Errorf("error updating timescale extension in %s db: %w", database, err)
		}
	}
	return nil
}

func startPGInstance(
	ctx context.Context,
	t Result,
	image string,
	testDataDir string,
	dataDir string,
	extensionState TestOptions,
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

	cmd := []string{
		"-c", "max_connections=100",
		"-c", "port=" + containerPort.Port(),
		"-c", "max_prepared_transactions=150",
		"-c", "log_line_prefix=" + nodeType + " %m [%d]",
		"-i",
	}

	if extensionState.UsesTimescaleDB() {
		cmd = append(cmd,
			"-c", "shared_preload_libraries=timescaledb",
			"-c", "timescaledb.max_background_workers=0",
		)
	}

	cmd = append(cmd,
		"-c", "local_preload_libraries=pgextwlist",
		//timescale_prometheus_extra included for upgrade tests with old extension name
		"-c", "extwlist.extensions=promscale,timescaledb,timescale_prometheus_extra",
	)

	var mounts []testcontainers.ContainerMount
	if testDataDir != "" {
		err := os.Chmod(testDataDir, 0777) // #nosec
		if err != nil {
			return nil, nil, err
		}
		mounts = append(mounts, testcontainers.BindMount(testDataDir, "/testdata"))
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
		err := os.Chmod(bindDir, 0777) // #nosec
		if err != nil {
			return nil, nil, err
		}
		mounts = append(mounts, testcontainers.BindMount(bindDir, "/var/lib/postgresql"))
	}

	req := testcontainers.ContainerRequest{
		Cmd:          cmd,
		Image:        image,
		ExposedPorts: []string{string(containerPort)},
		WaitingFor: wait.ForSQL(containerPort, "pgx", func(port nat.Port) string {
			return "dbname=postgres password=password user=postgres host=127.0.0.1 port=" + port.Port()
		}).Timeout(60 * time.Second),
		Env: map[string]string{
			"POSTGRES_PASSWORD": "password",
			"PGDATA":            "/var/lib/postgresql/data",
		},
		Networks:       networks,
		NetworkAliases: map[string][]string{"promscale-network": {"db" + containerPort.Port()}},
		Mounts:         testcontainers.Mounts(mounts...),
		SkipReaper:     false, /* switch to true not to kill docker container */
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          false,
	})
	if err != nil {
		return nil, nil, err
	}

	if printLogs {
		container.FollowOutput(stdoutLogConsumer{"postgres"})
	}

	err = container.Start(context.Background())
	if err != nil {
		PrintContainerLogs(container)
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
		StopContainer(ctx, container, printLogs, t)
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
	if !extensionState.UsesTimescaleDB() {
		err = removeTimescaleAndToolkitExtensions(container)
		if err != nil {
			return nil, closer, err
		}
	} else {
		err = updateTimescaleDB()
		if err != nil {
			return nil, closer, err
		}
	}

	for _, username := range users {
		_, err = db.Exec(context.Background(), fmt.Sprintf("CREATE USER %s WITH NOSUPERUSER CREATEROLE PASSWORD 'password'", username))
		if err != nil {
			//ignore duplicate errors
			pgErr, ok := err.(*pgconn.PgError)
			if !ok || pgErr.Code != "42710" {
				return nil, closer, err
			}
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
	_, err := db.Exec(context.Background(), "SELECT public.add_data_node('"+nodeName+"', host => 'db"+nodePort+"', port => "+nodePort+", if_not_exists=>true);")
	if err != nil {
		return err
	}

	for _, username := range users {
		_, err = db.Exec(context.Background(), "GRANT USAGE ON FOREIGN SERVER "+nodeName+" TO "+username)
		if err != nil {
			return err
		}
	}
	grantDistributed := "CALL public.distributed_exec($$ GRANT ALL ON DATABASE %s TO %s $$, transactional => false)"
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

var ConnectorPort = nat.Port("9201/tcp")

func StartConnectorWithImage(ctx context.Context, dbContainer testcontainers.Container, image string, version semver.Version, printLogs bool, cmds []string, dbname string) (testcontainers.Container, error) {
	dbUser := promUser
	if dbname == "postgres" {
		dbUser = "postgres"
	}
	dbHost := "172.17.0.1"
	if runtime.GOOS == "darwin" {
		dbHost = "host.docker.internal"
	}

	// In Promscale 0.10.0 we renamed some flags. We need to keep the old ones around for older promscale versions (used in upgrade tests).
	var cmd []string
	if version.GE(semver.MustParse("0.10.0")) {

		cmd = []string{
			"-db.host", dbHost,
			"-db.port", pgPort.Port(),
			"-db.user", dbUser,
			"-db.password", "password",
			"-db.name", dbname,
			"-db.ssl-mode", "prefer",
		}
	} else {
		cmd = []string{
			"-db-host", dbHost,
			"-db-port", pgPort.Port(),
			"-db-user", dbUser,
			"-db-password", "password",
			"-db-name", dbname,
			"-db-ssl-mode", "prefer",
		}
	}

	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{string(ConnectorPort)},
		WaitingFor:   wait.ForHTTP("/metrics").WithPort(ConnectorPort).WithAllowInsecure(true),
		SkipReaper:   false, /* switch to true not to kill docker container */
		Cmd:          append(cmd, cmds...),
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

	return container, nil
}
