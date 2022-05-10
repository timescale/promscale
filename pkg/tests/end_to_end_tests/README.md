Run these tests by executing `go test` in this directory. A specific test can
be targeted by running `go test -run <TestName>`.

The test binary takes a number of parameters which configure its runtime
behaviour. These are defined at the top of `main_test.go` in this directory.

Some particularly interesting options are:

- `-update`: Updates the golden SQL files which are used as a reference
- `-extended`: Run extended testing dataset and PromQL queries
- `-use-extension`: Use the promscale extension
- `-use-docker`: The test harness will start `timescaledb` in a docker
  container to test against. If set to `false`, the test harness will attempt
  to connect to `localhost:5432`
