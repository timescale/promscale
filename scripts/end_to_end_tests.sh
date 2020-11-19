#!/usr/bin/env bash

set -euf -o pipefail

PASSED=0
FAILED=0

TIMESCALE_IMAGE=${1:-"timescale/timescaledb:latest-pg12"}
SCRIPT_DIR=$(cd $(dirname ${0}) && pwd)
ROOT_DIR=$(dirname ${SCRIPT_DIR})
DB_URL="localhost:5432"
CONNECTOR_URL="localhost:9201"
PROM_URL="localhost:9090"

CONF=$(mktemp)

chmod 777 $CONF

echo "running tests against ${TIMESCALE_IMAGE}"

echo "scrape_configs:
  - job_name: 'connector'
    scrape_interval: 5s
    static_configs:
      - targets: ['localhost:9201']
remote_read:
- url: http://$CONNECTOR_URL/read
  remote_timeout: 1m
  read_recent: true

remote_write:
- url: http://$CONNECTOR_URL/write
  remote_timeout: 1m" > $CONF

cleanup() {
    rm $CONF
    docker stop e2e-prom || true
    if [ -n "$CONN_PID" ]; then
        kill $CONN_PID
    fi
    docker stop e2e-tsdb || true
}

trap cleanup EXIT

docker run --rm --name e2e-tsdb -p 5432:5432/tcp -e "POSTGRES_PASSWORD=postgres" "${TIMESCALE_IMAGE}"  > /dev/null 2>&1 &
docker run --rm --name e2e-prom --network="host" -p 9090:9090/tcp -v "$CONF:/etc/prometheus/prometheus.yml" prom/prometheus:latest > /dev/null 2>&1  &

cd $ROOT_DIR/cmd/promscale
go get ./...
go build .

wait_for() {
    echo "waiting for $1"

    ${SCRIPT_DIR}/wait-for.sh "$1" -t 60 -- echo "$1 may be ready"

    sleep 5

    ${SCRIPT_DIR}/wait-for.sh "$1" -t 60 -- echo "$1 ready"
}

echo "Waiting for database to be up..."
wait_for "$DB_URL"

TS_PROM_LOG_LEVEL=debug \
TS_PROM_DB_CONNECT_RETRIES=10 \
TS_PROM_DB_PASSWORD=postgres \
TS_PROM_DB_NAME=postgres \
TS_PROM_DB_SSL_MODE=disable \
TS_PROM_WEB_TELEMETRY_PATH=/metrics \
./promscale &

CONN_PID=$!

echo "Waiting for connector to be up..."
wait_for "$CONNECTOR_URL"


START_TIME=$(date +"%s")

echo "sending write request"

curl -v \
    -H "Content-Type: application/x-protobuf" \
    -H "Content-Encoding: snappy" \
    -H "X-Prometheus-Remote-Write-Version: 0.1.0" \
    --data-binary "@${ROOT_DIR}/pkg/pgmodel/testdata/real-dataset.sz" \
    "${CONNECTOR_URL}/write"

echo "sending import request"

curl -v \
    -H "Content-Type: application/json" \
    --data-binary "@${ROOT_DIR}/pkg/pgmodel/testdata/import.json" \
    "${CONNECTOR_URL}/write"

compare_connector_and_prom() {
    QUERY=${1}
    CONNECTOR_OUTPUT=$(curl -s "http://${CONNECTOR_URL}/api/v1/${QUERY}")
    PROM_OUTPUT=$(curl -s "http://${PROM_URL}/api/v1/${QUERY}")
    echo "ran: ${QUERY}"
    echo " connector response: ${CONNECTOR_OUTPUT}"
    echo "prometheus response: ${PROM_OUTPUT}"
    if [ "${CONNECTOR_OUTPUT}" != "${PROM_OUTPUT}" ]; then
        echo "mismatched output"
        ((FAILED+=1))
    else
        ((PASSED+=1))
    fi
}
END_TIME=$(date +"%s")

DATASET_START_TIME="2020-08-10T10:35:20Z"
DATASET_END_TIME="2020-08-10T11:43:50Z"


# Check that backfilled dataset is present in both sources.
compare_connector_and_prom "query_range?query=demo_disk_usage_bytes%7Binstance%3D%22demo.promlabs.com%3A10002%22%7D&start=$DATASET_START_TIME&end=$DATASET_END_TIME&step=30s"
compare_connector_and_prom "query?query=demo_cpu_usage_seconds_total%7Binstance%3D%22demo.promlabs.com%3A10000%22%2Cmode%3D%22user%22%7D&time=$DATASET_START_TIME"
# Check that connector metrics are scraped.  compare_connector_and_prom "query?query=ts_prom_received_samples_total&time=$START_TIME" # Check that connector is up.
compare_connector_and_prom "query?query=up&time=$START_TIME"
# Check series endpoint matches on connector series.
compare_connector_and_prom "series?match%5B%5D=ts_prom_sent_samples_total"

# Labels endpoint cannot be compared to Prometheus becuase it will always differ due to direct backfilling of the real dataset.
# We have to compare it to the correct expected output. Note that `namespace` and `node` labels are from JSON import payload.
EXPECTED_OUTPUT='{"status":"success","data":["__name__","code","handler","instance","job","le","method","mode","namespace","node","path","quantile","status","version"]}'
LABELS_OUTPUT=$(curl -s "http://${CONNECTOR_URL}/api/v1/labels")
echo "  labels response: ${LABELS_OUTPUT}"
echo "expected response: ${EXPECTED_OUTPUT}"

if [ "${LABELS_OUTPUT}" != "${EXPECTED_OUTPUT}" ]; then
    echo "TEST FAILED: mismatched output"
    ((FAILED+=1))
else
    ((PASSED+=1))
fi

echo "Passed: $PASSED"
echo "Failed: $FAILED"


if [[ $FAILED -eq 0 ]]; then
    exit 0
else
    exit 1
fi

