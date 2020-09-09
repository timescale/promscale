#!/usr/bin/env bash

set -euf -o pipefail

SCRIPT_DIR=$(dirname ${0})
ROOT_DIR=$(dirname ${SCRIPT_DIR})
CONNECTOR_URL="localhost:9201"
PROM_URL="localhost:9090"

docker-compose up &

trap "docker-compose down" EXIT

# the race condition between the connector and the test runner is real
# to ensure that the connector is actually started, we wait twice hpoing
# that will be long enough that the connector is really started
wait_for_connector() {
    echo "waiting for connector"

    sleep 10

    ${SCRIPT_DIR}/wait-for.sh ${CONNECTOR_URL} -t 60 -- echo "connector may be ready..."

    sleep 10

    ${SCRIPT_DIR}/wait-for.sh ${CONNECTOR_URL} -t 60 -- echo "connector ready"
}

wait_for_connector

echo "sending write request"

curl -v --request POST \
    -H "Content-Type: application/x-protobuf" \
    -H "Content-Encoding: snappy" \
    -H "X-Prometheus-Remote-Write-Version: 0.1.0" \
    --data-binary "@${SCRIPT_DIR}/real-dataset.sz" \
    "${CONNECTOR_URL}/write"

EXIT_CODE=0

compare_connector_and_prom() {
    QUERY=${1}
    CONNECTOR_OUTPUT=$(curl "http://${CONNECTOR_URL}/api/v1/query?query=${QUERY}")
    PROM_OUTPUT=$(curl "http://${PROM_URL}/api/v1/query?query=${QUERY}")
    echo "ran: ${QUERY}"
    echo " connector response: ${CONNECTOR_OUTPUT}"
    echo "prometheus response: ${PROM_OUTPUT}"
    if [ "${CONNECTOR_OUTPUT}" != "${PROM_OUTPUT}" ]; then
        echo "mismatched output"
        EXIT_CODE=1
    fi
}

# TODO we need some real tests here, ones that don't return empty data
compare_connector_and_prom "sum%20by%28mode%29%28demo_cpu_usage_seconds_total%29&time=2020-07-31T17:30:47.182Z"

exit ${EXIT_CODE}
