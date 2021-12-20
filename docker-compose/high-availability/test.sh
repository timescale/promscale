#!/usr/bin/env bash

set -euf -o pipefail

SELF_DIR=$(cd $(dirname ${0}) && pwd)
cd $SELF_DIR

echo "running tests"

#build latest image
docker build -t timescale/promscale:latest ../.. --file ../../build/Dockerfile

docker compose -p promscale_deploy_ha up -d

cleanup() {
    if (( $? != 0 )); then
        echo "ERROR"
    fi
    docker compose -p promscale_deploy_ha down > /dev/null 2>&1
}

trap cleanup EXIT

ha_line() {
    LAST=`docker exec $1 wget -O - localhost:9201/metrics-text 2>&1 | grep -i "promscale_ha_cluster_leader_info.*1$" | tail -n 1`
    echo "$LAST"
}

is_ingesting() {
    LAST=$(ha_line $1)
    if [[ "$LAST" == *"$1"* ]]; then
        true
    else
        false
    fi
}

is_not_ingesting() {
    if is_ingesting $1; then
        false
    else
        true
    fi
}

wait_for() {
    echo "waiting for $1"

    for i in `seq 10` ; do
        if [ ! -z "$(docker exec $1 wget -O - localhost:9201/metrics-text)" ] ; then
          echo "connector up"
          return 0
        fi
        sleep 10
    done
    echo "FAIL waiting for $1"
    exit 1
}

wait_for_ingestion() {
    echo "waiting for $1 to be in status $2"

    for i in `seq 10` ; do
        if $2 "$1"; then
          echo "connector $1 $2"
          return
        fi
        sleep 10
    done
    echo "FAIL waiting for $1 $2"
    exit 1
}

wait_for "promscale_deploy_ha-promscale-connector1-1"
wait_for "promscale_deploy_ha-promscale-connector2-1"

#make sure inital conditions are what we expect
if is_not_ingesting "promscale_deploy_ha-promscale-connector1-1"; then
    docker stop "promscale_deploy_ha-promscale-connector2-1"
    wait_for_ingestion "promscale_deploy_ha-promscale-connector1-1" "is_ingesting"
    docker start "promscale_deploy_ha-promscale-connector2-1"
    wait_for_ingestion "promscale_deploy_ha-promscale-connector2-1"  "is_not_ingesting"
fi

echo "check initial condition"
is_ingesting "promscale_deploy_ha-promscale-connector1-1" 
is_not_ingesting "promscale_deploy_ha-promscale-connector2-1" 

echo "kill connector1's prometheus"
docker stop promscale_deploy_ha-prometheus1-1
wait_for_ingestion "promscale_deploy_ha-promscale-connector2-1" "is_ingesting"
wait_for_ingestion "promscale_deploy_ha-promscale-connector1-1" "is_not_ingesting"

echo "bring prometheus 1 back and kill the current leader, connector 1 becomes leader again"
docker start promscale_deploy_ha-prometheus1-1
docker stop "promscale_deploy_ha-promscale-connector2-1"
wait_for_ingestion "promscale_deploy_ha-promscale-connector1-1" "is_ingesting"

echo "bring connector 2 up, it becomes follower"
docker start "promscale_deploy_ha-promscale-connector2-1"
wait_for "promscale_deploy_ha-promscale-connector2-1"
wait_for_ingestion "promscale_deploy_ha-promscale-connector2-1" "is_not_ingesting"

echo "SUCCESS"
