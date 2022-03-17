#!/usr/bin/env bash

# This test script exercises Promscale's HA leader election and failover code
# paths. In principle the setup is the following:
# - Two prometheus instances with `__replica__` labels of `prometheus1` and
#   `prometheus2` respectively, are _both_ connected to two Promscale connector
#   instances.
# - The Promscale connector instances should both choose the same "leader"
#   prometheus instance. Only metrics from this instance are collected. The
#   current leader is exposed through the `promscale_ha_cluster_leader_info`
#   metric.
# - By starting and stopping the `prometheus1` and `prometheus2` instances, we
#   can trigger a failover from one leader to the other.

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

leader_name() {
    LEADER=`docker exec $1 wget -O - localhost:9201/metrics-text 2>&1 | grep -i "promscale_ha_cluster_leader_info.*1$" | sed 's/^.*replica=\"\(.*\)\".*$/\1/'`
    echo "$LEADER"
}

is_leader() {
    LEADER1=$(leader_name "promscale_deploy_ha-promscale-connector1-1")
    LEADER2=$(leader_name "promscale_deploy_ha-promscale-connector2-1")
    if [[ "$LEADER1" == $1 && "$LEADER2" == $1 ]]; then
        true
    else
        false
    fi
}

is_not_leader() {
    if is_leader $1; then
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

wait_for_leader() {
    echo "waiting for $1 to be leader"

    for i in `seq 10` ; do
        if is_leader "$1"; then
          echo "$1 is leader"
          return
        fi
        sleep 10
    done
    echo "FAIL waiting for $1 to be leader"
    exit 1
}

wait_for "promscale_deploy_ha-promscale-connector1-1"
wait_for "promscale_deploy_ha-promscale-connector2-1"

#make sure initial conditions are what we expect
if is_not_leader "prometheus1"; then
    docker stop "promscale_deploy_ha-prometheus2-1"
    wait_for_leader "prometheus1"
    docker start "promscale_deploy_ha-prometheus2-1"
fi

echo "1: check initial condition"
is_leader "prometheus1"
is_not_leader "prometheus2"

echo "2: kill prometheus1, prometheus2 becomes leader"
docker stop promscale_deploy_ha-prometheus1-1
wait_for_leader "prometheus2"

echo "3: bring prometheus 1 back and kill the current leader, prometheus 1 becomes leader again"
docker start promscale_deploy_ha-prometheus1-1
docker stop "promscale_deploy_ha-prometheus2-1"
wait_for_leader "prometheus1"

echo "SUCCESS"

echo "Stopping containers"