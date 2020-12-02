#!/usr/bin/env bash

set -euf -o pipefail

SELF_DIR=$(cd $(dirname ${0}) && pwd)
cd $SELF_DIR

echo "running tests"

#build latest image
docker build -t timescale/promscale:latest ../.. --file ../../build/Dockerfile

docker-compose -p promscale_deploy_ha up -d

cleanup() {
    if (( $? != 0 )); then
        echo "ERROR"
    fi
    docker-compose -p promscale_deploy_ha down > /dev/null 2>&1
}

trap cleanup EXIT

last_line() {
    LAST=`docker logs $1  2>&1| grep component=leader_election || true | tail -1`
    echo $LAST
}

is_role() {
    LAST=$(last_line $1)
    if [[ "$LAST" == *"status=$2"* ]]; then
        true
    else
        false
    fi
}

wait_for() {
    echo "waiting for $1"

    for i in `seq 10` ; do
        if [ ! -z "$(last_line $1)" ] ; then
          echo "connector up"
          return 0
        fi
        sleep 5
    done
    echo "FAIL waiting for $1"
    exit 1
}

wait_for_role() {
    echo "waiting for $1 in role $2"

    for i in `seq 10` ; do
        if is_role "$1" "$2" ; then
          echo "connector $1 is at role $2"
          return
        fi
        sleep 5
    done
    echo "FAIL waiting for $1 in role $2"
    exit 1
}

wait_for "promscale_deploy_ha_promscale-connector1_1"

#make sure inital conditions are what we expect
if is_role "promscale_deploy_ha_promscale-connector1_1" "follower"; then
    docker stop "promscale_deploy_ha_promscale-connector2_1"
    wait_for_role "promscale_deploy_ha_promscale-connector1_1" "leader"
    docker start "promscale_deploy_ha_promscale-connector2_1"
    wait_for_role "promscale_deploy_ha_promscale-connector2_1" "follower"
fi

echo "check initial condition"
is_role "promscale_deploy_ha_promscale-connector1_1" "leader"
is_role "promscale_deploy_ha_promscale-connector2_1" "follower"

echo "kill connector1's prometheus"
docker stop promscale_deploy_ha_prometheus1_1
wait_for_role "promscale_deploy_ha_promscale-connector1_1" "follower"
wait_for_role "promscale_deploy_ha_promscale-connector2_1" "leader"

echo "bring prometheus 1 back and kill the current leader, connector 2 becomes leader again"
docker start promscale_deploy_ha_prometheus1_1
docker stop "promscale_deploy_ha_promscale-connector2_1"
wait_for_role "promscale_deploy_ha_promscale-connector1_1" "leader"

echo "bring connector 2 up, it becomes follower"
docker start "promscale_deploy_ha_promscale-connector2_1"
wait_for_role "promscale_deploy_ha_promscale-connector2_1" "follower"

echo "SUCCESS"