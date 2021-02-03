#!/usr/bin/env bash

set -euf -o pipefail

SELF_DIR=$(cd $(dirname ${0}) && pwd)
cd $SELF_DIR

echo "running tests"

#build latest image
docker build -t timescale/promscale:latest ../ --file ../build/Dockerfile

docker-compose -p test_docker-compose up -d

cleanup() {
    if (( $? != 0 )); then
        echo "ERROR"
    fi
    docker-compose -p test_docker-compose down > /dev/null 2>&1
}

trap cleanup EXIT

## list all container names
declare -a arr=("db" "promscale" "prometheus" "node_exporter")

## now loop through the above array
## to check containers are running
for i in "${arr[@]}"
do
   containerName=`docker ps -q -f name="$i"`
   if [ -n "$containerName" ]; then
    echo "$i container is running"
else
    echo "$i container failed to run"
    exit 1
fi
done

## As prometheus scrape interval is 10s
## sleep for 30s so we can find ingestion logs in promscale
sleep 30

writeLog=$(docker logs test_docker-compose_promscale_1  2>&1| grep samples/sec | tail -n 1 || true)
   if [ -n "$writeLog" ]; then
    echo "promscale is ingesting data"
else
    echo "failed to find promscale ingesting data logs"
    exit 1
fi

echo "SUCCESS"