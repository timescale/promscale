#!/usr/bin/env bash

set -euf -o pipefail

SELF_DIR=$(cd $(dirname ${0}) && pwd)
cd $SELF_DIR

echo "running tests"

#build latest image
docker build -t timescale/promscale:latest ../ --file ../build/Dockerfile

docker compose -p test up -d

cleanup() {
    if (( $? != 0 )); then
        echo "ERROR"
    fi
    docker compose -p test down > /dev/null 2>&1
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

i=0
while [ "$(docker logs -n10 test-promscale-1 2>&1| grep samples/sec | wc -l)" -lt 1 ]; do
	i=$((i+=1))
	if [ "$i" -gt 600 ]; then
		echo "ERROR: Promscale couldn't ingest data for over 600s. Exiting."
		exit 1
	fi
	echo "Waiting for promscale to ingest data"
	sleep 1
done

echo "SUCCESS"
