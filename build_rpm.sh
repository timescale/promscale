#!/bin/bash
set -e

# if started multiple times it will fail
# need to exit gracefully
if [ -f ".buildrpmstarted" ]; then
    echo "Already running, exiting"
    exit 0
fi
touch .buildrpmstarted

if [[ -z "${1}" ]]; then
  echo "Usage 'build_rpm.sh <VERSION>' you must specify the version to build"
  exit 1
else
  VERSION="${1}"
fi

if [ ! -d ./dist/rpm ];
then
	mkdir -p dist/rpm
fi

echo "Building docker image that will build the Promscale binary"
docker build --tag=promscale_builder --file=Dockerfile .

echo "Building docker image that will build the rpm"
docker build --build-arg VERSION=$VERSION --tag=promscale_rpm_builder --file=BuildRPMDockerfile .

echo "Starting container"
docker run --name rpm_builder -d promscale_rpm_builder

echo "Copying rpm to ./dist"
docker cp rpm_builder:/root/rpmbuild/RPMS/x86_64/promscale-$VERSION-1.el8.x86_64.rpm  ./dist/rpm

echo "Killing container rpm_builder"
docker kill rpm_builder

echo "Removing container rpm_builder"
docker rm rpm_builder

rm -f .buildrpmstarted