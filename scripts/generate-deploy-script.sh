#!/bin/bash

if [ -n "$DEBUG" ]; then
	set -x
fi

# Execute whole script from repository top level
cd "$(git rev-parse --show-toplevel)"

OUTPUT_FILE="deploy/static/deploy.yaml"

# Default to promscale helm release name i.e. adds the label values as promscale
RELEASE_NAME=promscale
# Default to to default namespace
NAMESPACE=default

HELMDIR="deploy/helm-chart"
# Default values.yaml file to provide to helm template command.
FILE_ARG="${HELMDIR}/values.yaml"

# Check if values file filepath was supplied.
if [ "$#" -eq  "1" ]; then
	if [ -f "$1" ]; then
		FILE_ARG=$1
	fi
fi

set -o errexit
set -o nounset
set -o pipefail

helm dependency update "${HELMDIR}"

helm template "$RELEASE_NAME" "${HELMDIR}" --namespace "$NAMESPACE" --values "$FILE_ARG" | tee "${OUTPUT_FILE}"