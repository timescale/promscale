#!/bin/bash

if [ -n "$DEBUG" ]; then
	set -x
fi

DIR=$(cd $(dirname "${BASH_SOURCE}") && pwd -P)
HELMDIR="${DIR}/../helm-chart"

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

# Default to promscale helm release name i.e. adds the label values as promscale
RELEASE_NAME=promscale
# Default to to default namespace
NAMESPACE=default

helm dependency update ${HELMDIR}

OUTPUT_FILE="${DIR}/../deploy/static/deploy.yaml"
cat << EOF | helm template $RELEASE_NAME ${HELMDIR} --namespace $NAMESPACE --values $FILE_ARG > ${OUTPUT_FILE}
EOF

echo "$(cat ${OUTPUT_FILE})" > ${OUTPUT_FILE}
