#!/bin/sh

# Abort if any command returns an error value
set -e

USER=promscale

# Following user part should be tested on both RPM and DEB systems
if ! getent group "${USER}" > /dev/null 2>&1 ; then
  groupadd --system "${USER}"
fi
GID=$(getent group "${USER}" | cut -d: -f 3)
if ! id "${USER}" > /dev/null 2>&1 ; then
  adduser --system --no-create-home \
    --gid "${GID}" --shell /bin/false \
    "${USER}"
fi
