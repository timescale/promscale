#!/usr/bin/env bash

# This script takes a number of docker images and outputs the
# first one which exists, or "no image present" if none exists.

for img in "$@"; do
  if docker image inspect "${img}" 1>/dev/null 2>&1 || docker manifest inspect "${img}" 1>/dev/null 2>&1; then
		echo "${img}"
		exit 0
	fi
done
echo "no image present"
exit 1
