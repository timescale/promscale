#!/bin/sh

# Abort if any command returns an error value
set -e

USER=promscale

if getent passwd "${USER}" > /dev/null 2>&1 ; then 
  userdel "${USER}" 2>/dev/null
fi
if getent group "${USER}" > /dev/null 2>&1 ; then
  groupdel "${USER}" 2>/dev/null
fi
