#!/bin/bash

set -u

# jump one level up (to project directory)
cd "$(dirname $0)/.."

echor() { echo "$@" 1>&2; }

UIO_BIN=`which uio`

if [ $? -eq 0 ]; then
    echor "Upgrading binary at \`$UIO_BIN\`."
    set -xe

    lein do clean, midje, install, bin
    cp ./target/uio "$UIO_BIN"

else
    echor "Could find \`uio\` binary in PATH"
    exit 1
fi

