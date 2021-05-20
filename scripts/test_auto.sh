#!/bin/bash

set -xue

# jump one level up (to project directory)
cd "$(dirname $0)/.."

lein midje :autotest
