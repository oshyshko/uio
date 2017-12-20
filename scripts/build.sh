#!/bin/bash

set -xue

lein do clean, midje, install, bin
