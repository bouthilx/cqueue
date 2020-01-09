#!/bin/bash

SCRIPT_PATH=$(dirname "$0")

mkdir -p $SCRIPT_PATH/cqueue/backends/bin

wget -qO- https://binaries.cockroachdb.com/cockroach-v19.2.2.linux-amd64.tgz | tar  xvz

cp -i cockroach-v19.2.2.linux-amd64/cockroach $SCRIPT_PATH/cqueue/backends/bin

rm -rf cockroach-v19.2.2.linux-amd64/cockroach
