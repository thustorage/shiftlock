#!/bin/bash

if [[ $# -lt 2 ]]; then
    echo "Run fallible clients"
    echo "Usage: run-basic.sh <SERVER_URI> <FAILPROB> <LOCK> [<WORKLOAD>]"
    exit 1
fi

set -e

SERVER_URI=$1
FAILPROB=$2
LOCK=$3
WORKLOAD=${4:-"micro:zipf,wi:5"}

echo "Server:   $SERVER_URI"
echo "Failure:  $FAILPROB"
echo "Lock:     $LOCK"
echo "Workload: $WORKLOAD"

SCRIPT_DIR=$(dirname $(readlink -f "$0"))
HANDLOCK_NODES=$($SCRIPT_DIR/utils/set-nodes.sh)

$SCRIPT_DIR/../target/release/zero --server $1

pdsh -w "ssh:$HANDLOCK_NODES" "rm -f /home/gaoj/results/output.txt"
pdsh -w "ssh:$HANDLOCK_NODES" "/home/gaoj/handlock/target/release/client-fallible --server $SERVER_URI --nthreads 48 --lock $LOCK --workload $WORKLOAD --failprob $FAILPROB"