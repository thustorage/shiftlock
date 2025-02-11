#!/bin/bash

if [[ $# -lt 2 ]]; then
    echo "Run basic distributed clients"
    echo "Usage: run-basic.sh <SERVER_URI> <LOCK> [<WORKLOAD>]"
    exit 1
fi

set -e

SERVER_URI=$1
LOCK=$2
WORKLOAD=${3:-"micro:zipf,wi:5"}

echo "Server:   $SERVER_URI"
echo "Lock:     $LOCK"
echo "Workload: $WORKLOAD"

SCRIPT_DIR=$(dirname $(readlink -f "$0"))
SHIFTLOCK_NODES=$($SCRIPT_DIR/utils/set-nodes.sh)

$SCRIPT_DIR/../target/release/zero --server $1

pdsh -w "ssh:$SHIFTLOCK_NODES" "rm -f $SCRIPT_DIR/../results/output.txt"
pdsh -w "ssh:$SHIFTLOCK_NODES" "$SCRIPT_DIR/../target/release/client --server $SERVER_URI --nthreads 48 --lock $LOCK --workload $WORKLOAD --output $SCRIPT_DIR/../results/output.txt"

IFS=','
read -ra ADDRS <<< "$SHIFTLOCK_NODES"

DIRNAME="$(date +%Y%m%d-%H%M%S)-$LOCK"
mkdir -p $SCRIPT_DIR/../data/$DIRNAME

for addr in "${ADDRS[@]}"; do
    scp gaoj@$addr:$SCRIPT_DIR/../results/output.txt $SCRIPT_DIR/../data/$DIRNAME/$addr.txt
done
