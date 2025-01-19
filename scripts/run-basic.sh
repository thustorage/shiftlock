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
HANDLOCK_NODES=$($SCRIPT_DIR/utils/set-nodes.sh)

$SCRIPT_DIR/../target/release/zero --server $1

pdsh -w "ssh:$HANDLOCK_NODES" "rm -f /home/gaoj/results/output.txt"
pdsh -w "ssh:$HANDLOCK_NODES" "/home/gaoj/handlock/target/release/client --server $SERVER_URI --nthreads 48 --lock $LOCK --workload $WORKLOAD --output /home/gaoj/results/output.txt"

IFS=','
read -ra ADDRS <<< "$HANDLOCK_NODES"

DIRNAME="$(date +%Y%m%d-%H%M%S)-$LOCK"
mkdir -p /home/gaoj/handlock/data/$DIRNAME

for addr in "${ADDRS[@]}"; do
    scp gaoj@$addr:/home/gaoj/results/output.txt /home/gaoj/handlock/data/$DIRNAME/$addr.txt
done
