#!/bin/bash

if [[ $# -lt 3 ]]; then
    echo "Run distributed clients"
    echo "Usage: run-nthreads.sh <SERVER_URI> <LOCK> <N> [<BACKOFF>]"
    exit 1
fi

set -e

SERVER_URI=$1
LOCK=$2
N=$3
BACKOFF=${4:-"1862"}
WORKLOAD="micro:zipf,wi:10"

echo "Server:   $SERVER_URI"
echo "Lock:     $LOCK"
echo "N:        $N"
echo "Workload: $WORKLOAD"

SCRIPT_DIR=$(dirname $(readlink -f "$0"))
HANDLOCK_NODES=$($SCRIPT_DIR/utils/set-nodes.sh)

$SCRIPT_DIR/../target/release/zero --server $1

pdsh -w "ssh:$HANDLOCK_NODES" "rm -f /home/gaoj/results/output.txt"
pdsh -w "ssh:$HANDLOCK_NODES" "/home/gaoj/handlock/target/release/client --backoff $BACKOFF --server $SERVER_URI --nthreads $N --lock $LOCK --workload $WORKLOAD --output /home/gaoj/results/output.txt"

IFS=','
read -ra ADDRS <<< "$HANDLOCK_NODES"

DIRNAME="$(date +%Y%m%d-%H%M%S)-$LOCK"
mkdir -p /home/gaoj/handlock/data/$DIRNAME

for addr in "${ADDRS[@]}"; do
    scp gaoj@$addr:/home/gaoj/results/output.txt /home/gaoj/handlock/data/$DIRNAME/$addr.txt
done

