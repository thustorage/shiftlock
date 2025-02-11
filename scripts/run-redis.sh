#!/bin/bash

if [[ $# -lt 1 ]]; then
    echo "Run smallbank clients"
    echo "Usage: run-redis.sh <LOCK>"
    exit 1
fi

set -e

SERVER_URI=10.0.2.110:31850
REDIS_URI=192.168.2.110:6379
LOCK=$1

echo "Redis:    $REDIS_URI"
echo "Server:   $SERVER_URI"
echo "Lock:     $LOCK"

SCRIPT_DIR=$(dirname $(readlink -f "$0"))
SHIFTLOCK_NODES=$($SCRIPT_DIR/utils/set-nodes.sh)

if [ $LOCK == "ShiftLock" ]; then
    $SCRIPT_DIR/../target/release/zero --server $SERVER_URI
fi

$SCRIPT_DIR/../target/release/client-redis --redis $REDIS_URI --prepare

pdsh -w "ssh:$SHIFTLOCK_NODES" "rm -f $SCRIPT_DIR/../results/output.txt"
pdsh -w "ssh:$SHIFTLOCK_NODES" "$SCRIPT_DIR/../target/release/client-redis --redis $REDIS_URI --server $SERVER_URI --duration 10 --nthreads 48 --lock $LOCK --output $SCRIPT_DIR/../results/output.txt"

IFS=','
read -ra ADDRS <<< "$SHIFTLOCK_NODES"

DIRNAME="$(date +%Y%m%d-%H%M%S)-$LOCK"
mkdir -p $SCRIPT_DIR/../data/$DIRNAME

for addr in "${ADDRS[@]}"; do
    scp gaoj@$addr:$SCRIPT_DIR/../results/output.txt $SCRIPT_DIR/../data/$DIRNAME/$addr.txt
done
