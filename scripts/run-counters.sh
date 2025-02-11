#!/bin/bash

if [[ $# -lt 2 ]]; then
    echo "Run basic distributed clients and show counters"
    echo "Usage: run-counters.sh <SERVER_URI> <LOCK> [<BACKOFF>]"
    echo "   Backoff default to 1862"
    exit 1
fi

set -e

SERVER_URI=$1
LOCK=$2
BACKOFF=${3:-"1862"}
WORKLOAD="micro:zipf,wi:10"

echo "Server:   $SERVER_URI"
echo "Lock:     $LOCK"
echo "Workload: $WORKLOAD"

SCRIPT_DIR=$(dirname $(readlink -f "$0"))
SHIFTLOCK_NODES=$($SCRIPT_DIR/utils/set-nodes.sh)

$SCRIPT_DIR/../target/release/zero --server $1
START_A=$(cat /sys/class/infiniband/mlx5_0/ports/1/hw_counters/rx_atomic_requests)
START_R=$(cat /sys/class/infiniband/mlx5_0/ports/1/hw_counters/rx_read_requests)
echo "start = $START_A $START_R"

pdsh -w "ssh:$SHIFTLOCK_NODES" "rm -f $SCRIPT_DIR/../results/output.txt"
pdsh -w "ssh:$SHIFTLOCK_NODES" "$SCRIPT_DIR/../target/release/client --backoff $BACKOFF --server $SERVER_URI --nthreads 48 --count 10000000 --lock $LOCK --workload $WORKLOAD --output $SCRIPT_DIR/../results/output.txt"

IFS=','
read -ra ADDRS <<< "$SHIFTLOCK_NODES"

DIRNAME="$(date +%Y%m%d-%H%M%S)-$LOCK"
mkdir -p $SCRIPT_DIR/../data/$DIRNAME

for addr in "${ADDRS[@]}"; do
    scp gaoj@$addr:$SCRIPT_DIR/../results/output.txt $SCRIPT_DIR/../data/$DIRNAME/$addr.txt
done

END_A=$(cat /sys/class/infiniband/mlx5_0/ports/1/hw_counters/rx_atomic_requests)
END_R=$(cat /sys/class/infiniband/mlx5_0/ports/1/hw_counters/rx_read_requests)
echo "end = $END_A $END_R"
echo
echo "diff = $(($END_A-$START_A)) $(($END_R-$START_R))"

echo "$(($END_A-$START_A)) $(($END_R-$START_R))" >> $SCRIPT_DIR/../data/$DIRNAME/counters.txt
