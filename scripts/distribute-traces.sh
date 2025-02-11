#!/bin/bash

SCRIPT_DIR=$(dirname $(readlink -f "$0"))
SHIFTLOCK_NODES=$($SCRIPT_DIR/utils/set-nodes.sh)

IFS=','
read -ra ADDRS <<< "$SHIFTLOCK_NODES"

TRACES=("tatp" "tpcc" "tpcc-h")

for i in "${!ADDRS[@]}"; do
    addr=${ADDRS[$i]}
    j=$((i+1))

    echo $addr

    pdsh -w "ssh:$addr" "mkdir -p $SCRIPT_DIR/../trace-dist"
    for trace in "${TRACES[@]}"; do
        scp $SCRIPT_DIR/../traces/h$j-$trace.csv gaoj@$addr:$SCRIPT_DIR/../trace-dist/$trace.csv
    done
    echo
done
