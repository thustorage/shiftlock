#!/bin/bash

SCRIPT_DIR=$(dirname $(readlink -f "$0"))
HANDLOCK_NODES=$($SCRIPT_DIR/utils/set-nodes.sh)

IFS=','
read -ra ADDRS <<< "$HANDLOCK_NODES"

TRACES=("tatp" "tpcc" "tpcc-h")

for i in "${!ADDRS[@]}"; do
    addr=${ADDRS[$i]}
    j=$((i+1))

    echo $addr

    pdsh -w "ssh:$addr" "mkdir -p /home/gaoj/handlock-trace"
    for trace in "${TRACES[@]}"; do
        scp /home/gaoj/handlock/traces/h$j-$trace.csv gaoj@$addr:/home/gaoj/handlock-trace/$trace.csv
    done
    echo
done
