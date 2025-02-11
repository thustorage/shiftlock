#!/bin/bash

SCRIPT_DIR=$(dirname $(readlink -f "$0"))
SHIFTLOCK_NODES=$($SCRIPT_DIR/set-nodes.sh)

pdsh -w "ssh:10.0.2.110,$SHIFTLOCK_NODES" "killall -9 client client-fallible client-redis >> /dev/null 2>&1"  >> /dev/null 2>&1
tmux kill-session -t shiftlock-server >> /dev/null 2>&1
tmux kill-session -t redis >> /dev/null 2>&1
killall -9 server >> /dev/null 2>&1
