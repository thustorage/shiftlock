#!/bin/bash
# Experiment 5: Application.
# - Figure 11: Performance of the banking application.
#
# Estimated run time: ~10min

# REDIS_DIR="/home/gaoj/redis"
REDIS_DIR=""

if [ -z "$REDIS_DIR" ]; then
    echo "Please set the path to the Redis directory."
    echo "It must contain a redis-server executable and a redis.conf file."
    echo "An example redis.conf is in 'etc' subdirectory."
    exit 1
fi

SCRIPT_DIR=$(dirname $(readlink -f "$0"))
source $SCRIPT_DIR/../utils/run_once.fn.sh

mkdir -p $SCRIPT_DIR/../../data/exp5
rm -rf $SCRIPT_DIR/../../data/exp5/*
cargo build --release --workspace --quiet

LOCKS=(ShiftLock RedLock)
WORKLOADS=(tatp tpcc-h)

TIME=5

# Run experiments.
for lock in ${LOCKS[@]}; do
    # Launch Redis.
    rm -rf $REDIS_DIR/dump.rdb
    rm -rf ./dump.rdb
    
    tmux new-session -t redis -d
    tmux send-keys -t redis "cd $REDIS_DIR" C-m
    sleep 1
    tmux send-keys -t redis "$REDIS_DIR/redis-server $REDIS_DIR/redis.conf" C-m
    sleep 2

    echo "Running $lock..."
    run_once "$SCRIPT_DIR/../run-redis.sh $lock"

    # Modify the output directory name
    output_dir=$(ls $SCRIPT_DIR/../../data | grep $lock)
    mv $SCRIPT_DIR/../../data/$output_dir $SCRIPT_DIR/../../data/exp5/$lock

    # Stop the server.
    tmux send-keys -t redis C-c
    sleep 1
    tmux kill-session -t redis >> /dev/null 2>&1
    killall -9 redis-server >> /dev/null 2>&1

    sleep 1
done
