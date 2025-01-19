#!/bin/bash
# Experiment 6: Latency breakdown.
# - Table 3: Latency breakdown of ShiftLock (in microseconds, on average) with different client numbers.
# - Table 4: Latency breakdown of different lock schemes (in microseconds, on average) with 240 clients.
#
# Estimated run time: ~10min

SCRIPT_DIR=$(dirname $(readlink -f "$0"))
source $SCRIPT_DIR/../utils/run_once.fn.sh

mkdir -p $SCRIPT_DIR/../../data/exp6
rm -rf $SCRIPT_DIR/../../data/exp6/*
mkdir -p $SCRIPT_DIR/../../data/exp6/table3
mkdir -p $SCRIPT_DIR/../../data/exp6/table4

HANDLOCK_CONSWRT_THRESHOLD=2147483647 cargo build --release --workspace --quiet --features timed,custcwt

TIME=10

# Table 3
CLIENTS=(1 4 8 16 24 32 40 48)
for c in ${CLIENTS[@]}; do
    run_once "$SCRIPT_DIR/../run-nthreads.sh 10.0.2.110:31850 Handlock $c"
    output_dir=$(ls $SCRIPT_DIR/../../data | grep Handlock)
    mv $SCRIPT_DIR/../../data/$output_dir $SCRIPT_DIR/../../data/exp6/table3/$c
done

# Table 4
LOCKS=(Handlock Cas Dslr)
for lock in ${LOCKS[@]}; do
    run_once "$SCRIPT_DIR/../run-nthreads.sh 10.0.2.110:31850 $lock 48"
    output_dir=$(ls $SCRIPT_DIR/../../data | grep $lock)
    mv $SCRIPT_DIR/../../data/$output_dir $SCRIPT_DIR/../../data/exp6/table4/$lock
done

run_once "$SCRIPT_DIR/../run-nthreads.sh 10.0.2.110:31850 Cas 48 0"
output_dir=$(ls $SCRIPT_DIR/../../data | grep Cas)
mv $SCRIPT_DIR/../../data/$output_dir $SCRIPT_DIR/../../data/exp6/table4/CasNB

# Restore a clean build
cargo build --release --workspace --quiet
