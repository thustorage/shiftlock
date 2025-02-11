#!/bin/bash
# Experiment 7: Parameter selection.
# - Figure 12: Performance of ShiftLock with different maximum consecutive writer counts.
#
# Estimated run time: ~5min

SCRIPT_DIR=$(dirname $(readlink -f "$0"))
source $SCRIPT_DIR/../utils/run_once.fn.sh

mkdir -p $SCRIPT_DIR/../../data/exp7
rm -rf $SCRIPT_DIR/../../data/exp7/*

THRESHOLDS=(1 2 4 8 12 16 20 24 28 32 2147483647)
TIME=10

# Run experiments.
for thres in ${THRESHOLDS[@]}; do
    SHIFTLOCK_CONSWRT_THRESHOLD=$thres cargo build --release --workspace --quiet --features custcwt
    workload="trace:$SCRIPT_DIR/../../trace-dist/$wl.csv:$TIME"
    echo "Running conswrt_thres = $thres..."

    run_once "$SCRIPT_DIR/../run-basic.sh 10.0.2.110:31850 ShiftLock micro:zipf,wi:$TIME"

    # Modify the output directory name
    output_dir=$(ls $SCRIPT_DIR/../../data | grep ShiftLock)
    mv $SCRIPT_DIR/../../data/$output_dir $SCRIPT_DIR/../../data/exp7/$thres
done

# Restore a clean build
cargo build --release --workspace --quiet
