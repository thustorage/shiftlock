#!/bin/bash
# Experiment 8: Failure recovery.
# - Figure 13: Performance of ShiftLock and DSLR under different client failure rates.
#
# Estimated run time: ~3min

SCRIPT_DIR=$(dirname $(readlink -f "$0"))
source $SCRIPT_DIR/../utils/run_once.fn.sh

mkdir -p $SCRIPT_DIR/../../data/exp8
rm -rf $SCRIPT_DIR/../../data/exp8/*

LOCKS=(ShiftLock Dslr)
FR=(0.00001 0.0001 0.001 0.01)
TIME=10

cargo build --release --workspace --quiet --features recovery

# Run experiments.
for lock in ${LOCKS[@]}; do
    for fr in ${FR[@]}; do
        echo "Running $lock with failrate=$fr..."
        $SCRIPT_DIR/../utils/kill.sh >> /dev/null 2>&1

        temp_file=$(mktemp)
        run_once "$SCRIPT_DIR/../run-fallible.sh 10.0.2.110:31850 $fr $lock" 2>&1 | tee "$temp_file"
        last_lines=$(tail -n 10 "$temp_file")
        echo "$last_lines" > $SCRIPT_DIR/../../data/exp8/$lock-$fr.txt
        rm "$temp_file"
    done
done

# Restore a clean build
cargo build --release --workspace --quiet
