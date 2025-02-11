#!/bin/bash
# Experiment 4: Distributed Tansactions.
# - Figure 10: Performance of evaluated lock schemes under distributed transaction benchmarks.
#
# Estimated run time: ~10min

SCRIPT_DIR=$(dirname $(readlink -f "$0"))
source $SCRIPT_DIR/../utils/run_once.fn.sh

mkdir -p $SCRIPT_DIR/../../data/exp4
rm -rf $SCRIPT_DIR/../../data/exp4/*
cargo build --release --workspace --quiet

LOCKS=(Mcs ShiftLock Cas Dslr Drtm Rmarw Rpc)
WORKLOADS=(tatp tpcc-h)

TIME=5

# Run experiments.
for wl in ${WORKLOADS[@]}; do
    for lock in ${LOCKS[@]}; do
        $SCRIPT_DIR/../utils/kill.sh    
        workload="trace:$SCRIPT_DIR/../../trace-dist/$wl.csv:$TIME"
        echo "Running $lock with $wl..."

        run_once "$SCRIPT_DIR/../run-basic.sh 10.0.2.110:31850 $lock $workload"

        # Modify the output directory name
        output_dir=$(ls $SCRIPT_DIR/../../data | grep $lock)
        mv $SCRIPT_DIR/../../data/$output_dir $SCRIPT_DIR/../../data/exp4/$wl-$lock
    done
done
