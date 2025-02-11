#!/bin/bash
# Experiment 1: Microbenchmark, with various skewness and read/write ratios.
# - Figure 4: Goodputs of locks under the microbenchmark.
# - Figure 5: Latencies of locks under the microbenchmark.
#
# Estimated run time: ~20min

SCRIPT_DIR=$(dirname $(readlink -f "$0"))
source $SCRIPT_DIR/../utils/run_once.fn.sh

mkdir -p $SCRIPT_DIR/../../data/exp1
rm -rf $SCRIPT_DIR/../../data/exp1/*
cargo build --release --workspace --quiet

LOCKS=(Mcs ShiftLock Cas Dslr Drtm Rmarw Rpc)
SKEWNESS=(zipf uniform)
WR=(wi ri ro)

TIME=10

# Run experiments.
for lock in ${LOCKS[@]}; do
    for skew in ${SKEWNESS[@]}; do
        for wr in ${WR[@]}; do
            $SCRIPT_DIR/../utils/kill.sh
            workload="micro:$skew,$wr:$TIME"
            echo "Running $lock with $workload..."

            run_once "$SCRIPT_DIR/../run-basic.sh 10.0.2.110:31850 $lock $workload"

            # Modify the output directory name
            output_dir=$(ls $SCRIPT_DIR/../../data | grep $lock)
            mv $SCRIPT_DIR/../../data/$output_dir $SCRIPT_DIR/../../data/exp1/$lock-$skew-$wr
        done
    done
done
