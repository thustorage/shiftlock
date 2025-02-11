#!/bin/bash
# Experiment 2: Microbenchmark, with various lock count.
# - Figure 7: Goodputs of locks with different lock counts.
#
# Estimated run time: ~25min

SCRIPT_DIR=$(dirname $(readlink -f "$0"))
source $SCRIPT_DIR/../utils/run_once.fn.sh

mkdir -p $SCRIPT_DIR/../../data/exp2
rm -rf $SCRIPT_DIR/../../data/exp2/*
cargo build --release --workspace --quiet

SKEWNESS=(zipf)
LOCKS=(Mcs ShiftLock Cas Dslr Drtm Rmarw Rpc)
NUMS=(1000 3162 10000 31623 100000 316228 1000000 3162278 10000000)

TIME=10

# Run experiments.
for skew in ${SKEWNESS[@]}; do
    for lock in ${LOCKS[@]}; do
        for num in ${NUMS[@]}; do
            $SCRIPT_DIR/../utils/kill.sh
            workload="micro:$skew,wi:$TIME"
            echo "Running $lock with $workload..."

            run_once "$SCRIPT_DIR/../run-nlocks.sh 10.0.2.110:31850 $lock $workload $num"

            # Modify the output directory name
            output_dir=$(ls $SCRIPT_DIR/../../data | grep $lock)
            mv $SCRIPT_DIR/../../data/$output_dir $SCRIPT_DIR/../../data/exp2/$skew-$lock-$num
        done
    done
done
