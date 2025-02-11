#!/bin/bash
# Experiment 1: Microbenchmark, with various skewness and read/write ratios.
#               Run a single data point.
# - Figure 3: Goodputs of locks under the microbenchmark.
# - Figure 4: Latencies of locks under the microbenchmark.
#
# Estimated run time: ~20min

SCRIPT_DIR=$(dirname $(readlink -f "$0"))
source $SCRIPT_DIR/../utils/run_once.fn.sh

mkdir -p $SCRIPT_DIR/../../data/exp1
cargo build --release --workspace --quiet

LOCK=${1:-"ShiftLock"}
SKEWNESS=${2:-"uniform"}
WR=${3:-"ri"}

rm -rf $SCRIPT_DIR/../../data/exp1/$LOCK-$SKEWNESS-$WR
TIME=10

# Run experiment.
workload="micro:$SKEWNESS,$WR:$TIME"
echo "Running $LOCK with $workload..."

run_once "$SCRIPT_DIR/../run-basic.sh 10.0.2.110:31850 $LOCK $workload"

# Modify the output directory name
output_dir=$(ls $SCRIPT_DIR/../../data | grep $LOCK)
mv $SCRIPT_DIR/../../data/$output_dir $SCRIPT_DIR/../../data/exp1/$LOCK-$SKEWNESS-$WR
