#!/bin/bash
# Experiment 3: Microbenchmark.
# - Figure 6: Average numbers of RDMA one-sided verbs required to acquire and release a lock.
#
# Estimated run time: ~4min

SCRIPT_DIR=$(dirname $(readlink -f "$0"))
source $SCRIPT_DIR/../utils/run_once.fn.sh

mkdir -p $SCRIPT_DIR/../../data/exp3
rm -rf $SCRIPT_DIR/../../data/exp3/*
cargo build --release --workspace --quiet

LOCKS=(Mcs Handlock Cas Dslr Drtm Rmarw)

# Run experiments.
for lock in ${LOCKS[@]}; do
    $SCRIPT_DIR/../utils/kill.sh        
    echo "Running $lock..."

    run_once "$SCRIPT_DIR/../run-counters.sh 10.0.2.110:31850 $lock"

    # Modify the output directory name
    output_dir=$(ls $SCRIPT_DIR/../../data | grep $lock)
    mv $SCRIPT_DIR/../../data/$output_dir $SCRIPT_DIR/../../data/exp3/$lock
done

# An extra experiment for CAS-NB.
if [[ ! -z $(echo ${LOCKS[@]} | grep "Cas") ]]; then
    run_once "$SCRIPT_DIR/../run-counters.sh 10.0.2.110:31850 Cas 0"
    output_dir=$(ls $SCRIPT_DIR/../../data | grep Cas)
    mv $SCRIPT_DIR/../../data/$output_dir $SCRIPT_DIR/../../data/exp3/CasNB
fi
