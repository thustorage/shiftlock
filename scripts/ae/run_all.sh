#!/bin/bash

SCRIPT_DIR=$(dirname $(readlink -f "$0"))

$SCRIPT_DIR/exp1.sh
sleep 1
$SCRIPT_DIR/../utils/kill.sh

$SCRIPT_DIR/exp2.sh
sleep 1
$SCRIPT_DIR/../utils/kill.sh

$SCRIPT_DIR/exp3.sh
sleep 1
$SCRIPT_DIR/../utils/kill.sh

$SCRIPT_DIR/exp4.sh
sleep 1
$SCRIPT_DIR/../utils/kill.sh

$SCRIPT_DIR/exp5.sh
sleep 1
$SCRIPT_DIR/../utils/kill.sh

cargo clean

$SCRIPT_DIR/exp6.sh
sleep 1
$SCRIPT_DIR/../utils/kill.sh

cargo clean

$SCRIPT_DIR/exp7.sh
sleep 1
$SCRIPT_DIR/../utils/kill.sh

$SCRIPT_DIR/exp8.sh
$SCRIPT_DIR/../utils/kill.sh
