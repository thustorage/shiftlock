#!/bin/bash

SCRIPT_DIR=$(dirname $(readlink -f "$0"))
rm -rf $SCRIPT_DIR/../../data/exp*
