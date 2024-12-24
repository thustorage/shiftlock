#!/bin/bash

if [[ $# -lt 1 ]]; then
    echo "Zero the server memory"
    echo "Usage: zero.sh <SERVER_URI>"
    exit 1
fi

SCRIPT_DIR=$(dirname $(readlink -f "$0"))
$SCRIPT_DIR/../../target/release/zero --server $1
