#!/bin/bash
# Script to build the Linux .so library for ThunderDB

set -e

echo "Building Linux .so library..."
cargo build --release

echo "Library available at: target/release/libthunderdb.so"
ls -lh target/release/libthunderdb.so
