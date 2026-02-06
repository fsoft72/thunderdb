#!/bin/bash
# Script to build the Linux executable for ThunderDB

set -e

echo "Building Linux executable..."
cargo build --release --features repl

echo "Binary available at: target/release/thunderdb"
ls -lh target/release/thunderdb
