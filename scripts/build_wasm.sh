#!/bin/bash
# Script to build the WASM module for ThunderDB

set -e

if ! command -v wasm-pack &> /dev/null
then
    echo "wasm-pack could not be found. Please install it with:"
    echo "curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh"
    exit 1
fi

echo "Building WASM module..."
wasm-pack build --target web -- --features wasm

echo "WASM module available in the 'pkg' directory"
ls -lh pkg/
