# ThunderDB Build Instructions

This directory contains scripts to build ThunderDB for different targets.

## Prerequisites

- **Rust**: Ensure you have Rust and Cargo installed.
- **wasm-pack**: Required for building the WASM module.
  ```bash
  curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh
  ```

## Building

### 1. Linux Executable
Builds the standalone CLI tool with REPL support.
```bash
./scripts/build_linux_bin.sh
```
Result: `target/release/thunderdb`

### 2. Linux Shared Library (.so)
Builds the C-compatible shared library.
```bash
./scripts/build_linux_so.sh
```
Result: `target/release/libthunderdb.so`

### 3. WebAssembly Module
Builds the WASM module with JavaScript bindings.
```bash
./scripts/build_wasm.sh
```
Result: `pkg/` directory containing `thunderdb.wasm` and `thunderdb.js`.

## Running the WASM Example

To run the WASM example, you need a local web server because browsers block WASM loading from `file://` URLs due to CORS.

1. Build the WASM module: `./scripts/build_wasm.sh`
2. Start a web server from the project root:
   ```bash
   # Using Python
   python3 -m http.server 8080
   # Or using Node.js
   npx serve .
   ```
3. Open `http://localhost:8080/examples/wasm_example.html` in your browser.

**Note:** The current WASM implementation uses a dummy path for database initialization. In a real browser environment, `std::fs` operations will fail. For production use in the browser, a custom storage backend (e.g., using IndexedDB) would be required.
