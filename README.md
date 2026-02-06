# ThunderDB

A custom database engine written in Rust, designed for high performance with minimal dependencies.

## Features

- **Zero heavy dependencies**: Uses only minimal standard crates (serde, serde_json)
- **High performance**: Lazy B-Tree indexing and append-only storage
- **Dual interface**: SQL parser + direct type-safe API
- **Embeddable**: Designed to be embedded in applications
- **Future WebAssembly support**: Architecture ready for WASM compilation

## Architecture

### Storage Layer
- Append-only data file (`data.bin`) for durability
- Record Address Table (RAT) for fast lookups
- Value serialization with support for multiple data types

### Indexing
- B-Tree indices with lazy updates
- Support for multiple indices per table
- Efficient range queries and LIKE operations

### Query Layer
- **Direct API**: Type-safe CRUD operations with zero parsing overhead
- **SQL Interface**: Standard SQL support (SELECT, INSERT, UPDATE, DELETE)

### Interface
- REPL for interactive queries
- Embeddable library API

## Installation

```bash
cargo build --release
```

## Quick Start

### Using the REPL

```bash
cargo run --release

customdb> CREATE TABLE users (id INT64, email VARCHAR, age INT32);
customdb> INSERT INTO users VALUES (1, 'alice@example.com', 25);
customdb> SELECT * FROM users WHERE age > 20;
```

### Using the Direct API

```rust
use thunderdb::{Database, Value, Operator, Filter};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Database::open("./my_database")?;

    // Insert data
    let row_id = db.insert_row("users", vec![
        Value::Int64(1),
        Value::Varchar("alice@example.com".into()),
        Value::Int32(25),
    ])?;

    // Query data
    let results = db.scan("users", vec![
        Filter {
            column: "age".into(),
            operator: Operator::GreaterThan(Value::Int32(20)),
        }
    ])?;

    println!("Found {} users", results.len());
    Ok(())
}
```

## Configuration

Copy `config.example.json` to `config.json` and adjust settings:

```json
{
  "storage": {
    "data_dir": "./data",
    "fsync_on_write": false,
    "fsync_interval_ms": 1000
  },
  "index": {
    "btree_order": 100,
    "node_cache_size": 1000,
    "lazy_update_threshold": 100
  }
}
```

## Development Status

**Current Phase**: MVP Development

- [x] Project setup and configuration
- [x] Storage layer (Value, RAT, data.bin, TableEngine)
- [x] B-Tree indexing
- [x] Direct API
- [x] SQL parser
- [x] REPL interface
- [ ] Testing and benchmarks

## Testing

Run all tests including unit and integration tests:

```bash
cargo test
```

To run specifically the SQL command test suite:

```bash
cargo test --test blog_suite
```

## Performance Targets

- INSERT: >10,000 rows/sec (single), >50,000 rows/sec (batch)
- SELECT by ID: <1ms
- Range query (1000 rows): <10ms

## License

MIT License - see LICENSE file for details
