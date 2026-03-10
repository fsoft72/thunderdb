## ThunderDB just got 10x faster

I've been building ThunderDB — a lightweight, embeddable database engine written in Rust with zero heavy dependencies — and I'm excited to share the latest round of improvements.

### What's new

**Correctness fixes** that make the engine production-ready:

- ORDER BY with LIMIT/OFFSET now works correctly — pagination happens after sorting, as it should
- Creating an index on a table that already has data now backfills automatically — no more silent empty-index bugs
- Float NaN values are handled safely in indexes instead of panicking
- Internal memory safety hardened by making SmallString internals private

**Performance improvements** that speak for themselves:

- **Full table scans are 10x faster** — buffered 256KB reads replaced thousands of individual syscalls
- **Filtered queries are 10x faster** — same scan optimization benefits all read paths
- **O(1) startup time** — row ID tracking uses BTreeMap's last key instead of scanning every ID
- **O(1) row counting** — cached active count, no more iterating the entire record table
- **Smarter disk access** — indexed row fetches are now reordered by physical offset, turning random I/O into sequential reads
- **Zero-allocation cardinality counting** — replaced HashSet + format!() with sorted comparison

### The numbers

```
scan_all_10k:            -90.5% (was ~15ms, now 1.4ms)
search_with_filter:      -91.0% (was ~15ms, now 1.4ms)
insert_single_row:       1.06 µs  (~944K rows/sec)
insert_batch_100:        15.1 µs  (~6.6M rows/sec)
get_by_id:               842 ns
```

Full scans over 10,000 rows went from ~15ms down to 1.4ms. Single inserts run under 2 microseconds. Batch inserts exceed 6 million rows per second.

### Why ThunderDB?

- **Zero heavy dependencies** — only serde/serde_json
- **Embeddable** — use it as a library in your Rust app
- **Dual interface** — type-safe Direct API or standard SQL
- **B-Tree indexing** — with range queries and LIKE support
- **WebAssembly ready** — architecture designed for WASM compilation

Sometimes you don't need Postgres. You need something small, fast, and embedded right in your binary.

ThunderDB is open source under MIT. Link in comments.

#rust #database #performance #opensource #engineering
