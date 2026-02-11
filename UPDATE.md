# ThunderDB-Rust — Performance & Optimization Roadmap

This document lists all identified performance bottlenecks, architectural inefficiencies, and proposed optimizations, ordered by impact. Each section includes the problem, affected files, and the recommended fix.

---

## Table of Contents

1. [Critical — Storage Layer](#1-critical--storage-layer)
2. [Critical — Index Layer](#2-critical--index-layer)
3. [High — Query Execution](#3-high--query-execution)
4. [High — Serialization & I/O](#4-high--serialization--io)
5. [Medium — Parser & SQL Pipeline](#5-medium--parser--sql-pipeline)
6. [Medium — Concurrency & Atomics](#6-medium--concurrency--atomics)
7. [Low — Housekeeping & Ergonomics](#7-low--housekeeping--ergonomics)
8. [Benchmark Gaps](#8-benchmark-gaps)

---

## 1. Critical — Storage Layer

### 1.1 RAT Insert is O(n) — Replace with BTreeMap ✅ DONE (P0)

**File:** `src/storage/rat.rs`

**Problem:** `RatTable` stores entries in a `Vec<RatEntry>` kept in sorted order. Every insert does `binary_search` + `Vec::insert`, which is **O(n)** because `Vec::insert` shifts all trailing elements. At 1M rows, each insert shifts up to 1M entries.

**Fix:** Replace `Vec<RatEntry>` with `BTreeMap<u64, RatEntry>` (keyed by `row_id`). This gives O(log n) insert, O(log n) lookup, and ordered iteration for free. The serialization format on disk stays the same — just change the in-memory representation.

```rust
// Before
pub struct RatTable {
    entries: Vec<RatEntry>,
}

// After
use std::collections::BTreeMap;

pub struct RatTable {
    entries: BTreeMap<u64, RatEntry>,
}
```

**Impact:** Turns the #1 bottleneck for large tables from O(n) to O(log n) per insert. For 1M rows, this is ~1,000,000x fewer element shifts.

---

### 1.2 Batch Insert is Not Truly Batched ✅ DONE (P0)

**File:** `src/storage/table_engine.rs`

**Problem:** `batch_insert()` calls `insert_row()` in a loop. Each call independently: serializes the row, seeks to end of data file, writes, updates RAT, and updates every index. There is no amortization.

**Fix:** Implement a real batch path:
- Serialize all rows into a single buffer, then write once (one seek, one write syscall).
- Collect all RAT entries, then insert them in bulk (sorted merge if using Vec, or batch insert into BTreeMap).
- Collect all index entries per column, then batch-insert into each B-Tree (sorted insert is faster — avoids random tree walks).
- Call `fsync` once at the end, not per-row.

```rust
pub fn batch_insert(&mut self, rows: &[Vec<Value>]) -> Result<Vec<u64>, ThunderError> {
    let mut buffer = Vec::with_capacity(rows.len() * 128);
    let mut rat_entries = Vec::with_capacity(rows.len());
    let mut index_entries: HashMap<String, Vec<(Value, u64)>> = HashMap::new();

    let base_offset = self.data_file.current_offset();
    for row_values in rows {
        let row_id = self.next_row_id.fetch_add(1, Ordering::Relaxed);
        let row = Row::new(row_id, row_values.clone());
        let start = buffer.len();
        row.serialize_into(&mut buffer);
        let length = buffer.len() - start;
        rat_entries.push(RatEntry::new(row_id, base_offset + start as u64, length as u32));
        // collect index entries...
    }

    self.data_file.write_all(&buffer)?;
    self.rat.bulk_insert(rat_entries);
    // bulk index update...
    Ok(ids)
}
```

**Impact:** 5-20x throughput improvement for bulk loads depending on row count and I/O subsystem.

---

### 1.3 Tombstone Accumulation — No Automatic Compaction

**Files:** `src/storage/data_file.rs`, `src/storage/rat.rs`

**Problem:** UPDATE and DELETE mark rows as deleted (tombstone) but never reclaim space. Over time:
- Data file grows unboundedly with dead data.
- RAT keeps deleted entries (flagged), increasing scan time.
- Full table scans read dead rows and discard them.

**Fix:** Implement background compaction:
1. **Online compaction** — Rewrite data file skipping tombstones, update RAT offsets atomically.
2. **Threshold trigger** — Auto-compact when `deleted_count / total_count > 0.3` (configurable).
3. **RAT compaction** — Already has `compact()` method but it's never called automatically; wire it into post-delete or periodic maintenance.

Add a configuration option:
```rust
pub struct StorageConfig {
    pub compaction_threshold: f64,  // 0.3 = 30% dead rows triggers compaction
    pub auto_compact: bool,
}
```

**Impact:** Prevents unbounded storage growth and keeps scan performance stable.

---

### 1.4 Data File Write Buffering

**File:** `src/storage/data_file.rs`

**Problem:** Each `append_row()` does a `seek_to_end` + `write`. For high-throughput inserts, this means one syscall per row.

**Fix:** Add a write-ahead buffer that flushes on configurable thresholds:
- Flush after N rows or M bytes.
- Flush on explicit `sync()` call.
- Flush before any read that might hit unflushed data.

This is distinct from OS-level buffering — it avoids repeated `seek` syscalls and batches small writes into larger ones.

**Impact:** 2-5x insert throughput improvement by reducing syscall overhead.

---

## 2. Critical — Index Layer

### 2.1 B-Tree Node Storage: HashMap → Vec (Arena) ✅ DONE (P0)

**File:** `src/index/btree.rs`

**Problem:** B-Tree nodes are stored in `HashMap<u64, BTreeNode>`. Every tree traversal (insert, search, range scan) does HashMap lookups, which have:
- Hash computation overhead
- Poor cache locality (nodes scattered in heap)
- Pointer chasing through hash buckets

**Fix:** Replace with arena-style `Vec<BTreeNode>` where `node_id` is the index. This gives O(1) lookup with perfect cache locality for sequential node IDs.

```rust
// Before
pub struct BTree<K, V> {
    nodes: HashMap<u64, BTreeNode<K, V>>,
    // ...
}

// After
pub struct BTree<K, V> {
    nodes: Vec<Option<BTreeNode<K, V>>>,  // node_id = index
    free_list: Vec<u64>,                   // recycled slots
    // ...
}
```

**Impact:** 2-4x improvement in tree traversal speed due to cache-friendly layout and eliminated hashing.

---

### 2.2 Index Deletion Not Implemented ✅ DONE (P1)

**File:** `src/index/manager.rs`, `src/index/btree.rs`

**Problem:** `IndexManager::delete_row()` is a no-op (`// TODO`). Deleted rows remain in every index. Queries must post-filter results against the RAT to check liveness. This means:
- Index size grows monotonically.
- Range scans return stale entries.
- Point lookups may return deleted row IDs.

**Fix:** Implement B-Tree deletion:
1. **Lazy deletion** (simpler) — Mark entries as deleted in the leaf node; compact during rebuild.
2. **Eager deletion** (correct) — Remove the key-value pair from the leaf and rebalance (merge underflowing nodes).

Lazy is recommended as a first step since the B-Tree already needs rebuild on load:

```rust
// In BTreeNode leaf:
pub struct LeafData<K, V> {
    keys: Vec<K>,
    values: Vec<V>,
    deleted: BitVec,  // bit flag per entry
}
```

**Impact:** Keeps index size proportional to live data. Prevents query result pollution with stale entries.

---

### 2.3 Index Load is O(n log n) — Serialize Tree Structure ✅ DONE (P1)

**File:** `src/index/persist.rs`

**Problem:** Index persistence saves all key-value pairs as a flat list. Loading reconstructs the B-Tree by re-inserting every entry, which is **O(n log n)**. For a 1M-entry index, this is ~20M comparisons on startup.

**Fix:** Serialize the tree structure directly — save nodes with their relationships instead of flattening:
```
[Header]
[Node 0: type, keys, values/children, next_leaf]
[Node 1: ...]
...
[Footer: root_id, node_count]
```

Loading becomes O(n) — just deserialize each node into the arena.

**Impact:** Database startup/recovery time drops from O(n log n) to O(n). For 1M entries, roughly 20x faster index load.

---

### 2.4 Duplicate Key Backtracking is Linear

**File:** `src/index/btree.rs`

**Problem:** When searching for a key that has duplicates, `search()` finds one match, then backtracks through the leaf chain to find the first occurrence. This backtrack is O(d) where d = number of duplicates. For low-cardinality columns (e.g., status = "active" on 90% of rows), this is nearly a full index scan.

**Fix:** Store a `first_occurrence_leaf` pointer for each unique key in internal nodes. Alternatively, during insertion, always insert duplicates to the right of existing keys (append semantics), so the first match found by binary search is always the leftmost.

Simpler fix: change the binary search in `find_position` to use `partition_point` which always returns the leftmost insertion position, making the forward scan sufficient without backtracking.

**Impact:** Eliminates O(d) backtracking for duplicate-heavy columns.

---

### 2.5 Implement Bloom Filters for Existence Checks

**File:** New — `src/index/bloom.rs`

**Problem:** Every WHERE clause with `=` currently requires a full B-Tree traversal. For non-existent keys, this traversal is wasted.

**Fix:** Add a Bloom filter per indexed column. Before B-Tree lookup, check the Bloom filter. If the key is definitely absent, skip the tree entirely.

```rust
pub struct BloomFilter {
    bits: Vec<u64>,
    num_hashes: u32,
    num_bits: usize,
}
```

Bloom filters are tiny (8KB for 100K entries at 1% false positive rate) and O(k) where k = number of hash functions (typically 3-7).

**Impact:** Eliminates tree traversal for non-existent keys. Particularly valuable for JOIN-like operations and IN clauses.

---

## 3. High — Query Execution

### 3.1 Push Filters Down to Storage Layer ✅ DONE (P1)

**File:** `src/query/direct.rs`

**Problem:** `scan_with_limit()` fetches all matching rows from the index (or full scan), collects them into a `Vec`, then applies remaining filters, then applies offset/limit. This means:
- All rows are materialized even if only 10 are needed.
- Offset skips rows that were already fetched and filtered.

**Fix:** Implement an iterator/streaming model:
1. Return an `Iterator<Item = Row>` instead of `Vec<Row>`.
2. Apply filters during iteration (short-circuit as early as possible).
3. Skip `offset` rows without materializing.
4. Stop after `limit` rows.

```rust
pub fn scan_streaming<'a>(
    &'a self,
    filters: &'a [Filter],
    offset: usize,
    limit: usize,
) -> impl Iterator<Item = Row> + 'a {
    self.data_file.scan_iter()
        .filter(move |row| filters.iter().all(|f| f.matches(row)))
        .skip(offset)
        .take(limit)
}
```

**Impact:** For `LIMIT 10 OFFSET 1000`, currently materializes 1010+ rows. With streaming, materializes exactly 1010 rows with zero intermediate `Vec` allocations.

---

### 3.2 Index Selection Should Use Statistics

**File:** `src/query/direct.rs`

**Problem:** `choose_index()` picks the first indexable filter it finds, preferring Equals over Range over Like. It does not consider selectivity. A filter on `status = 'active'` (90% of rows) is chosen over `age BETWEEN 25 AND 30` (5% of rows) just because `Equals` has higher priority.

**Fix:** Use the index statistics from `src/index/stats.rs` to estimate cardinality:
```rust
fn choose_index(filters: &[Filter], stats: &IndexStats) -> Option<(usize, &Filter)> {
    filters.iter()
        .enumerate()
        .filter(|(_, f)| f.can_use_index())
        .min_by_key(|(_, f)| stats.estimated_rows(f))
}
```

**Impact:** Choosing the most selective index can reduce rows scanned by 10-100x in real workloads.

---

### 3.3 Multi-Index Intersection

**File:** `src/query/direct.rs`

**Problem:** Only one index is used per query. If a query has `WHERE age = 25 AND city = 'Rome'`, only one index is consulted; the other filter is applied post-scan.

**Fix:** When multiple indexed filters exist:
1. Query each index separately to get row ID sets.
2. Intersect the sets (sorted merge or bitset AND).
3. Fetch only the intersected row IDs from the data file.

```rust
fn multi_index_scan(&self, filters: &[Filter]) -> Vec<u64> {
    let mut sets: Vec<Vec<u64>> = filters.iter()
        .filter(|f| f.can_use_index() && self.has_index(f.column()))
        .map(|f| self.index_lookup(f))
        .collect();

    sets.sort_by_key(|s| s.len());  // smallest first
    // intersect progressively...
}
```

**Impact:** Dramatically reduces rows fetched from storage for multi-predicate queries.

---

### 3.4 Column Projection Push-Down

**File:** `src/query/direct.rs`, `src/storage/table_engine.rs`

**Problem:** `SELECT col1, col2 FROM ...` fetches entire rows (all columns) from storage, then the caller extracts the needed columns. For wide tables, this wastes bandwidth and deserialization effort.

**Fix:** Pass the requested column indices to the storage layer. During row deserialization, skip unneeded columns:

```rust
fn deserialize_projected(data: &[u8], columns: &[usize]) -> Vec<Value> {
    // Read header, then skip values not in `columns` set
}
```

**Impact:** Reduces deserialization work proportionally to `selected_columns / total_columns`.

---

## 4. High — Serialization & I/O

### 4.1 Value Deserialization Allocates Strings

**File:** `src/storage/value.rs`

**Problem:** Every Varchar deserialization creates a new `String` via `String::from_utf8()`. For scan-heavy workloads, this creates millions of short-lived allocations.

**Fix (short-term):** Use a string interner or arena allocator for deserialized strings within a query scope:
```rust
pub struct QueryArena {
    strings: bumpalo::Bump,
}
```

**Fix (long-term):** Implement zero-copy deserialization where `Value::Varchar` holds a `&[u8]` slice into the read buffer instead of an owned `String`. This requires lifetime management but eliminates all string allocation during reads.

**Impact:** 30-50% reduction in allocator pressure during full table scans.

---

### 4.2 Row Serialization: Avoid Intermediate Vec

**File:** `src/storage/row.rs`

**Problem:** `to_bytes()` creates a `Vec<u8>`, serializes into it, then the caller writes that Vec to the data file. This is an unnecessary intermediate allocation.

**Fix:** Serialize directly into the data file's write buffer or into a reusable buffer:
```rust
pub fn serialize_into<W: Write>(&self, writer: &mut W) -> io::Result<usize> {
    writer.write_all(&self.row_id.to_le_bytes())?;
    writer.write_all(&(self.values.len() as u32).to_le_bytes())?;
    for value in &self.values {
        value.write_to(writer)?;
    }
    Ok(bytes_written)
}
```

The `write_to` pattern already exists for `Value` but `Row` doesn't use it — it converts to bytes first.

**Impact:** Eliminates one allocation per row write.

---

### 4.3 fsync Strategy: Group Commit ✅ DONE (P1)

**File:** `src/storage/data_file.rs`

**Problem:** With `fsync_on_write: true`, every row write triggers a disk sync, which is extremely slow (~10ms per fsync on spinning disk, ~0.1ms on NVMe). With `fsync_on_write: false`, durability is not guaranteed.

**Fix:** Implement group commit:
- Accumulate writes in a buffer.
- Flush + fsync on a timer (e.g., every 100ms) or when buffer exceeds a threshold.
- Return "durable" only after the group containing the write is synced.

This batches the fsync cost across many writes while maintaining durability guarantees.

**Impact:** 10-100x write throughput with fsync enabled.

---

### 4.4 Use `BufWriter` for Data File ✅ DONE (P1)

**File:** `src/storage/data_file.rs`

**Problem:** Raw `File` writes go through the OS page cache but each `write()` call is still a syscall. For many small writes (row-by-row), syscall overhead dominates.

**Fix:** Wrap the file in `BufWriter<File>` with a large buffer (64KB-256KB). Flush explicitly before reads and on sync.

```rust
use std::io::BufWriter;

pub struct DataFile {
    writer: BufWriter<File>,
    // ...
}
```

**Impact:** Reduces syscall count by 100-1000x for sequential inserts.

---

## 5. Medium — Parser & SQL Pipeline

### 5.1 Tokenizer: Avoid Vec\<char\> Allocation

**File:** `src/parser/token.rs`

**Problem:** The tokenizer converts the input `&str` into `Vec<char>`, allocating 4 bytes per character. For a 1KB SQL statement, this allocates 4KB.

**Fix:** Iterate directly over `str::chars()` or use byte-level parsing with `&[u8]`. The tokenizer only needs ASCII keywords and simple string literals, so byte-level parsing works:

```rust
pub struct Tokenizer<'a> {
    input: &'a [u8],
    pos: usize,
}
```

**Impact:** Eliminates tokenizer heap allocation entirely.

---

### 5.2 AST: Reduce Box Allocations

**File:** `src/parser/ast.rs`

**Problem:** Expressions use `Box<Expression>` for every binary op, unary op, function call, etc. A complex WHERE clause like `a > 1 AND b < 2 AND c = 3` creates 5+ Box allocations.

**Fix:** Use an expression arena (index-based references instead of Box):
```rust
pub struct ExprArena {
    nodes: Vec<ExprNode>,
}

pub struct ExprRef(u32);  // index into arena

pub enum ExprNode {
    BinaryOp { op: Op, left: ExprRef, right: ExprRef },
    Literal(Value),
    Column(String),
    // ...
}
```

**Impact:** Eliminates all Box allocations during parsing. Improves cache locality for expression evaluation.

---

### 5.3 Prepared Statement Cache

**Files:** `src/parser/mod.rs`, `src/lib.rs`

**Problem:** Every SQL execution re-tokenizes, re-parses, re-validates, and re-plans the query. For REPL workloads or application loops executing similar queries, this is redundant.

**Fix:** Cache parsed + validated ASTs keyed by the SQL string (or a hash of it):
```rust
use std::collections::HashMap;

pub struct PreparedCache {
    cache: HashMap<u64, Statement>,  // hash of SQL → parsed AST
    max_size: usize,
}
```

For parameterized queries, normalize the SQL (replace literals with `?`) before hashing.

**Impact:** Eliminates parse overhead for repeated queries. Typical speedup: 5-10x for parse-bound workloads.

---

## 6. Medium — Concurrency & Atomics

### 6.1 Relax Atomic Ordering

**File:** `src/storage/table_engine.rs`

**Problem:** `next_row_id` uses `Ordering::SeqCst` (sequentially consistent), the strongest and slowest ordering. Row ID generation only needs monotonicity, not global sequential consistency.

**Fix:** Use `Ordering::Relaxed` for the fetch_add. Since row IDs only need to be unique (not globally ordered with respect to other atomic operations), Relaxed is sufficient:

```rust
let row_id = self.next_row_id.fetch_add(1, Ordering::Relaxed);
```

**Impact:** Marginal on x86 (SeqCst and Relaxed compile to the same instruction for fetch_add), but significant on ARM/RISC-V (eliminates memory fence).

---

### 6.2 Reader-Writer Concurrency

**File:** `src/storage/table_engine.rs`, `src/lib.rs`

**Problem:** The entire database is single-threaded. `&mut self` is required for all operations, meaning no concurrent reads.

**Fix (future):** Introduce `RwLock<TableEngine>` at the table level:
- Multiple concurrent readers for SELECT.
- Exclusive writer for INSERT/UPDATE/DELETE.
- Consider per-table locking (not global) for cross-table parallelism.

```rust
pub struct Database {
    tables: HashMap<String, RwLock<TableEngine>>,
}
```

**Impact:** Enables multi-threaded query execution. Required for any server/connection-pool architecture.

---

## 7. Low — Housekeeping & Ergonomics

### 7.1 `list_tables()` Scans Disk Every Call

**File:** `src/lib.rs`

**Problem:** `list_tables()` reads the filesystem directory on every invocation to discover tables. This is an I/O operation that should be cached.

**Fix:** Maintain an in-memory `HashSet<String>` of known table names. Update it on create/drop/open. Only scan disk on startup.

**Impact:** Eliminates unnecessary I/O on a frequently-called operation.

---

### 7.2 Formatter: Pre-compute Column Widths

**File:** `src/repl/formatter.rs`

**Problem:** Column width computation iterates all rows twice (once for width, once for display).

**Fix:** Compute widths in a single pass while building the output string. Use a `Vec<usize>` for max widths and update incrementally.

**Impact:** Minor — only affects REPL display, not core performance.

---

### 7.3 Config Validation: Move to Compile-Time

**File:** `src/config/types.rs`

**Problem:** Several config parameters are defined but never used (`node_cache_size`, `lazy_update_threshold`, `rebuild_threshold`, `max_data_file_size_mb`). Dead config creates confusion.

**Fix:** Remove unused configuration fields until they're actually implemented. This avoids misleading users and simplifies the config surface.

**Impact:** Code clarity only — no performance effect, but prevents premature optimization attempts based on non-functional knobs.

---

## 8. Benchmark Gaps

The current benchmarks only cover a fraction of the performance surface. Add:

| Benchmark | Why |
|---|---|
| **Batch insert (1K, 10K, 100K rows)** | Validate batch path performance |
| **UPDATE throughput** | Updates are expensive (delete + insert + index update) |
| **DELETE throughput** | Tombstone marking performance |
| **Index build time** | Cost of `CREATE INDEX` on existing table |
| **Index load time** | Startup cost for large indices |
| **Compaction** | Time to compact a table with 50% tombstones |
| **Large dataset (1M+ rows)** | Expose O(n) bottlenecks in RAT, index |
| **Memory usage** | Track heap usage under load |
| **Concurrent reads** | When RwLock is added |
| **Mixed read/write** | Realistic OLTP workload |

---

## Summary — Priority Matrix

| Priority | Item | Expected Speedup | Effort | Status |
|---|---|---|---|---|
| **P0** | 1.1 RAT → BTreeMap | 1000x insert at scale | Low | **DONE** |
| **P0** | 2.1 BTree nodes → Vec arena | 2-4x tree ops | Medium | **DONE** |
| **P0** | 1.2 True batch insert | 5-20x batch insert | Medium | **DONE** |
| **P1** | 2.2 Index deletion | Prevents index bloat | Medium | **DONE** |
| **P1** | 3.1 Streaming/iterator model | 10x for LIMIT queries | Medium | **DONE** |
| **P1** | 4.4 BufWriter for data file | 2-5x insert throughput | Low | **DONE** |
| **P1** | 4.3 Group commit (fsync) | 10-100x durable writes | Medium | **DONE** |
| **P1** | 2.3 Serialize tree structure | 20x index load | High | **DONE** |
| **P2** | 3.2 Statistics-based index selection | 10-100x selective queries | Medium | |
| **P2** | 3.3 Multi-index intersection | 5-50x multi-filter | Medium | |
| **P2** | 4.1 String arena / zero-copy | 30-50% scan alloc | High | |
| **P2** | 5.3 Prepared statement cache | 5-10x repeated queries | Low | |
| **P2** | 1.3 Auto-compaction | Stable performance | Medium | |
| **P3** | 3.4 Column projection push-down | Proportional to width | Medium | |
| **P3** | 5.1 Tokenizer byte parsing | Eliminates alloc | Low | |
| **P3** | 5.2 Expression arena | Better cache locality | Medium | |
| **P3** | 2.5 Bloom filters | Avoid wasted lookups | Low | |
| **P3** | 6.1 Relax atomic ordering | ARM speedup | Trivial | |
| **P3** | 7.1 Cache table list | Eliminates I/O | Trivial | |
| **P4** | 6.2 Reader-writer concurrency | Enables parallelism | High | |
| **P4** | 4.2 Direct row serialization | 1 alloc per write | Low | |
| **P4** | 7.2 Formatter single-pass | REPL only | Trivial | |
| **P4** | 7.3 Remove dead config | Code clarity | Trivial | |

---

## P0 — Completed (2026-02-11)

All three P0 items shipped in a single commit:

- **RAT → BTreeMap:** `Vec<RatEntry>` replaced with `BTreeMap<u64, RatEntry>`. O(n) insert → O(log n). Added `bulk_insert()`.
- **BTree Vec arena:** `HashMap<u64, BTreeNode>` replaced with `Vec<BTreeNode>`. O(1) cache-friendly lookups. Added O(1) `len()`/`is_empty()` via `entry_count`.
- **True batch insert:** `append_rows_batch()` in DataFile (single I/O), bulk RAT insert, column mapping computed once. Single `fsync` at end.

---

## P1 — Completed (2026-02-11)

All five P1 items shipped in a single commit:

- **Index deletion (2.2):** `BTree::delete(key, value)` walks to leaf, removes matching pair, decrements `entry_count`. No rebalancing — underflow is harmless since tree rebuilds on load. `IndexManager::delete_row` now accepts row values + column mapping. `TableEngine::delete_by_id` reads the row before deleting to pass values to index. `update_row` removes old index entries before inserting new ones.
- **BufWriter (4.4):** `File` wrapped in `BufWriter<File>` with 256KB buffer. All writes go through BufWriter. Before any read (`read_row`, `scan_rows`, `scan_all`), the buffer is flushed via `writer.flush()`, then `get_mut()` accesses the inner `File`. `mark_deleted` (random-access write) flushes first too.
- **Group commit (4.3):** Added `group_commit_interval_ms` to `StorageConfig` (default: 0 = disabled). `DataFile` tracks `last_sync: Option<Instant>`. `maybe_sync()` checks elapsed time — only calls `fsync` if threshold exceeded. All write methods use `maybe_sync()`. `sync()` always forces a real sync.
- **Persist v2 (2.3):** New binary format serializes tree structure directly. Header: magic, version, order, root_id, node_count, first_leaf_id, entry_count. Per node: type, keys, values/children, parent, next_leaf. `save_index` iterates `tree.nodes()` directly. `load_index` reads nodes and calls `BTree::from_parts()`. O(n) load vs O(n log n). Backward compat: v1 files still load via re-insert path.
- **Streaming queries (3.1):** `scan_with_limit()` refactored for single-pass: iterate → filter → skip offset → collect up to limit → break. No intermediate `Vec` for full result set. Early `break` avoids processing remaining rows.

---

## P2 — Roadmap (Next)

### P2.1 Statistics-Based Index Selection (3.2)

**Files:** `src/query/direct.rs`, `src/index/stats.rs`, `src/index/manager.rs`

**Problem:** `choose_index()` picks the first indexable filter by operator priority (Equals > Range > Like). It ignores selectivity — a filter on `status = 'active'` matching 90% of rows beats `age BETWEEN 25 AND 30` matching 5% just because Equals ranks higher.

**Approach:**
1. Have `IndexManager` cache `IndexStatistics` per column (computed on load/rebuild, updated incrementally on insert/delete).
2. Modify `choose_index()` to accept stats and estimate result size:
   - Equals: `total_entries / cardinality`
   - Range: estimate fraction based on min/max bounds
   - Like prefix: estimate based on prefix selectivity
3. Pick the filter with the smallest estimated result set.

**Impact:** 10-100x fewer rows scanned for queries on low-selectivity indexed columns.

---

### P2.2 Multi-Index Intersection (3.3)

**Files:** `src/query/direct.rs`, `src/lib.rs`

**Problem:** Only one index is used per query. `WHERE age = 25 AND city = 'Rome'` uses one index and post-filters the rest.

**Approach:**
1. Identify all filters that have a matching index.
2. Query each index to get a `Vec<u64>` of row IDs.
3. Sort sets by size (smallest first), then intersect progressively via sorted merge.
4. Fetch only intersected row IDs from storage.
5. Apply any remaining non-indexed filters.

**Key decision:** Sorted merge (O(n+m)) vs HashSet intersection (O(min(n,m))). Sorted merge is better for our case since index results are already sorted.

**Impact:** 5-50x fewer rows fetched for multi-predicate queries with multiple indices.

---

### P2.3 String Arena / Zero-Copy Deserialization (4.1)

**Files:** `src/storage/value.rs`, `src/storage/row.rs`

**Problem:** Every `Value::Varchar` deserialization allocates a new `String`. Full table scans create millions of short-lived heap strings.

**Approach (phased):**
1. **Phase A — Reusable buffer:** Add a `QueryArena` that pre-allocates a large byte buffer. Deserialize Varchar values as slices into this buffer within a query scope. Requires lifetime annotation on `Value<'a>`.
2. **Phase B — Zero-copy:** For the read path, keep the raw read buffer alive and have `Value::Varchar` hold a `&[u8]` reference into it. This eliminates all string allocation during reads but requires careful lifetime management.

Phase A is the pragmatic first step. Phase B is a larger refactor.

**Impact:** 30-50% reduction in allocator pressure during full table scans.

---

### P2.4 Prepared Statement Cache (5.3)

**Files:** `src/parser/mod.rs`, `src/lib.rs`

**Problem:** Every SQL execution re-tokenizes, re-parses, and re-validates. For REPL loops or application code executing similar queries, this work is redundant.

**Approach:**
1. Add a `PreparedCache` with an LRU eviction policy (capacity configurable, default 128).
2. Key: hash of the normalized SQL string (literals replaced with `?` placeholders).
3. Value: parsed `Statement` + extracted literal values.
4. On cache hit: substitute literals into the cached AST and skip parse+validate.

```rust
pub struct PreparedCache {
    cache: HashMap<u64, (Statement, Vec<Value>)>,
    lru: VecDeque<u64>,
    capacity: usize,
}
```

**Impact:** 5-10x speedup for repeated queries. Parse overhead drops to near-zero for cached statements.

---

### P2.5 Auto-Compaction (1.3)

**Files:** `src/storage/table_engine.rs`, `src/storage/data_file.rs`, `src/config/types.rs`

**Problem:** UPDATE and DELETE leave tombstones. Over time, the data file grows unboundedly with dead data, and full scans read/skip dead rows.

**Approach:**
1. Add `compaction_threshold: f64` to `StorageConfig` (default: 0.5 = 50% dead rows triggers compaction).
2. After each delete, check `deleted_count / total_count > threshold`.
3. Compaction procedure:
   - Create `data.bin.new`, copy only active rows (sequential read + sequential write).
   - Build new RAT from the fresh offsets.
   - Atomic rename `data.bin.new` → `data.bin`.
   - Rebuild indices from the compacted data.
4. RAT compaction: call `rat.compact()` automatically after table compaction.

**Impact:** Prevents unbounded storage growth. Keeps scan performance stable over time.

---

### P2 Summary

| # | Item | Expected Impact | Effort | Dependencies |
|---|---|---|---|---|
| 1 | Statistics-based index selection | 10-100x selective queries | Medium | Needs IndexStatistics caching |
| 2 | Multi-index intersection | 5-50x multi-filter | Medium | Benefits from #1 for set ordering |
| 3 | String arena / zero-copy | 30-50% scan alloc | High | Lifetime refactor on Value |
| 4 | Prepared statement cache | 5-10x repeated queries | Low | Independent |
| 5 | Auto-compaction | Stable performance | Medium | Independent |

Recommended order: **4 → 1 → 2 → 5 → 3** (low effort wins first, then query optimizer, then the lifetime refactor last).

---

*Generated on 2026-02-11 — based on full codebase review of ThunderDB-Rust v0.1.0. Updated with P0/P1 completion status and P2 roadmap.*
