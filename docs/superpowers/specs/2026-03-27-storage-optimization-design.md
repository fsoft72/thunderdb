# Storage & I/O Optimization Design

Five optimizations targeting the remaining performance gaps vs SQLite: per-row
allocation cost, syscall overhead, and unnecessary deserialization.

## 1. Increase SmallString INLINE_CAP from 23 to 32

### Problem

Strings between 24-32 bytes (common for titles, names) allocate on the heap.

### Solution

Change `INLINE_CAP` in `src/storage/small_string.rs` from 23 to 32.

```rust
const INLINE_CAP: usize = 32;
```

Covers ~80% of typical short strings without heap allocation. Trade-off:
each inline SmallString uses 33 bytes of stack instead of 24, but this has
no measurable impact since `Value::Varchar(SmallString)` is already the
largest enum variant.

All existing tests pass without modification — the threshold is transparent
to consumers.

## 2. Zero-Copy read_raw_with() Callback

### Problem

`read_raw()` allocates a `Vec<u8>` per row via `.to_vec()`. In
`get_by_ids_filtered()` with 2000 rows, this means 2000 allocations even
when most rows are discarded by the predicate.

### Solution

New method on `DataFile`:

```rust
pub fn read_raw_with<F, R>(
    &mut self,
    offset: u64,
    length: u32,
    f: F,
) -> Result<Option<R>>
where
    F: FnOnce(&[u8]) -> R,
```

Reads into the existing `self.read_buffer` (reused across calls) and passes
a borrowed slice to the callback. Zero per-row allocations.

For the `Memory` backend, the callback receives a direct slice from the
in-memory data — no copy at all.

`get_by_ids_filtered()` is updated to use `read_raw_with()`:

```rust
for (offset, length) in entries {
    let maybe_row = self.data_file.read_raw_with(offset, length, |raw| {
        if predicate(raw) {
            Some(Row::from_bytes(raw))
        } else {
            None
        }
    })?;
    if let Some(Some(row)) = maybe_row {
        rows.push(row?);
    }
}
```

`read_raw()` remains available for callers that need owned bytes.

## 3. Batch I/O for Indexed Fetches

### Problem

`fetch_rows_sorted_by_offset()` does 1 seek + 1 read per row. For 2000
rows that's ~4000 syscalls. Rows are sorted by offset and often adjacent
on disk, but read individually.

### Solution

Group adjacent rows into clusters and read each cluster with a single I/O
operation.

New method on `DataFile`:

```rust
pub fn read_batch_sequential(
    &mut self,
    entries: &[(u64, u32)],  // (offset, length), sorted by offset
) -> Result<Vec<Row>>
```

Algorithm:
1. Scan `entries` and group into clusters. A new cluster starts when the
   gap between the end of the previous row and the start of the next
   exceeds `BATCH_GAP_THRESHOLD` (64 KB).
2. For each cluster, read the entire range
   `[first_offset .. last_offset + last_length + 5]` with a single read.
3. From the cluster buffer, extract each row using relative offsets
   (skipping marker + length prefix), call `Row::from_bytes()`.

Constant:

```rust
const BATCH_GAP_THRESHOLD: u64 = 64 * 1024;
```

`fetch_rows_sorted_by_offset()` and `get_by_ids()` are rewritten to use
`read_batch_sequential`.

A filtered variant `read_batch_sequential_filtered` is also added for
`get_by_ids_filtered()`, applying a predicate on raw bytes before
deserialization.

## 4. Projection Pushdown

### Problem

`Row::from_bytes()` deserializes all columns. For
`SELECT id, title FROM blog_posts`, the large `content` column is
deserialized and immediately discarded.

### Solution

New method on `Row`:

```rust
pub fn from_bytes_projected(bytes: &[u8], col_indices: &[usize]) -> Result<Self>
```

Uses the offset array to deserialize only the columns specified in
`col_indices`, in the given order. Returns a `Row` with
`values.len() == col_indices.len()`.

Integration:
- Add `projection: Option<Vec<usize>>` parameter to
  `scan_with_limit()` and the `DirectDataAccess` trait.
- When `projection` is `Some`, the final deserialization step uses
  `from_bytes_projected()` instead of `from_bytes()`.
- Filters are already evaluated on raw bytes via `Row::value_at()`,
  so projection only affects the final output — independent of filtering.
- When `projection` is `None`, behavior is unchanged.

The `QueryBuilder` and REPL pass `projection` when the SELECT clause
specifies a subset of columns.

## 5. Memory-Mapped I/O (mmap)

### Problem

Every data file read goes through `seek` + `read_exact` syscalls (~1µs
each). The kernel already caches file pages, but syscall overhead is
fixed.

### Solution

Add a third backend to `DataFileBackend`:

```rust
enum DataFileBackend {
    File(BufWriter<File>),
    Memory(Vec<u8>),
    Mmap {
        file: File,
        map: memmap2::Mmap,
    },
}
```

**Dependency**: `memmap2` added to `[dependencies]` in `Cargo.toml`.

**Read path**: All read methods (`scan_rows_limited`, `scan_rows_callback`,
`read_row`, `read_raw`, `read_raw_with`, `read_batch_sequential`) in the
`Mmap` branch access `&map[offset..offset+length]` directly — zero
syscalls, zero copies.

**Write path**: Writes go through `File` directly (not the mmap). After
writes, the mmap is remapped lazily — only before the next read operation.
Remapping is lightweight (~10µs).

**Configuration**: `TableEngine` receives a `use_mmap: bool` flag
(default `true` for the `File` backend). The `Memory` backend is
unaffected.

**Constructor**: `DataFile::open_mmap()` creates the mmap backend.

## Implementation Order

1. **SmallString INLINE_CAP** — 1 line change
2. **read_raw_with()** — isolated in `data_file.rs` + `table_engine.rs`
3. **Batch I/O** — `data_file.rs` + `table_engine.rs`
4. **Projection pushdown** — `row.rs`, `lib.rs`, `direct.rs`
5. **mmap backend** — `data_file.rs`, `table_engine.rs`, `Cargo.toml`

## Out of Scope

- Zero-copy VARCHAR (lifetime parameters through Value).
- Page-based storage format.
- Parallel scan execution.
