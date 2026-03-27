# Text Matching Optimization Design

Three optimizations to improve ThunderDB's text matching performance, targeting
the 3-5x gap vs SQLite observed in the blog benchmark.

## 1. Row Format with Column Offsets

### Problem

Every scan deserializes all columns of every row, even when only one column is
needed for filtering. For a LIKE query on `title`, the entire `content` field
(the largest column) is deserialized and immediately discarded.

### Solution

Add a per-row offset array so that any column can be accessed in O(1) without
deserializing preceding columns.

**Row payload format** (inside the data file's `[marker:1][length:4]` envelope):

```
[row_id:8][col_count:4][off0:2][off1:2]...[offN-1:2][val0][val1]...[valN-1]
```

- `col_count`: u32 LE — number of columns.
- Offset array: `col_count` × u16 LE values. Each offset is relative to the
  start of the values area (immediately after the last offset entry). This
  supports row payloads up to 64 KB.
- Values: serialized `Value` entries, concatenated.

**Overhead**: 2 bytes × N columns per row. For 4 columns = 8 bytes per row
(~80 KB across 10 000 rows).

No backward compatibility — the old format is removed entirely.

### API Changes

**`Row` (`src/storage/row.rs`)**:

- `to_bytes()` / `write_to()` — write the offset array followed by values.
- `from_bytes(bytes)` — full deserialization (uses offsets internally).
- `value_at(bytes: &[u8], col_idx: usize) -> Result<Value>` — **new**. Reads
  a single column from raw bytes using the offset array. Does not allocate a
  `Row` or touch other columns.

**`DataFile` (`src/storage/data_file.rs`)**:

- `scan_rows_callback(limit, callback: Fn(&[u8]) -> bool) -> Result<Vec<Row>>`
  — **new**. Scans active rows. For each active row, passes the raw byte
  buffer to the callback. If the callback returns `true`, the row is fully
  deserialized and included in the result. If `false`, the row is skipped
  without deserialization. DataFile has no knowledge of filters — the callback
  encapsulates the filtering logic.

**`TableEngine` (`src/storage/table_engine.rs`)**:

- `scan_all_filtered(callback: Fn(&[u8]) -> bool) -> Result<Vec<Row>>` —
  **new**. Delegates to `DataFile::scan_rows_callback`.

**`Database` (`src/lib.rs`)**:

- `scan_with_limit()` — when the source is a full table scan with filters,
  build a closure that uses `Row::value_at()` to extract filter columns and
  `Filter::matches()` to evaluate them, then pass it to
  `scan_all_filtered()`. Only matching rows are fully deserialized.

### Performance Impact

For a LIKE query on `title` (column 2) in a 4-column table with a large
`content` column:
- Before: deserialize all 4 columns × 10 000 rows.
- After: deserialize only column 2 for filtering; full deserialization only for
  matching rows.

## 2. memchr SIMD Matching for LIKE

### Problem

`LikePattern::matches_string()` uses `str::starts_with`, `str::contains`,
`str::ends_with` — standard library methods that do byte-by-byte comparison
without SIMD acceleration.

### Solution

Add `memchr` as a direct dependency and use its SIMD-accelerated routines.

**Changes to `LikePattern` (`src/index/like.rs`)**:

- `Prefix`: use `s.as_bytes().starts_with(prefix.as_bytes())` — avoids UTF-8
  boundary checks.
- `Suffix`: use `s.as_bytes().ends_with(suffix.as_bytes())`.
- `Contains`: pre-build a `memchr::memmem::Finder` at parse time and cache it
  in the `LikePattern` variant. The Finder pre-computes SIMD lookup tables
  once; per-row matching is just the SIMD search.

**Contains variant change**:

```rust
// Before
Contains(String)

// After
Contains {
    needle: String,
    finder: memchr::memmem::Finder<'static>,
}
```

The `Finder` borrows from the `needle`'s heap allocation. Since `String`'s
heap buffer has a stable address (moving the struct does not move the heap
data), the `Finder`'s internal pointer remains valid. Construction uses unsafe
lifetime extension:

```rust
let needle = content.to_string();
let ptr = needle.as_bytes().as_ptr();
let len = needle.as_bytes().len();
let static_bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
let finder = memchr::memmem::Finder::new(static_bytes);
Contains { needle, finder }
```

This is safe because:
- `needle` is heap-allocated and lives alongside `finder` in the same struct.
- The struct is immutable after construction.
- `needle` is never modified or dropped before `finder` (same enum variant,
  dropped together, `needle` field declared first).

**`PartialEq` for `LikePattern`**: the `Finder` does not implement `PartialEq`.
Implement `PartialEq` manually, comparing only the `needle` strings.

**Dependency**: `memchr` added to `[dependencies]` in `Cargo.toml` (direct
dependency, no feature flag).

### Performance Impact

`memchr::memmem::Finder` uses AVX2/SSE2 for substring search — 2-10x faster
than `str::contains` on strings > 32 bytes. Prefix/suffix checks benefit
marginally from byte-slice comparison (avoids UTF-8 boundary checks). The
Finder preprocessing happens once at pattern parse time, not per row.

## 3. B-tree Index for Prefix LIKE on VARCHAR Columns

### Problem

The blog benchmark LIKE queries on `title` and `content` do a full table scan
because those columns have no index. The B-tree infrastructure and the query
planner already support prefix LIKE queries on indexed columns — the path
`choose_index → Operator::Like → prefix_search_by_index` exists but is never
exercised because no VARCHAR columns are indexed.

### Solution

No engine code changes required. The existing path works end-to-end:

1. `choose_index()` recognizes `Like("prefix%")` as indexable.
2. `scan_with_limit()` calls `prefix_search_by_index(col, prefix)`.
3. `prefix_search_by_index` does a B-tree range scan using `get_range_bounds()`.

**Changes**:

- **Benchmark test** (`tests/integration/thunderdb_vs_sqlite_bench.rs`): add
  `create_index("title")` on ThunderDB's `blog_posts` table so the prefix LIKE
  query exercises the index path.
- **SQLite benchmark**: add `CREATE INDEX idx_posts_title ON blog_posts(title)`
  for a fair comparison.
- Verify that the planner selects the index by confirming the query returns the
  same results with drastically lower latency.

### Performance Impact

Transforms prefix LIKE from O(n) full scan to O(log n + k) where k is the
number of matches. For `LIKE 'Post about rust%'` matching 2000 out of 10 000
rows, this eliminates 8000 unnecessary row reads.

## Implementation Order

1. **memchr integration** — isolated change in `like.rs` + `Cargo.toml`
2. **Row format with column offsets** — `row.rs`, `data_file.rs`,
   `table_engine.rs`, `lib.rs`, and all tests
3. **Prefix index in benchmark** — `thunderdb_vs_sqlite_bench.rs`

## Out of Scope

- Filter cost-based reordering (reorder post-scan filters by estimated cost,
  e.g., integer comparison before LIKE). Tracked separately for future work.
- Trigram indexes for `%contains%` and `%suffix` patterns.
- Columnar storage format.
