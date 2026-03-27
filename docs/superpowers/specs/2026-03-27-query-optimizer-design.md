# Query Optimizer Improvements Design

Three optimizations targeting the remaining performance gaps vs SQLite in the
blog benchmark: COUNT 32x, indexed EQ/IN/BETWEEN 6-13x, and multi-filter
short-circuit efficiency.

## 1. Optimized COUNT with Filters

### Problem

`Database::count()` with filters calls `self.scan(table, filters)` which
fully deserializes all matching rows into `Vec<Row>`, then returns `.len()`.
Benchmark 11 (COUNT WHERE author_id=2) is 32x slower than SQLite.

### Solution: Two-path count

**Path A — Index-only count:** When all filters can be resolved entirely via
indices, count the row IDs without touching the data file.

- Single filter on indexed column: `index_mgr.search(col, val).len()` or
  `index_mgr.query_row_ids(col, op).len()`.
- Multi-filter with all columns indexed: `multi_index_scan()` returns
  intersected row IDs → `.len()`.
- If any filter cannot use an index, fall through to Path B.

**Path B — Callback count:** When filters cannot use indices, count via
callback scan without deserializing rows.

New methods:
- `DataFile::count_rows_callback(predicate: Fn(&[u8]) -> bool) -> Result<usize>`
  — same scan loop as `scan_rows_callback` but increments a counter instead of
  collecting rows. No `Vec<Row>` allocation, no `Row::from_bytes()`.
- `TableEngine::count_filtered(predicate: Fn(&[u8]) -> bool) -> Result<usize>`
  — delegates to `DataFile::count_rows_callback`.

**`Database::count()` rewrite:**

```
if filters.is_empty():
    return active_row_count()          // O(1)

// Path A: try index-only count
if all filters are indexable:
    row_ids = multi_index_scan() or single index query
    if no remaining filters:
        return row_ids.len()           // no data file I/O

// Path B: callback count
build predicate closure using Row::value_at() + filter.matches()
return table_engine.count_filtered(predicate)
```

### Performance Impact

- Index-only count: O(log n) — no data file access at all.
- Callback count: O(n) scan but no Row allocation, no SmallString
  construction — only the filter columns are deserialized per row.

## 2. Partial Deserialization for Indexed Paths

### Problem

When an index returns row IDs (e.g., `author_id = 1` → 2000 rows), all rows
are fully deserialized via `get_by_ids() → fetch_rows_sorted_by_offset() →
data_file.read_row() → Row::from_bytes()`. If there are remaining filters,
most of these deserialized rows are discarded.

Even without remaining filters, every column (including large VARCHAR fields)
is deserialized when the caller may only need a subset.

### Solution: Callback in fetch

**New method on `DataFile`:**

```rust
read_raw(offset: u64, length: u32) -> Result<Option<Vec<u8>>>
```

Like `read_row()` but returns the raw byte buffer without calling
`Row::from_bytes()`. Checks the marker byte and returns `None` for
tombstoned rows.

**New method on `TableEngine`:**

```rust
get_by_ids_filtered<F>(row_ids: &[u64], predicate: F) -> Result<Vec<Row>>
where F: Fn(&[u8]) -> bool
```

Internally:
1. Resolve RAT entries for each row_id → `(offset, length)`.
2. Sort by offset for sequential I/O.
3. For each entry, call `data_file.read_raw(offset, length)`.
4. If `predicate(&raw_bytes)` returns `true`, call `Row::from_bytes()` and
   include in results. Otherwise skip.

**Integration in `Database::scan_with_limit()`:**

The single-index path currently calls methods like `search_by_index()` which
internally do `index_mgr.search() → get_by_ids()` and return `Vec<Row>`.
To inject the predicate, refactor the single-index path to:
1. Get `row_ids: Vec<u64>` via `index_mgr.query_row_ids(col, op)`.
2. Collect remaining filters (all filters except the one used for the index).
3. If remaining filters exist, call `get_by_ids_filtered()`. Otherwise
   call `get_by_ids()`.

```
// Multi-index path:
if remaining_filters.is_empty():
    rows = get_by_ids(&row_ids)
else:
    rows = get_by_ids_filtered(&row_ids, |raw| {
        evaluate remaining_filters via Row::value_at()
    })

// Single-index path:
row_ids = index_mgr.query_row_ids(col, &op)
remaining = filters excluding the index filter
if remaining.is_empty():
    rows = get_by_ids(&row_ids)
else:
    rows = get_by_ids_filtered(&row_ids, |raw| {
        evaluate remaining via Row::value_at()
    })
```

When there are no remaining filters, use `get_by_ids()` as before (no
overhead added). The existing `*_by_index()` methods on TableEngine remain
unchanged — they are not called from the refactored path.

### Performance Impact

For `IN (1, 3)` on `author_id` returning 4000 rows: previously all 4000 are
fully deserialized. With this optimization, only the filter columns are
extracted per row, and full deserialization happens only for rows that pass
all remaining filters.

## 3. Filter Cost-Based Reordering

### Problem

Post-scan filters are applied in the order the user writes them. A costly
LIKE filter running before a cheap integer comparison wastes work on rows
that the integer check would have eliminated.

### Solution

Add `Filter::estimated_cost() -> u8` based on the operator type:

| Cost | Operators |
|------|-----------|
| 1 | `IsNull`, `IsNotNull` |
| 2 | `Equals(numeric)`, `NotEquals(numeric)` |
| 3 | `GreaterThan`, `LessThan`, `GreaterThanOrEqual`, `LessThanOrEqual` |
| 4 | `Between` |
| 5 | `In`, `NotIn` |
| 6 | `Equals(VARCHAR)` |
| 7 | `Like` prefix, `NotLike` prefix |
| 8 | `Like` suffix/contains/complex, `NotLike` suffix/contains/complex |

For operators containing a `Value` (e.g., `Equals(Value)`), the cost
distinguishes numeric vs VARCHAR by inspecting the value type.
`Value::Varchar` → higher cost, everything else → lower cost.

For `Like`/`NotLike`, the cost inspects the cached `LikePattern`: `Prefix`
→ cost 7, everything else → cost 8.

**Where to apply:** In `scan_with_limit()`, sort filters by cost before
building the callback closure or entering the post-filter loop:

```rust
active_filters.sort_by_key(|f| f.estimated_cost());
```

One sort point — applies to both the callback scan path and the indexed
post-filter path.

### Performance Impact

Zero impact on single-filter queries (the benchmark's current state).
For multi-filter queries, cheap filters eliminate rows before expensive
filters run, reducing total per-row work.

## Implementation Order

1. **Filter cost reordering** — isolated change in `filter.rs` + one line in
   `lib.rs`
2. **COUNT optimization** — `data_file.rs`, `table_engine.rs`, `lib.rs`
3. **Partial deserialization for indexed paths** — `data_file.rs`,
   `table_engine.rs`, `lib.rs`

## Out of Scope

- Histogram or CDF-based cardinality estimation.
- LIMIT pushdown into indexed scans.
- Parallel scan execution.
